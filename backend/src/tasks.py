import logging

from celery import Celery

from src.models import CounterEvent, db
from src.settings import Settings as S


logger = logging.getLogger(__name__)

# Total delivery attempts before an event is marked failed in the database.
MAX_ATTEMPTS = 5
STALE_EVENT_SECONDS = 30


def make_celery(app):
    # Create and configure the celery app
    celery = Celery(
        app.import_name,
        backend=app.config['CELERY_RESULT_BACKEND'],
        broker=app.config['CELERY_BROKER_URL']
    )
    celery.conf.update(**S.CELERY_CONFIG)

    # Subclass the base Task so that tasks run in a Flask app context
    TaskBase = celery.Task
    class ContextTask(TaskBase):  # noqa
        def __call__(self, *args, **kwargs):  # noqa
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)
    celery.Task = ContextTask

    # Add the tasks
    Tasks.bind(celery)

    return celery


class Tasks:

    # How to add new tasks:
    # - Create the static method (like `apply_counter_event()`)
    # - Add the corresponding bind in the bind() method

    @classmethod
    def bind(cls, celery):
        """Bind the Celery app to all the tasks"""
        cls.apply_counter_event = celery.task(
            name='src.tasks.apply_counter_event',
            bind=True,
            max_retries=MAX_ATTEMPTS - 1,
            default_retry_delay=1,
        )(cls.apply_counter_event)
        cls.requeue_stale_counter_events = celery.task(
            name='src.tasks.requeue_stale_counter_events',
        )(cls.requeue_stale_counter_events)

    @staticmethod
    def apply_counter_event(self, event_id):
        """Apply one counter event, with retries and a terminal failed mark.

        The actual accumulation is an atomic compare-and-swap inside the
        model, so concurrent deliveries and redelivered retries produce the
        exact net change of the event set.
        """
        outcome = process_counter_event(event_id, task=self)
        return outcome

    @staticmethod
    def requeue_stale_counter_events(older_than_seconds=STALE_EVENT_SECONDS):
        """Recover pending events whose processing died mid-flight."""
        event_ids = CounterEvent.requeue_stale(older_than_seconds)
        for event_id in event_ids:
            Tasks.apply_counter_event.delay(event_id)
        return event_ids


def process_counter_event(event_id, task=None):
    """Framework-free core of the counter-event worker.

    Kept importable/callable without a Celery round-trip so tests can drive
    it from threads; the Celery task above is a thin wrapper.

    Returns ``'applied'`` or ``'duplicate'`` on success. Raises
    ``counter_event_exhausted``-style retries through the bound task; on the
    final attempt the event is marked ``failed`` in the database instead, so
    it never blocks the counter pipeline silently.
    """
    event = CounterEvent.get(id=event_id)
    if event is None:
        return 'duplicate'

    try:
        return CounterEvent.apply_event(event_id)
    except Exception as error:
        db.session.rollback()
        # Reload to observe attempts made by other worker deliveries.
        event = CounterEvent.get(id=event_id)
        if event is not None and event.status == CounterEvent.STATUS_PENDING:
            event.mark_attempt(error)

        event = CounterEvent.get(id=event_id)
        attempts = event.attempts if event is not None else MAX_ATTEMPTS
        if attempts >= MAX_ATTEMPTS:
            CounterEvent.mark_failed(event_id, error)
            logger.error(
                'counter event %s failed permanently after %s attempts: %s',
                event_id, attempts, error,
            )
            return 'failed'

        if task is not None:
            raise task.retry(exc=error, countdown=min(2 ** attempts, 30))
        raise


def dispatch_counter_event(event_id):
    """Hand an event off to the worker.

    The event is already committed at this point, so a broker hiccup must
    not fail the request: ``requeue_stale_counter_events`` (celery beat)
    re-enqueues pending events. Returns True when delivery was attempted
    successfully.
    """
    try:
        Tasks.apply_counter_event.delay(event_id)
        return True
    except Exception as error:  # pragma: no cover - depends on broker state
        logger.warning(
            'could not enqueue counter event %s (%s); '
            'stale-event recovery will retry it',
            event_id, error,
        )
        return False
