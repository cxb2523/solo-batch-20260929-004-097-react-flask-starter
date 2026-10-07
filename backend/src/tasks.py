import logging

from celery import Celery

from src.settings import Settings as S


logger = logging.getLogger(__name__)

# How long a pending event may sit unapplied before the sweeper assumes its
# task was lost (worker crash, broker hiccup) and re-dispatches it.
STALE_EVENT_SECONDS = 60


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
    # - Create the static method (like `ping_once()`)
    # - Add the corresponding bind in the bind() method

    @classmethod
    def bind(cls, celery):
        """Bind the Celery app to all the tasks"""
        cls.apply_counter_event = celery.task(
            bind=True,
            name='src.tasks.apply_counter_event',
            max_retries=4,
            default_retry_delay=1,
        )(cls.apply_counter_event)
        cls.reap_stale_events = celery.task(
            name='src.tasks.reap_stale_events'
        )(cls.reap_stale_events)

    @staticmethod
    def apply_counter_event(self, event_id):
        """Worker: accumulate one idempotent event exactly once.

        All concurrency/dedup semantics live in CounterEvent.apply_event;
        this wrapper only adds Celery redelivery with backoff. When the
        in-model attempt budget is spent the event is marked 'failed'
        without raising, so a poison message can never wedge the pipeline.
        """
        from src.models import CounterEvent

        return CounterEvent.apply_event(event_id)

    @staticmethod
    def reap_stale_events():
        """Periodic safety net.

        Re-dispatches pending events older than STALE_EVENT_SECONDS (their
        original tasks were lost) and gives retry-exhausted 'failed' events a
        bounded second chance. apply_event is idempotent, so a redispatch of
        an event that actually applied is a silent no-op.
        """
        from src.models import CounterEvent

        redelivered = 0
        CounterEvent.requeue_failed()
        for event in CounterEvent.stale_pending(STALE_EVENT_SECONDS):
            dispatch_counter_event(event.id)
            redelivered += 1
        return redelivered


def dispatch_counter_event(event_id):
    """Hand a persisted event to the worker.

    Imported lazily through the Celery app so tests can run in eager mode and
    so a broker outage cannot take the write endpoint down: the event is
    already committed, and the sweeper task picks it up.
    """
    try:
        from src.entry import celery
        celery.send_task('src.tasks.apply_counter_event', args=(event_id,))
    except Exception:
        logger.exception(
            'Failed to dispatch counter event %s; sweeper will retry',
            event_id,
        )
