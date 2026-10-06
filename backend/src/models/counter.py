import datetime

from sqlalchemy import update

from src.models.base import BaseModel, db


def _utcnow():
    return datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)


class Counter(BaseModel):

    count = db.Column(db.Integer, default=0, nullable=False)
    label = db.Column(db.String(), nullable=False, unique=True)

    RESET_THRESHOLD = 1000000000

    @classmethod
    def reset_atomic(cls, label='Test'):
        """Set the counter back to zero with a single conditional UPDATE."""
        counter = cls.get_create(label=label)
        db.session.execute(
            update(cls).where(cls.id == counter.id).values(count=0)
        )
        db.session.commit()
        return cls.get(id=counter.id)

    @classmethod
    def atomic_add(cls, amount, label='Test'):
        """Unconditional atomic ``count += amount`` (replay protection lives
        in :meth:`CounterEvent.apply`, which gates this call)."""
        counter = cls.get_create(label=label)
        db.session.execute(
            update(cls).where(cls.id == counter.id).values(
                count=cls.count + amount
            )
        )
        db.session.commit()
        return cls.get(id=counter.id)

    @classmethod
    def current(cls, label='Test'):
        """Read-only accessor that also materializes a zero-valued row."""
        counter = cls.get(label=label)
        if counter is not None:
            return counter
        try:
            return cls.create(label=label, count=0)
        except Exception:
            # Lost a create race against a concurrent worker/request.
            db.session.rollback()
            return cls.get(label=label)


class CounterEvent(BaseModel):
    """A single counting event.

    The ``event_key`` column carries the idempotency key and has a UNIQUE
    constraint, so a redelivered event is rejected by the database itself.
    Events move through ``pending -> applied`` (or ``failed`` once retries
    are exhausted).
    """

    STATUS_PENDING = 'pending'
    STATUS_APPLIED = 'applied'
    STATUS_FAILED = 'failed'

    DEFAULT_LABEL = 'Test'

    event_key = db.Column(db.String(64), nullable=False, unique=True, index=True)
    label = db.Column(db.String(), nullable=False, default=DEFAULT_LABEL)
    amount = db.Column(db.Integer, nullable=False, default=1)
    status = db.Column(
        db.String(16), nullable=False, default=STATUS_PENDING, index=True
    )
    attempts = db.Column(db.Integer, nullable=False, default=0)
    last_error = db.Column(db.Text, nullable=True)
    created_at = db.Column(
        db.DateTime, nullable=False, default=_utcnow
    )
    applied_at = db.Column(db.DateTime, nullable=True)

    @classmethod
    def record(cls, event_key, amount=1, label=DEFAULT_LABEL):
        """Idempotently insert an event.

        Returns ``(event, created)``. A duplicate ``event_key`` is silently
        dropped and the pre-existing event is returned instead.
        """
        existing = cls.get(event_key=event_key)
        if existing is not None:
            return existing, False
        event = cls(
            event_key=event_key,
            amount=amount,
            label=label,
            status=cls.STATUS_PENDING,
        )
        db.session.add(event)
        try:
            db.session.commit()
        except Exception:
            # Lost an insert race against another producer/worker.
            db.session.rollback()
            existing = cls.get(event_key=event_key)
            if existing is not None:
                return existing, False
            raise
        return cls.get(event_key=event_key), True

    @classmethod
    def apply_event(cls, event_id):
        """Apply one event to its counter exactly once.

        Strategy: conditional atomic update (compare-and-swap) rather than a
        ``SELECT ... FOR UPDATE`` recompute.

        Trade-off: a row lock + ``SUM(pending events)`` recompute is
        resilient to arbitrary drift of ``Counter.count``, but it serializes
        every worker on the counter row (lock waits, deadlock retries) and
        has to hold the lock while scanning events. The conditional update
        below is a single statement: it claims the event only if it is still
        ``pending``; a second worker (or a redelivered task after a crash)
        flips zero rows and treats the event as a no-op. Claim + counter
        increment happen in one transaction, so the net count always equals
        the number of applied events -- provided the counter row itself is
        only mutated through this path, which the service guarantees.

        Returns ``'applied'`` or ``'duplicate'``.
        """
        event = cls.get(id=event_id)
        if event is None or event.status != cls.STATUS_PENDING:
            return 'duplicate'

        # Make sure the counter row exists before the atomic claim so the
        # UPDATE below always has a target.
        Counter.current(label=event.label)

        claimed = db.session.execute(
            update(cls)
            .where(cls.id == event_id)
            .where(cls.status == cls.STATUS_PENDING)
            .values(
                status=cls.STATUS_APPLIED,
                applied_at=_utcnow(),
                attempts=cls.attempts + 1,
                last_error=None,
            )
        ).rowcount
        if not claimed:
            db.session.rollback()
            return 'duplicate'

        db.session.execute(
            update(Counter)
            .where(Counter.label == event.label)
            .values(count=Counter.count + event.amount)
        )
        db.session.commit()
        return 'applied'

    def mark_attempt(self, error):
        """Record a failed processing attempt while staying pending."""
        db.session.execute(
            update(self.__class__)
            .where(self.__class__.id == self.id)
            .values(
                attempts=self.__class__.attempts + 1,
                last_error=str(error),
            )
        )
        db.session.commit()

    @classmethod
    def mark_failed(cls, event_id, error):
        """Terminal marker for retries that have been exhausted.

        Without this a poison event would be redelivered forever (or get
        buried on a dead-letter queue nobody reads) and the counter would
        appear permanently stuck. Only a still-pending event can be marked.
        """
        updated = db.session.execute(
            update(cls)
            .where(cls.id == event_id)
            .where(cls.status == cls.STATUS_PENDING)
            .values(
                status=cls.STATUS_FAILED,
                last_error=str(error),
            )
        ).rowcount
        db.session.commit()
        return bool(updated)

    @classmethod
    def requeue_stale(cls, older_than_seconds=30):
        """Re-enqueue pending events whose worker presumably died mid-task.

        Events are claimed atomically inside :meth:`apply_event`, so
        re-delivery is harmless; this only recovers events that would
        otherwise stay pending forever. Returns the list of event ids.
        """
        cutoff = _utcnow() - datetime.timedelta(seconds=older_than_seconds)
        stale = (
            cls.query.filter(cls.status == cls.STATUS_PENDING)
            .filter(cls.created_at < cutoff)
            .order_by(cls.id)
            .all()
        )
        return [event.id for event in stale]

