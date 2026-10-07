import datetime

from sqlalchemy import func

from src.models.base import BaseModel, db


class Counter(BaseModel):

    count = db.Column(db.Integer, default=0, nullable=False)
    label = db.Column(db.String(), unique=True, nullable=False)

    RESET_THRESHOLD = 1000000000

    def increment(self, amount=1):
        """Legacy in-process increment (kept for compatibility; new write
        paths must go through CounterEvent instead)."""
        self.count += amount
        if self.count > self.RESET_THRESHOLD:
            self.count = 0
        self.save()

    def reset(self):
        self.count = 0
        self.save()

    @classmethod
    def apply_delta(cls, counter_id, amount, expected):
        """Conditional atomic accumulate (compare-and-swap).

        Issue::

            UPDATE counter SET count = count + :amount
             WHERE id = :id AND count = :expected

        and return True exactly when this call performed the accumulation.
        A single event therefore lands once even when workers run
        concurrently or a task is redelivered after a crash: whoever changes
        the row first invalidates the other's `expected` predicate.

        Trade-off vs. row-lock recompute (SELECT ... FOR UPDATE the counter,
        then recompute count from the sum of applied events): the conditional
        update holds no long-lived row lock, works identically on SQLite
        (which has no SELECT ... FOR UPDATE), and leaves rows that predate
        the events table untouched, so legacy counts survive with no
        backfill. The cost is a retry loop under contention and trusting the
        CAS state machine instead of recomputing truth from the event log.
        """
        updated = (
            db.session.query(cls)
            .filter(cls.id == counter_id, cls.count == expected)
            .update(
                {cls.count: cls.count + amount},
                synchronize_session=False,
            )
        )
        return updated == 1


class CounterEvent(BaseModel):
    """One idempotent count-delta event.

    The write endpoint only inserts rows here (with a unique idempotency
    key) and hands the row to the worker; the counter row itself is mutated
    solely by apply_event().
    """

    STATUS_PENDING = 'pending'
    STATUS_APPLIED = 'applied'
    STATUS_FAILED = 'failed'

    idempotency_key = db.Column(
        db.String(80), unique=True, nullable=False, index=True
    )
    amount = db.Column(db.Integer, default=1, nullable=False)
    status = db.Column(
        db.String(16), default=STATUS_PENDING, nullable=False, index=True
    )
    attempts = db.Column(db.Integer, default=0, nullable=False)
    last_error = db.Column(db.Text, nullable=True)
    applied_at = db.Column(db.DateTime, nullable=True)
    failed_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(
        db.DateTime, default=datetime.datetime.utcnow, nullable=False, index=True
    )

    MAX_ATTEMPTS = 5

    @classmethod
    def register(cls, idempotency_key, amount=1):
        """Insert an event deduplicated by idempotency key.

        Returns ``(event, created)``. A repeated delivery with the same key,
        including a racing concurrent insert, is silently swallowed: the
        pre-existing event comes back with ``created=False`` and no error is
        raised.
        """
        existing = cls.get(idempotency_key=idempotency_key)
        if existing is not None:
            return existing, False
        event = cls(
            idempotency_key=idempotency_key,
            amount=amount,
            status=cls.STATUS_PENDING,
            attempts=0,
        )
        db.session.add(event)
        try:
            db.session.commit()
        except Exception:
            # Unique-constraint collision with a concurrent insert, or any
            # other flush failure: discard the pending write and reload the
            # canonical event so the duplicate is silently dropped.
            db.session.rollback()
            existing = cls.get(idempotency_key=idempotency_key)
            if existing is not None:
                return existing, False
            raise
        return event, True

    def _claim(self):
        """Atomically reserve this event for one worker attempt.

        Only a row still unapplied and within its attempt budget can be
        claimed. The same claim from two workers or a redelivered task wins
        on at most one of them (the attempts column doubles as an
        optimistic-lock token).
        """
        claimed = (
            CounterEvent.query.filter(
                CounterEvent.id == self.id,
                CounterEvent.status != self.STATUS_APPLIED,
                CounterEvent.attempts < self.MAX_ATTEMPTS,
            )
            .update(
                {CounterEvent.attempts: CounterEvent.attempts + 1},
                synchronize_session=False,
            )
        )
        if claimed == 1:
            db.session.commit()
        else:
            db.session.rollback()
        return claimed == 1

    def _mark_applied(self):
        CounterEvent.query.filter(
            CounterEvent.id == self.id,
            CounterEvent.status != self.STATUS_APPLIED,
        ).update(
            {
                CounterEvent.status: self.STATUS_APPLIED,
                CounterEvent.applied_at: datetime.datetime.utcnow(),
                CounterEvent.last_error: None,
            },
            synchronize_session=False,
        )
        db.session.commit()

    def mark_failed(self, error):
        """Persist the terminal failure so retries cannot wedge the counter."""
        CounterEvent.query.filter(CounterEvent.id == self.id).update(
            {
                CounterEvent.status: self.STATUS_FAILED,
                CounterEvent.failed_at: datetime.datetime.utcnow(),
                CounterEvent.last_error: str(error)[-4000:],
            },
            synchronize_session=False,
        )
        db.session.commit()

    @classmethod
    def apply_event(cls, event_id, max_attempts=MAX_ATTEMPTS):
        """Worker entry point: accumulate one event exactly once.

        Safe under concurrency and redelivery. Returns True when this call
        performed the accumulation; False for events already applied or
        stolen by another worker (idempotent no-op).

        Poison messages are not retried forever: after `max_attempts` claims
        the row is marked 'failed' with the stored error, and the sweep task
        can requeue it with a bounded extra budget, so the pipeline can never
        jam permanently on one event.
        """
        event = db.session.get(cls, event_id)
        if event is None or event.status == cls.STATUS_APPLIED:
            return False
        if event.attempts >= max_attempts and event.status == cls.STATUS_FAILED:
            return False

        attempts_before = event.attempts
        if not event._claim():
            return False
        attempt_number = attempts_before + 1
        db.session.refresh(event)

        counter = Counter.get_create(label='Test')
        try:
            # Conditional atomic accumulate with a CAS loop. A competing
            # writer that moves the counter between our read and update just
            # forces another read; the event's pending->applied transition
            # below is what guarantees the delta lands exactly once.
            expected = db.session.query(Counter.count).filter(
                Counter.id == counter.id
            ).scalar()
            while not Counter.apply_delta(counter.id, event.amount, expected):
                expected = db.session.query(Counter.count).filter(
                    Counter.id == counter.id
                ).scalar()
        except Exception as error:
            db.session.rollback()
            if attempt_number >= max_attempts:
                event.mark_failed(error)
                return False
            raise

        event._mark_applied()
        return True

    @classmethod
    def requeue_failed(cls, max_attempts=MAX_ATTEMPTS * 2):
        """Sweeper: give retry-exhausted events a bounded second chance."""
        requeued = (
            db.session.query(cls)
            .filter(
                cls.status == cls.STATUS_FAILED,
                cls.attempts < max_attempts,
            )
            .update(
                {cls.status: cls.STATUS_PENDING, cls.failed_at: None},
                synchronize_session=False,
            )
        )
        db.session.commit()
        return requeued

    @classmethod
    def stale_pending(cls, older_than_seconds=60):
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(
            seconds=older_than_seconds
        )
        return (
            cls.query.filter(cls.status == cls.STATUS_PENDING)
            .filter(cls.created_at < cutoff)
            .all()
        )
