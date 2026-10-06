import threading

import pytest

from src.models import Counter, CounterEvent, db
from src.tasks import MAX_ATTEMPTS, process_counter_event


class FakeTask:
    def __init__(self, exc):
        self.exc = exc

    def retry(self, exc=None, countdown=None):
        raise self.exc


def _push_app(app):
    ctx = app.app_context()
    ctx.push()
    return ctx


def test_record_is_deduplicated_by_event_key(app):
    event, created = CounterEvent.record('dup-key')
    assert created is True
    assert event.status == CounterEvent.STATUS_PENDING

    again, created_again = CounterEvent.record('dup-key')
    assert created_again is False
    assert again.id == event.id
    assert CounterEvent.query.count() == 1


def test_apply_event_accumulates_exactly_once(app):
    event, _ = CounterEvent.record('e1')

    assert CounterEvent.apply_event(event.id) == 'applied'
    assert CounterEvent.apply_event(event.id) == 'duplicate'

    assert Counter.current(label='Test').count == 1
    applied = CounterEvent.get(id=event.id)
    assert applied.status == CounterEvent.STATUS_APPLIED
    assert applied.applied_at is not None


def test_concurrent_record_same_key_keeps_single_row(app):
    """Repeated web deliveries of one idempotency key are silently dropped."""
    results = []
    barrier = threading.Barrier(8)

    def worker():
        ctx = _push_app(app)
        try:
            barrier.wait()
            _, created = CounterEvent.record('same-key')
            results.append(created)
        finally:
            db.session.remove()
            ctx.pop()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert CounterEvent.query.filter_by(event_key='same-key').count() == 1
    assert sum(1 for created in results if created) == 1


def test_concurrent_apply_same_event_counts_net_one(app):
    """Competing/redelivered worker executions equal a single net increment."""
    event, _ = CounterEvent.record('race-apply')
    outcomes = []
    barrier = threading.Barrier(8)

    def worker():
        ctx = _push_app(app)
        try:
            barrier.wait()
            outcomes.append(CounterEvent.apply_event(event.id))
        except Exception:
            db.session.rollback()
            outcomes.append('error')
        finally:
            db.session.remove()
            ctx.pop()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes.count('applied') == 1
    assert outcomes.count('duplicate') == 7
    assert Counter.current(label='Test').count == 1


def test_concurrent_distinct_events_equal_net_event_increment(app):
    """Many concurrent events must equal their algebraic net change."""
    count = 12
    events = []
    for index in range(count):
        event, _ = CounterEvent.record(f'unique-{index}')
        events.append(event.id)

    errors = []
    barrier = threading.Barrier(count)

    def worker(event_id):
        ctx = _push_app(app)
        try:
            barrier.wait()
            assert CounterEvent.apply_event(event_id) == 'applied'
        except Exception as exc:
            errors.append(exc)
        finally:
            db.session.rollback()
            db.session.remove()
            ctx.pop()

    threads = [threading.Thread(target=worker, args=(eid,)) for eid in events]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert Counter.current(label='Test').count == count
    assert CounterEvent.query.filter_by(
        status=CounterEvent.STATUS_APPLIED
    ).count() == count


def test_retries_exhausted_marks_event_failed_and_does_not_block(app,
                                                                 monkeypatch):
    event, _ = CounterEvent.record('poison')

    def boom(event_id):
        raise RuntimeError('worker blew up')

    monkeypatch.setattr(CounterEvent, 'apply_event', boom)
    fake = FakeTask(RuntimeError('stop retrying'))

    for attempt in range(MAX_ATTEMPTS - 1):
        with pytest.raises(RuntimeError):
            process_counter_event(event.id, task=fake)

    # Last attempt: no exception, event is marked failed in the database.
    assert process_counter_event(event.id, task=fake) == 'failed'

    failed = CounterEvent.get(id=event.id)
    assert failed.status == CounterEvent.STATUS_FAILED
    assert failed.attempts == MAX_ATTEMPTS
    assert 'worker blew up' in failed.last_error

    # A healthy event afterwards still processes fine -- no permanent stall.
    monkeypatch.undo()
    good, _ = CounterEvent.record('healthy')
    assert process_counter_event(good.id, task=fake) == 'applied'
    assert Counter.current(label='Test').count == 1


def test_requeue_stale_returns_only_old_pending_events(app):
    import datetime

    old, _ = CounterEvent.record('old')
    young, _ = CounterEvent.record('young')
    CounterEvent.query.filter_by(id=old.id).update(
        {'created_at': datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(hours=1)}
    )
    db.session.commit()

    ids = CounterEvent.requeue_stale(older_than_seconds=30)
    assert ids == [old.id]
    assert young.id not in ids


