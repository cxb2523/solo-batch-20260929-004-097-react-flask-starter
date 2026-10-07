import threading

import pytest

from src.models import Counter, CounterEvent, db


def test_register_dedupes_identical_idempotency_key(app):
    with app.app_context():
        event, created = CounterEvent.register('key-1')
        assert created is True
        assert event.status == CounterEvent.STATUS_PENDING

        duplicate, created_again = CounterEvent.register('key-1')
        assert created_again is False
        assert duplicate.id == event.id

        assert CounterEvent.query.count() == 1


def test_concurrent_events_are_all_registered_and_applied(app):
    event_count = 40

    def register_in_thread(index):
        with app.app_context():
            CounterEvent.register(f'conc-{index}')

    threads = [
        threading.Thread(target=register_in_thread, args=(i,))
        for i in range(event_count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    with app.app_context():
        assert CounterEvent.query.count() == event_count

        events = CounterEvent.list()

    def apply_in_thread(event_id):
        with app.app_context():
            CounterEvent.apply_event(event_id)

    workers = [
        threading.Thread(target=apply_in_thread, args=(event.id,))
        for event in events
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()

    with app.app_context():
        counter = Counter.get(label='Test')
        assert counter.count == event_count
        assert CounterEvent.query.filter_by(
            status=CounterEvent.STATUS_APPLIED
        ).count() == event_count


def test_same_event_applied_concurrently_by_two_workers(app):
    with app.app_context():
        event, _ = CounterEvent.register('race-one')
        event_id = event.id

    results = []

    def worker():
        with app.app_context():
            results.append(CounterEvent.apply_event(event_id))

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Exactly one worker performed the accumulation.
    assert sorted(results) == [False, True]
    with app.app_context():
        assert Counter.get(label='Test').count == 1


def test_reapplying_an_applied_event_is_a_noop(app):
    with app.app_context():
        event, _ = CounterEvent.register('already-done')
        assert CounterEvent.apply_event(event.id) is True
        assert CounterEvent.apply_event(event.id) is False
        assert Counter.get(label='Test').count == 1


def test_retry_exhaustion_marks_event_failed_and_pipeline_moves_on(app, monkeypatch):
    with app.app_context():
        bad_event, _ = CounterEvent.register('poison')
        good_event, _ = CounterEvent.register('healthy')

        # Simulate a poison message: accumulation always raises.
        def boom(*_args, **_kwargs):  # noqa: ARG001
            raise RuntimeError('disk on fire')

        monkeypatch.setattr(Counter, 'apply_delta', classmethod(boom))

        for _ in range(CounterEvent.MAX_ATTEMPTS - 1):
            with pytest.raises(RuntimeError):
                CounterEvent.apply_event(bad_event.id)
        # Final attempt: no raise, terminal failure is persisted.
        assert CounterEvent.apply_event(bad_event.id) is False

        monkeypatch.undo()

        # Pipeline is not wedged: a later event applies normally.
        assert CounterEvent.apply_event(good_event.id) is True
        assert Counter.get(label='Test').count == 1

        db.session.refresh(bad_event)
        assert bad_event.status == CounterEvent.STATUS_FAILED
        assert 'disk on fire' in bad_event.last_error
        assert bad_event.attempts == CounterEvent.MAX_ATTEMPTS


def test_requeue_failed_gives_a_bounded_second_chance(app, monkeypatch):
    with app.app_context():
        event, _ = CounterEvent.register('revived')
        monkeypatch.setattr(
            Counter,
            'apply_delta',
            classmethod(lambda *a, **k: (_ for _ in ()).throw(
                RuntimeError('nope'))),
        )
        for _ in range(CounterEvent.MAX_ATTEMPTS - 1):
            with pytest.raises(RuntimeError):
                CounterEvent.apply_event(event.id)
        CounterEvent.apply_event(event.id)
        db.session.refresh(event)
        assert event.status == CounterEvent.STATUS_FAILED

        assert CounterEvent.requeue_failed() == 1
        monkeypatch.undo()
        assert CounterEvent.apply_event(event.id) is True
        assert Counter.get(label='Test').count == 1


def test_legacy_counter_row_is_respected_without_backfill(app):
    with app.app_context():
        legacy = Counter.create(label='Test', count=42)
        event, _ = CounterEvent.register('post-legacy')
        CounterEvent.apply_event(event.id)
        assert Counter.get(id=legacy.id).count == 43
        assert CounterEvent.query.count() == 1
