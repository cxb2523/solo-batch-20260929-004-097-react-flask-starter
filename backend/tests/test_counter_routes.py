import json
import threading

from src.models import Counter, CounterEvent, db


def _post_increment(client, idempotency_key, outcomes, statuses):
    response = client.post(
        '/api/v1/increment/',
        data=json.dumps({}),
        headers={
            'Content-Type': 'application/json',
            'Idempotency-Key': idempotency_key,
        },
    )
    statuses.append(response.status_code)
    outcomes.append(response.get_json())


def test_get_is_read_only(client):
    first = client.get('/api/v1/').get_json()
    second = client.get('/api/v1/').get_json()
    assert first['count'] == second['count'] == 0
    assert 'response' in first


def test_increment_post_enqueues_and_applies(client):
    response = client.post(
        '/api/v1/increment/',
        data=json.dumps({}),
        headers={
            'Content-Type': 'application/json',
            'Idempotency-Key': 'click-1',
        },
    )
    body = response.get_json()
    assert response.status_code == 200
    assert body['status'] == 'enqueued'
    assert body['duplicated'] is False
    # Eager celery: the worker has already applied the event.
    assert body['count'] == 1
    assert client.get('/api/v1/').get_json()['count'] == 1


def test_duplicate_idempotency_key_is_silently_dropped(client):
    for _ in range(3):
        response = client.post(
            '/api/v1/increment/',
            data=json.dumps({}),
            headers={
                'Content-Type': 'application/json',
                'Idempotency-Key': 'click-once',
            },
        )
        assert response.status_code == 200

    assert CounterEvent.query.count() == 1
    assert Counter.current(label='Test').count == 1
    assert client.get('/api/v1/').get_json()['count'] == 1


def test_concurrent_posts_distinct_keys_equal_net_count(app, client):
    outcomes = []
    statuses = []
    barrier = threading.Barrier(10)

    def worker(index):
        barrier.wait()
        # The test client is not guaranteed thread-safe; use a fresh one.
        local_client = app.test_client()
        _post_increment(local_client, f'key-{index}', outcomes, statuses)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert set(statuses) == {200}
    assert CounterEvent.query.count() == 10
    assert CounterEvent.query.filter_by(
        status=CounterEvent.STATUS_APPLIED
    ).count() == 10
    assert Counter.current(label='Test').count == 10


def test_concurrent_posts_same_key_net_one(app):
    outcomes = []
    statuses = []
    barrier = threading.Barrier(10)

    def worker():
        barrier.wait()
        local_client = app.test_client()
        _post_increment(local_client, 'shared-key', outcomes, statuses)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert set(statuses) == {200}
    assert CounterEvent.query.count() == 1
    assert Counter.current(label='Test').count == 1


def test_request_body_fingerprint_dedupes_without_header(client):
    payload = json.dumps({'amount': 1})
    for _ in range(2):
        client.post(
            '/api/v1/increment/',
            data=payload,
            headers={'Content-Type': 'application/json'},
        )
    assert CounterEvent.query.count() == 1
    assert Counter.current(label='Test').count == 1


def test_reset_zeroes_counter(client):
    client.post(
        '/api/v1/increment/',
        data=json.dumps({}),
        headers={'Idempotency-Key': 'k1',
                 'Content-Type': 'application/json'},
    )
    assert client.get('/api/v1/').get_json()['count'] == 1

    reset = client.post('/api/v1/reset/')
    assert reset.get_json()['count'] == 0
    assert client.get('/api/v1/').get_json()['count'] == 0
