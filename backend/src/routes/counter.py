import hashlib
import json
import uuid

from flask import Blueprint, jsonify, request

from src.models import Counter, CounterEvent
from src.tasks import dispatch_counter_event


blueprint = Blueprint('counter', __name__)

RESPONSE_TEMPLATE = 'Flask server running on port 8080. Pinged {count} {times}.'
COUNTER_LABEL = 'Test'


def _snapshot():
    return Counter.get_create(label=COUNTER_LABEL)


def _fingerprint():
    """Fallback idempotency key when the client sends no uuid.

    Built from the method, path and the canonical request body so the exact
    same request retried (e.g. a client/proxy redelivery) collapses, while
    distinct clicks still produce distinct bodies (the frontend sends a
    per-click uuid explicitly).
    """
    payload = request.get_json(silent=True)
    raw = json.dumps(
        {'method': request.method, 'path': request.path, 'body': payload or {}},
        sort_keys=True,
        separators=(',', ':'),
        default=str,
    )
    return 'fp-' + hashlib.sha256(raw.encode('utf-8')).hexdigest()


def _idempotency_key():
    key = request.headers.get('Idempotency-Key')
    if not key:
        payload = request.get_json(silent=True) or {}
        key = payload.get('idempotency_key')
    return key or _fingerprint()


@blueprint.route('/')
def index():
    return jsonify(response='Flask server')


@blueprint.route('/api/v1/count/', methods=['GET'])
def count():
    counter = _snapshot()
    times = 'time' if counter.count == 1 else 'times'
    return jsonify(
        count=counter.count,
        response=RESPONSE_TEMPLATE.format(count=counter.count, times=times),
    )


@blueprint.route('/api/v1/reset/', methods=['GET', 'POST'])
def reset():
    counter = _snapshot()
    counter.reset()
    return jsonify(count=counter.count, response=counter.count)


@blueprint.route('/api/v1/')
def api():
    """Read-only snapshot. Incrementing on GET double-wrote the counter
    (page load + worker ping); writes now live on POST /api/v1/count/."""
    counter = _snapshot()
    times = 'time' if counter.count == 1 else 'times'
    return jsonify(
        count=counter.count,
        response=RESPONSE_TEMPLATE.format(count=counter.count, times=times),
    )


@blueprint.route('/api/v1/count/', methods=['POST'])
def increment():
    """Record one idempotent count event and hand it to the worker.

    The counter row is never touched here: only an event with a unique
    idempotency key is persisted, so concurrent or retried POSTs cannot lose
    an increment. A repeated delivery with the same key is a silent no-op.
    """
    payload = request.get_json(silent=True) or {}
    amount = payload.get('amount', 1)
    try:
        amount = int(amount)
    except (TypeError, ValueError):
        return jsonify(error='amount must be an integer'), 400
    if amount < 0:
        return jsonify(error='amount must be non-negative'), 400

    key = _idempotency_key()
    event, created = CounterEvent.register(key, amount=amount)
    if created:
        # The event is durable first; dispatch failure must not lose it
        # because the periodic sweeper re-enqueues stale pending events.
        dispatch_counter_event(event.id)

    counter = _snapshot()
    times = 'time' if counter.count == 1 else 'times'
    return jsonify(
        count=counter.count,
        response=RESPONSE_TEMPLATE.format(count=counter.count, times=times),
        event_id=event.id,
        idempotency_key=event.idempotency_key,
        duplicate=not created,
        accepted=created,
    )
