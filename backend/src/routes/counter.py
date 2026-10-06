import datetime
import hashlib
import json
import uuid

from flask import Blueprint, jsonify, request

from src.models import Counter, CounterEvent
from src.tasks import dispatch_counter_event


blueprint = Blueprint('counter', __name__)

RESPONSE_TEMPLATE = (
    'Flask server running on port 8080. Pinged {count} {times}, '
    'most recently on {date}.'
)

COUNTER_LABEL = CounterEvent.DEFAULT_LABEL

# ``enqueued``  -> event persisted, worker delivery attempted
# ``deduplicated`` -> the same idempotency key was already recorded
STATUS_ENQUEUED = 'enqueued'
STATUS_DEDUPLICATED = 'deduplicated'


@blueprint.route('/')
def index():
    response = 'Flask server'
    return jsonify(response=response)


@blueprint.route('/api/v1/reset/', methods=['GET', 'POST'])
def reset():
    # Reset stays an operator action; it never touches the event stream, so
    # it cannot double-write counters.
    counter = Counter.reset_atomic(label=COUNTER_LABEL)
    return jsonify(count=counter.count, response=counter.count)


@blueprint.route('/api/v1/')
def api():
    """Read-only view of the counter (the old endpoint used to increment)."""
    counter = Counter.current(label=COUNTER_LABEL)
    date = datetime_now()
    times = 'time' if counter.count == 1 else 'times'
    response = RESPONSE_TEMPLATE.format(
        count=counter.count,
        times=times,
        date=date)
    return jsonify(
        count=counter.count,
        times=times,
        date=date,
        response=response,
    )


@blueprint.route('/api/v1/increment/', methods=['POST'])
def increment_event():
    """Write path: record one idempotent counter event and hand it to the
    worker. The counter row itself is never mutated here -- that removes the
    old route/worker double write.
    """
    payload = request.get_json(silent=True) or {}
    event_key = resolve_idempotency_key(payload)
    amount, label = parse_payload(payload)

    event, created = CounterEvent.record(
        event_key, amount=amount, label=label
    )
    status = STATUS_ENQUEUED if created else STATUS_DEDUPLICATED
    if created:
        dispatch_counter_event(event.id)

    counter = Counter.current(label=label)
    return jsonify(
        idempotency_key=event.event_key,
        status=status,
        duplicated=not created,
        event_status=event.status,
        count=counter.count,
    )


def parse_payload(payload):
    try:
        amount = int(payload.get('amount', 1))
    except (TypeError, ValueError):
        amount = 1
    label = payload.get('label') or COUNTER_LABEL
    return amount, label


def resolve_idempotency_key(payload):
    """Client-supplied uuid wins; otherwise the JSON body is fingerprinted;
    a bare click (empty body) gets a fresh server-side uuid.
    """
    supplied = (
        request.headers.get('Idempotency-Key')
        or payload.get('idempotency_key')
    )
    if supplied:
        return str(supplied)[:64]

    if payload:
        digest = hashlib.sha256()
        digest.update(
            json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()
        )
        digest.update(b'|')
        digest.update((request.remote_addr or '').encode())
        return digest.hexdigest()
    return uuid.uuid4().hex


def datetime_now():
    return datetime.datetime.now().strftime('%c')
