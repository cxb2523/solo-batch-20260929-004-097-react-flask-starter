import { useCallback, useEffect, useRef, useState } from 'react';
import { Box, Button, Card, Link } from '@material-ui/core';
import Prism from 'prismjs';

import flaskLogo from '../assets/images/flask.png';

function ResponseBlock(props) {
  useEffect(() => {
    Prism.highlightAll();
  }, []);
  return (
    <pre>
      <code className={props.language}>{`${props.code}`}</code>
    </pre>
  );
}

function uuid() {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  return `id-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function ConnectCard(props) {
  const [error, setError] = useState(null);
  const [isLoaded, setIsLoaded] = useState(false);
  const [data, setData] = useState(null);
  const [count, setCount] = useState(0);
  const [sending, setSending] = useState(false);
  const [actionError, setActionError] = useState(null);
  const { classes } = props;

  // countRef is used so rapid clicks always derive the optimistic value
  // from the latest known server value, not from a stale state snapshot.
  const countRef = useRef(0);

  const fetchData = useCallback(async () => {
    try {
      const res = await fetch('/api/v1/');
      if (!res.ok) throw new Error(`GET /api/v1/ failed: ${res.status}`);
      const result = await res.json();
      setData(result);
      // The server value is authoritative.
      countRef.current = result.count;
      setCount(result.count);
      setError(null);
      setIsLoaded(true);
    } catch (err) {
      // Offline / server down: keep the last known value, show the state.
      setIsLoaded(true);
      setError(err);
    }
  }, []);

  const incrementCounter = async () => {
    // Optimistic update: show +1 now, remember the previous value so we can
    // roll back if the write fails.
    const previousCount = countRef.current;
    const optimistic = previousCount + 1;
    countRef.current = optimistic;
    setCount(optimistic);
    setSending(true);
    setActionError(null);

    try {
      const res = await fetch('/api/v1/increment/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': uuid(),
        },
        body: JSON.stringify({}),
      });
      if (!res.ok) throw new Error(`increment failed: ${res.status}`);
      const result = await res.json();
      // Adopt the server-returned value, then reconcile with a fresh GET.
      countRef.current = result.count;
      setCount(result.count);
      await fetchData();
    } catch (err) {
      // Network failure (e.g. offline): revert the optimistic bump and
      // re-sync from the server.
      countRef.current = previousCount;
      setCount(previousCount);
      setActionError(err);
      await fetchData();
    } finally {
      setSending(false);
    }
  };

  const resetCounter = async () => {
    setSending(true);
    setActionError(null);
    try {
      const res = await fetch('/api/v1/reset/', { method: 'POST' });
      if (!res.ok) throw new Error(`reset failed: ${res.status}`);
      const result = await res.json();
      countRef.current = result.count;
      setCount(result.count);
      await fetchData();
    } catch (err) {
      setActionError(err);
    } finally {
      setSending(false);
    }
  };

  useEffect(() => {
    fetchData();
    // Periodic reconciliation: after connectivity is restored the number
    // converges back to whatever GET reports.
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, [fetchData]);

  const codeBlock = `@blueprint.route('/api/v1/increment/', methods=['POST'])
def increment_event():
    event_key = resolve_idempotency_key()  # client uuid or request fingerprint
    event, created = CounterEvent.record(event_key)   # UNIQUE, deduped
    if created:
        dispatch_counter_event(event.id)             # -> celery worker
    return jsonify(count=Counter.current().count)`;

  if (error) {
    return (
      <Card className={classes.card}>
        <h2>Connect to a server</h2>
        <div>
          Cannot reach the server ({error.message}). Last known count:{' '}
          <b>{count}</b>
        </div>
        <Box display="flex" justifyContent="center" mt={2}>
          <Button
            variant="contained"
            size="small"
            className={classes.contained}
            onClick={fetchData}
          >
            Retry
          </Button>
        </Box>
      </Card>
    );
  }
  if (!isLoaded) {
    return <div>Loading...</div>;
  }
  return (
    <Card className={classes.card}>
      <h2>Connect to a server</h2>
      <Box
        display="flex"
        flexDirection="row"
        alignItems="center"
        mt={-3}
        mb={-1}
      >
        <Box mr={1}>
          <h3>Powered by</h3>
        </Box>
        <img width="75px" height="100%" src={flaskLogo} alt="Flask Logo" />
      </Box>
      <p>
        This React frontend is connected to a Flask server. Below is the
        response message we receive when we read the counter:
      </p>

      <pre className={classes.pre}>
        <p className={classes.response}>
          {'>'} {data && data.response}
        </p>
      </pre>

      <p>
        Current counter value: <b>{count}</b>
      </p>
      {actionError && (
        <p style={{ color: '#ff8a80' }}>
          Last action failed and was rolled back: {actionError.message}
        </p>
      )}

      <Box display="flex" justifyContent="center" mb={2}>
        <Button
          variant="contained"
          size="small"
          className={classes.contained}
          onClick={incrementCounter}
          disabled={sending}
          style={{ marginRight: '10px' }}
        >
          Ping Server
        </Button>
        <Button
          variant="contained"
          size="small"
          className={classes.contained}
          onClick={resetCounter}
          disabled={sending}
        >
          Reset Counter
        </Button>
      </Box>
      <p>
        The write endpoint no longer increments directly: it records one
        idempotent <code>CounterEvent</code> and hands it to the Celery
        worker, which accumulates atomically. Rapid clicks and retried or
        duplicated requests cannot double-count.
      </p>
      <p>
        The <b>endpoint</b> that records ping events lives in{' '}
        <code>
          <Link
            className={classes.link}
            href={`backend/src/routes/counter.py`}
            target="_blank"
          >
            backend/src/routes/counter.py
          </Link>
        </code>
        :
      </p>
      <ResponseBlock language="language-js" code={codeBlock} />
    </Card>
  );
}

export default ConnectCard;
