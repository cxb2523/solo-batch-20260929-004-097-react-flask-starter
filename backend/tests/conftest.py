import os
import tempfile

import pytest


# Configure before importing the application package: Settings is read at
# import time.
DB_FD, DB_PATH = tempfile.mkstemp(prefix='counter-test-', suffix='.db')
os.close(DB_FD)
os.environ.setdefault('DATABASE_URL', f'sqlite:///{DB_PATH}')
os.environ['CELERY_TASK_ALWAYS_EAGER'] = 'true'
os.environ['AES_SECRET_KEY'] = 'test-aes-key'

from sqlalchemy import event  # noqa: E402

from src.models import db  # noqa: E402


@pytest.fixture(scope='session')
def app():
    from sqlalchemy.engine import Engine

    from src.app import Application

    @event.listens_for(Engine, 'connect')
    def _sqlite_pragmas(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute('PRAGMA journal_mode=WAL')
        cursor.execute('PRAGMA busy_timeout=30000')
        cursor.close()

    application = Application()
    flask_app = application.flask_app
    flask_app.config['TESTING'] = True
    with flask_app.app_context():
        db.create_all()
    yield flask_app
    with flask_app.app_context():
        db.session.remove()
        db.drop_all()
    try:
        os.remove(DB_PATH)
    except OSError:
        pass


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture(autouse=True)
def clean_tables(app):
    """Each test starts with empty counter/counter_event rows."""
    from src.models import Counter, CounterEvent

    with app.app_context():
        CounterEvent.query.delete()
        Counter.query.delete()
        db.session.commit()
    yield
    with app.app_context():
        db.session.rollback()
