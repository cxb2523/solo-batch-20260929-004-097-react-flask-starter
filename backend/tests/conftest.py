import os

import tempfile

import pytest


# Must be set before anything from ``src`` is imported.
DB_PATH = os.path.join(tempfile.gettempdir(), '_pytest_counter.db')
os.environ.setdefault(
    'DATABASE_URL', f'sqlite:///{DB_PATH}?timeout=30'
)
os.environ.setdefault('CELERY_ALWAYS_EAGER', 'true')
os.environ.setdefault('REDIS_HOST', 'localhost')


# Let concurrent writers wait for the SQLite write lock instead of failing.
from sqlalchemy import event  # noqa: E402
from sqlalchemy.pool import Pool  # noqa: E402


@event.listens_for(Pool, 'connect')
def _set_sqlite_pragmas(dbapi_connection, connection_record):
    if not str(type(dbapi_connection)).endswith("sqlite3.Connection'>"):
        return
    cursor = dbapi_connection.cursor()
    cursor.execute('PRAGMA busy_timeout=30000')
    cursor.execute('PRAGMA journal_mode=WAL')
    cursor.close()


@pytest.fixture()
def app():
    # Fresh Application per test; it reads DATABASE_URL at import/build time.
    from src.app import Application
    from src.models import db

    application = Application()
    flask_app = application.flask_app
    flask_app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'connect_args': {'timeout': 30},
    }
    with flask_app.app_context():
        db.drop_all()
        db.create_all()
        yield flask_app
        db.session.remove()
        db.drop_all()


@pytest.fixture()
def client(app):
    return app.test_client()
