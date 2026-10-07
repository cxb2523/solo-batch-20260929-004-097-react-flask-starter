import os


class Parse:
    """Class of utility functions to parse environment variables"""

    @staticmethod
    def bool(field):
        """Parse booleans (defaults to False)"""
        return os.getenv(field, '').lower() in ['true', '1']


class Settings:

    # General
    DEV = Parse.bool('DEV')
    REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
    REDIS_URL = f'redis://{REDIS_HOST}:6379'

    # Database
    AES_SECRET_KEY = os.getenv('AES_SECRET_KEY', 'fake-aes-key')
    SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL')

    # Celery
    CELERY_BROKER_URL = REDIS_URL
    CELERY_RESULT_BACKEND = REDIS_URL
    _CELERY_EAGER = Parse.bool('CELERY_TASK_ALWAYS_EAGER')
    CELERY_CONFIG = {
        'task_always_eager': _CELERY_EAGER,
        'task_eager_propagates': False,
        'beat_schedule': {
            # Safety net, not a writer: the worker must never increment the
            # counter on its own (that double-wrote with the GET endpoint).
            # This only redelivers lost/stuck events.
            'reap_stale_events': {
                'task': 'src.tasks.reap_stale_events',
                'schedule': 15.0,
            },
        }
    }
