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
    CELERY_CONFIG = {
        'beat_schedule': {
            'requeue-stale-counter-events': {
                'task': 'src.tasks.requeue_stale_counter_events',
                'schedule': 30.0,
            },
        },
        # When true (e.g. tests) tasks run synchronously in the caller.
        'task_always_eager': Parse.bool('CELERY_ALWAYS_EAGER'),
        'task_eager_propagates': Parse.bool('CELERY_ALWAYS_EAGER'),
    }
