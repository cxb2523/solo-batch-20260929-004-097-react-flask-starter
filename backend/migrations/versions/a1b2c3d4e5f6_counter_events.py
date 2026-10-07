"""Counter events with idempotency keys

Revision ID: a1b2c3d4e5f6
Revises: fb060a24643d
Create Date: 2026-10-07 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a1b2c3d4e5f6'
down_revision = 'fb060a24643d'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'counter_event',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('uuid', sa.String(length=36), nullable=True),
        sa.Column('idempotency_key', sa.String(length=80), nullable=False),
        sa.Column('amount', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('status', sa.String(length=16), nullable=False,
                  server_default='pending'),
        sa.Column('attempts', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column('applied_at', sa.DateTime(), nullable=True),
        sa.Column('failed_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('idempotency_key', name='uq_counter_event_idem_key'),
    )
    op.create_index(
        'ix_counter_event_idempotency_key',
        'counter_event',
        ['idempotency_key'],
        unique=True,
    )
    op.create_index(
        'ix_counter_event_status', 'counter_event', ['status']
    )
    op.create_index(
        'ix_counter_event_created_at', 'counter_event', ['created_at']
    )
    # Counter rows are now created under concurrent requests; the unique
    # label backs Counter.get_create's get-or-create. Legacy rows are kept as
    # is and deliberately NOT backfilled into counter_event.
    op.create_index(
        'ix_counter_label', 'counter', ['label'], unique=True
    )


def downgrade():
    op.drop_index('ix_counter_label', table_name='counter')
    op.drop_index('ix_counter_event_created_at', table_name='counter_event')
    op.drop_index('ix_counter_event_status', table_name='counter_event')
    op.drop_index(
        'ix_counter_event_idempotency_key', table_name='counter_event'
    )
    op.drop_table('counter_event')
