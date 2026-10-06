"""Add CounterEvent model

Revision ID: c4e7b91a2f80
Revises: fb060a24643d
Create Date: 2026-10-05 09:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c4e7b91a2f80'
down_revision = 'fb060a24643d'
branch_labels = None
depends_on = None


def upgrade():
    op.create_unique_constraint('uq_counter_label', 'counter', ['label'])
    op.create_table(
        'counter_event',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('uuid', sa.String(length=36), nullable=True),
        sa.Column('event_key', sa.String(length=64), nullable=False),
        sa.Column('label', sa.String(), nullable=False,
                  server_default='Test'),
        sa.Column('amount', sa.Integer(), nullable=False, server_default='1'),
        sa.Column('status', sa.String(length=16), nullable=False,
                  server_default='pending'),
        sa.Column('attempts', sa.Integer(), nullable=False,
                  server_default='0'),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('applied_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('event_key', name='uq_counter_event_event_key'),
    )
    op.create_index('ix_counter_event_event_key', 'counter_event',
                    ['event_key'])
    op.create_index('ix_counter_event_status', 'counter_event', ['status'])


def downgrade():
    op.drop_index('ix_counter_event_status', table_name='counter_event')
    op.drop_index('ix_counter_event_event_key', table_name='counter_event')
    op.drop_table('counter_event')
    op.drop_constraint('uq_counter_label', 'counter', type_='unique')
