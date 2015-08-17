#!/usr/bin/env python

# Things to do:
#  - create db
#  - config file to point at db? or just hard code it?
#  - 

from sqlalchemy import *

def make_tables(meta=None):
    if meta is None:
        meta = MetaData()

    events_tbl = Table('events', meta,
                Column('event_id', INTEGER, primary_key=True, nullable=False),
                Column('timestamp', TIMESTAMP(timezone=True), nullable=False),
                Column('reason', VARCHAR, nullable=True),
                Column('user', VARCHAR, nullable=False))

    block_tbl = Table('block', meta,
                Column('ev_create', BIGINT, primary_key=True, nullable=False),
                Column('ev_delete', BIGINT, nullable=True),
                Column('active',    BOOLEAN, nullable=True),
                Column('package',   VARCHAR, primary_key=True, nullable=False),
                ForeignKeyConstraint(['ev_create'], ['events.event_id']),
                ForeignKeyConstraint(['ev_delete'], ['events.event_id']),
                Index('idx_block_del', 'ev_delete', 'package'),
                Index('idx_block_package', 'package', 'ev_create'),
                UniqueConstraint('active', 'package'),
                CheckConstraint('(ev_delete > ev_create)'),
                CheckConstraint('(ev_delete IS NULL) = (active = 1)'),
                CheckConstraint('(active IS NULL) OR (active = 1)'))

    approve_tbl = Table('approve', meta,
                Column('ev_create', BIGINT, primary_key=True, nullable=False),
                Column('ev_delete', BIGINT, nullable=True),
                Column('active',    BOOLEAN, nullable=True),
                Column('package',   VARCHAR, primary_key=True, nullable=False),
                Column('version',   VARCHAR, primary_key=True, nullable=False),
                Index('idx_approve_del', 'ev_delete', 'package', 'version'),
                Index('idx_approve_package', 'package', 'version', 'ev_create'),
                ForeignKeyConstraint(['ev_create'], ['events.event_id']),
                ForeignKeyConstraint(['ev_delete'], ['events.event_id']),
                UniqueConstraint('active', 'package', 'version'),
                CheckConstraint('(ev_delete > ev_create)'),
                CheckConstraint('(ev_delete IS NULL) = (active = 1)'),
                CheckConstraint('(active IS NULL) OR (active = 1)'))

    exempt_tbl = Table('exempt', meta,
                Column('ev_create', BIGINT, primary_key=True, nullable=False),
                Column('ev_delete', BIGINT, nullable=True),
                Column('active',    BOOLEAN, nullable=True),
                Column('package',   VARCHAR, primary_key=True, nullable=False),
                Column('version',   VARCHAR, primary_key=True, nullable=False),
                Index('idx_exempt_del', 'ev_delete', 'package', 'version'),
                Index('idx_exempt_package', 'package', 'version', 'ev_create'),
                ForeignKeyConstraint(['ev_create'], ['events.event_id']),
                ForeignKeyConstraint(['ev_delete'], ['events.event_id']),
                UniqueConstraint('active', 'package', 'version'),
                CheckConstraint('(ev_delete > ev_create)'),
                CheckConstraint('(ev_delete IS NULL) = (active = 1)'),
                CheckConstraint('(active IS NULL) OR (active = 1)'))

    remove_tbl = Table('remove', meta,
                Column('ev_create', BIGINT, primary_key=True, nullable=False),
                Column('ev_delete', BIGINT, nullable=True),
                Column('active',    BOOLEAN, nullable=True),
                Column('package',   VARCHAR, primary_key=True, nullable=False),
                Column('version',   VARCHAR, primary_key=True, nullable=False),
                Index('idx_remove_del', 'ev_delete', 'package', 'version'),
                Index('idx_remove_package', 'package', 'version', 'ev_create'),
                ForeignKeyConstraint(['ev_create'], ['events.event_id']),
                ForeignKeyConstraint(['ev_delete'], ['events.event_id']),
                UniqueConstraint('active', 'package', 'version'),
                CheckConstraint('(ev_delete > ev_create)'),
                CheckConstraint('(ev_delete IS NULL) = (active = 1)'),
                CheckConstraint('(active IS NULL) OR (active = 1)'))

    transition_tbl = Table('transition', meta,
                Column('ev_create', BIGINT, primary_key=True, nullable=False),
                Column('ev_delete', BIGINT, nullable=True),
                Column('active',    BOOLEAN, nullable=True),
                Column('trans_id',  VARCHAR, primary_key=True, nullable=False),
                Column('package',   VARCHAR, primary_key=True, nullable=False),
                Column('version',   VARCHAR, nullable=False),
                ForeignKeyConstraint(['ev_create'], ['events.event_id']),
                ForeignKeyConstraint(['ev_delete'], ['events.event_id']),
                UniqueConstraint('active', 'trans_id', 'package'),
                CheckConstraint('(ev_delete > ev_create)'),
                CheckConstraint('(ev_delete IS NULL) = (active = 1)'),
                CheckConstraint('(active IS NULL) OR (active = 1)'))

    transition_type_tbl = Table('transition_type', meta,
                Column('ev_create', BIGINT, primary_key=True, nullable=False),
                Column('ev_delete', BIGINT, nullable=True),
                Column('active',    BOOLEAN, nullable=True),
                Column('trans_id',  VARCHAR, primary_key=True, nullable=False),
                Column('type',      INTEGER, nullable=False),
                ForeignKeyConstraint(['active', 'trans_id'],
                    ['transition.active', 'transition.trans_id']),
                ForeignKeyConstraint(['ev_create'], ['events.event_id']),
                ForeignKeyConstraint(['ev_delete'], ['events.event_id']),
                UniqueConstraint('active', 'trans_id'),
                CheckConstraint('0 <= type AND type <= 2'),
                CheckConstraint('(ev_delete > ev_create)'),
                CheckConstraint('(ev_delete IS NULL) = (active = 1)'),
                CheckConstraint('(active IS NULL) OR (active = 1)'))

    return meta

