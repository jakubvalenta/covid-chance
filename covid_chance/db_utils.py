from typing import Optional

import psycopg2


def db_connect(**kwargs):
    return psycopg2.connect(**kwargs)


def db_select(conn, table: str, **where) -> tuple:
    cur = conn.cursor()
    where_str = ' AND '.join(f'{k} = %s' for k in where.keys())
    where_values = tuple(where.values())
    cur.execute(
        f'SELECT * FROM {table} WHERE {where_str} LIMIT 1;', where_values
    )
    row = cur.fetchone()
    cur.close()
    return row


def db_insert(conn, table: str, cur=None, **kwargs):
    if cur is None:
        cur_ = conn.cursor()
    else:
        cur_ = cur
    columns = ', '.join(kwargs.keys())
    placeholders = ', '.join('%s' for _ in kwargs.values())
    values = tuple(kwargs.values())
    cur_.execute(
        f'INSERT INTO {table} ({columns}) VALUES ({placeholders});', values,
    )
    if cur is None:
        conn.commit()
        cur_.close()


def db_update(conn, table: str, where: dict, cur=None, **kwargs):
    if cur is None:
        cur_ = conn.cursor()
    else:
        cur_ = cur
    placeholders = ', '.join(f'{k} = %s' for k in kwargs.keys())
    where_str = ' AND '.join(f'{k} = %s' for k in where.keys())
    values = tuple(kwargs.values())
    where_values = tuple(where.values())
    cur_.execute(
        f'UPDATE {table} SET {placeholders} WHERE {where_str};',
        values + where_values,
    )
    if cur is None:
        conn.commit()
        cur_.close()


def db_update_or_insert(conn, table: str, where: dict, cur=None, **kwargs):
    if db_select(conn, table, **where):
        db_update(conn, table, where, cur=cur, **kwargs)
    else:
        db_insert(conn, table, cur=cur, **{**where, **kwargs})


def db_count(conn, table: str, where: Optional[dict] = None) -> int:
    cur = conn.cursor()
    if where:
        where_str = ' AND '.join(f'{k} = %s' for k in where.keys())
        where_values = tuple(where.values())
        cur.execute(
            f'SELECT COUNT(*) FROM {table} WHERE {where_str};', where_values
        )
    else:
        cur.execute(f'SELECT COUNT(*) FROM {table};')
    count = int(cur.fetchone()[0])
    cur.close()
    return count
