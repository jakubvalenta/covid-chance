import psycopg2


def db_connect(**kwargs):
    return psycopg2.connect(**kwargs)


def db_create_table(conn, table: str):
    cur = conn.cursor()
    try:
        cur.execute(
            f'''
CREATE TABLE {table} (
  update_id TEXT,
  url TEXT,
  line TEXT,
  inserted TIMESTAMP DEFAULT NOW()
);
CREATE INDEX index_{table}_update_id ON {table} (update_id);
'''
        )
    except psycopg2.ProgrammingError as e:
        if e.pgcode == psycopg2.errorcodes.DUPLICATE_TABLE:
            pass
        else:
            raise
    cur.close()


def db_select(conn, table: str, **kwargs) -> tuple:
    cur = conn.cursor()
    params = ' AND '.join(f'{k} = %s' for k in kwargs.keys())
    values = tuple(kwargs.values())
    cur.execute(f'SELECT * FROM {table} WHERE {params} LIMIT 1;', values)
    row = cur.fetchone()
    cur.close()
    return row


def db_insert(conn, table: str, **kwargs):
    cur = conn.cursor()
    columns = ', '.join(kwargs.keys())
    placeholders = ', '.join('%s' for _ in kwargs.values())
    values = tuple(kwargs.values())
    cur.execute(
        f'INSERT INTO {table} ({columns}) VALUES ({placeholders});', values,
    )
    cur.close()


def db_count(conn, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM {table};')
    count = int(cur.fetchone()[0])
    cur.close()
    return count

    db_connect,
    db_create_table,
    db_insert,
    db_select,
