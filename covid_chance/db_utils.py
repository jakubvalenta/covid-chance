import psycopg2


def db_connect(**kwargs):
    return psycopg2.connect(**kwargs)


def db_select(conn, table: str, **kwargs) -> tuple:
    cur = conn.cursor()
    params = ' AND '.join(f'{k} = %s' for k in kwargs.keys())
    values = tuple(kwargs.values())
    cur.execute(f'SELECT * FROM {table} WHERE {params} LIMIT 1;', values)
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


def db_count(conn, table: str) -> int:
    cur = conn.cursor()
    cur.execute(f'SELECT COUNT(*) FROM {table};')
    count = int(cur.fetchone()[0])
    cur.close()
    return count
