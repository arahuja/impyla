try:
    import pandas as pd    
except ImportError:
    print "Failed to import pandas"

try:
    import numpy as np    
except ImportError:
    print "Failed to import numpy"

from util import generate_random_table_name

def as_pandas(cursor):
    names = [metadata[0] for metadata in cursor.description]
    return pd.DataFrame([dict(zip(names, row)) for row in cursor], 
                        columns=names)

def join(df, impala_table, conn, on=[], impala_table_cols = []):
    df_table = generate_random_table_name()
    write_frame(df, df_table, conn)
    query = _create_join_query(df_table, impala_table, impala_table_cols = impala_table_cols, on = on)
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    result = as_pandas(cur)
    _drop_table(df_table, conn)

    return pd.merge([df, result])

def write_frame(df, table_name, conn, if_exists='fail'):
    exists = _table_exists(table_name, conn)
    if if_exists == 'fail' and exists:
        raise ValueError("Table '%s' already exists." % table_name)
    elif if_exists == 'replace' and exists:
        _drop_table(table_name, conn)
        exists = False
    if not exists:
        cur = conn.cursor()
        cur.execute(_get_create_statement(df, table_name))
        cur.close()

    cur = conn.cursor()
    _write_query(df, table_name, cur)
    cur.close()
    conn.commit()

def _create_join_query(df_table, impala_table, impala_table_cols = [], on = []):
    join_query = """SELECT %(col)s FROM %(impala_table)s JOIN %(df)s"""
    if len(impala_table_cols) > 0:
        select_cols = ",".join(impala_table_cols)
    else:
        select_cols = "*"
    where_clause = ""
    if len(on) > 0:
        where_clause += " WHERE " + " and ".join( "{0}.{2} = {1}.{2}".format(df_table, impala_table, col) for col in on)

    query = join_query % {'col' : select_cols, 'df' : df_table, 'impala_table' : impala_table} + where_clause
    return query
    

def _get_type(pytype):
    if issubclass(pytype, np.floating):
        'float'

    if issubclass(pytype, np.integer):
        'bigint'

    if issubclass(pytype, np.datetime64):
        return 'timestamp'

    if issubclass(pytype, np.bool_):
        return 'boolean'

    return 'string'

def _clean_column_names(col, replace_char = '_'):
    restricted_chars = [".", " "]
    for c in restricted_chars:
        col = col.replace(c, replace_char)
    return col.strip()

def _table_exists(name, conn):
    cur = conn.cursor()
    cur.execute("show tables like '%s'" % name.lower())
    rows = len(cur)
    cur.close()
    return rows > 0    

def _get_create_statement(frame, name):
    safe_columns = [ _clean_column_names(col) for col in frame.dtypes.index]
    column_types = zip(safe_columns, map(_get_type, frame.dtypes))
    columns = ',\n  '.join('`%s` %s' % x for x in column_types)
    create_template = """CREATE TABLE %(name)s ( %(columns)s)"""
    create_statement = create_template % {'name': name, 'columns': columns}
    return create_statement

def _drop_table(table_name, conn):
    cur = conn.cursor()
    cur.execute("DROP TABLE %s" % table_name)
    cur.close()

def _write_query(df, table_name, cursor):
    clean_names = [_clean_column_names(col) for col in df.columns]
    
    bracketed_names = ['`' + column + '`' for column in clean_names]
    data = tuple( tuple(zip(x, clean_names)) for x in df.values)
    col_names = ','.join(bracketed_names)

    insert_query = "INSERT INTO %s (%s)" % (table_name, col_names)
    insert_query += " VALUES " + "%s"
    insert_query = insert_query % ",".join(str(d) for d in data)
    cursor.execute(insert_query)
    cursor.close()
