from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import duckdb

@retry(stop=stop_after_attempt(5), wait=wait_fixed(1),
       retry=retry_if_exception_type(duckdb.Error), reraise=True)
def safe_executemany(con, sql, batch, na_token):
    # Only this exact token is considered missing
    def normalise(v):
        if isinstance(v, str):
            return None if v == na_token else v
        if isinstance(v, (bytes, bytearray)):  # just in case
            try:
                s = v.decode()
            except Exception:
                return v
            return None if s == na_token else s
        return v  # numbers, None, etc.
    cleaned = [tuple(normalise(x) for x in row) for row in batch]
    con.executemany(sql, cleaned)

@retry(stop=stop_after_attempt(5), wait=wait_fixed(1),
       retry=retry_if_exception_type(duckdb.Error), reraise=True)
def safe_commit(con):
    con.execute("COMMIT")

def _quote(val: str) -> str:
    """Safely quote a string for DuckDB SQL."""
    return "'" + val.replace("'", "''") + "'"

@retry(stop=stop_after_attempt(5), wait=wait_fixed(1),
       retry=retry_if_exception_type(duckdb.Error), reraise=True)
def safe_copy(con, table_name, out_path, delimiter, compressed, na_rep):
    # Ensure strings are quoted safely for SQL
    out_path_quoted = _quote(out_path)
    delimiter_quoted = _quote(delimiter)
    na_rep_quoted = _quote(na_rep)

    copy_opts = f"FORMAT CSV, HEADER, DELIMITER {delimiter_quoted}, NULL {na_rep_quoted}"
    if compressed:
        copy_opts += ", COMPRESSION 'gzip'"

    sql = f"COPY {table_name} TO {out_path_quoted} ({copy_opts})"
    con.execute(sql)