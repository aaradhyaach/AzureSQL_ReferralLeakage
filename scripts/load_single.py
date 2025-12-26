import argparse
import sys
import pandas as pd
import pyodbc
from datetime import datetime
import time

def connect(server, database, username, password, timeout=60):
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        f"Connection Timeout={timeout};"
    )
    # autocommit False so we control commits
    return pyodbc.connect(conn_str, autocommit=False)

def get_table_columns(cursor, schema, table):
    cursor.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, (schema, table))
    cols = [r[0] for r in cursor.fetchall()]
    if not cols:
        raise RuntimeError(f"Table {schema}.{table} does not exist or has no columns.")
    return cols

def normalize_df(df):
    return df.replace({"": None})

def truncate_table(cursor, schema, table):
    cursor.execute(f"TRUNCATE TABLE [{schema}].[{table}];")

def current_rowcount(cursor, schema, table):
    cursor.execute(f"SELECT COUNT(*) FROM [{schema}].[{table}];")
    return int(cursor.fetchone()[0])

def insert_chunk(cursor, schema, table, df, table_cols):
    # Validate columns align
    csv_cols = list(df.columns)
    csv_lower = {c.lower(): c for c in csv_cols}
    sql_lower = [c.lower() for c in table_cols]

    missing = [c for c in table_cols if c.lower() not in csv_lower]
    extra = [c for c in csv_cols if c.lower() not in set(sql_lower)]
    if missing:
        raise ValueError(f"Missing columns for {table}: {missing}")
    if extra:
        raise ValueError(f"Extra columns in CSV for {table}: {extra}")

    ordered_cols = [csv_lower[c.lower()] for c in table_cols]
    df = df[ordered_cols]

    cols_sql = ", ".join(f"[{c}]" for c in table_cols)
    placeholders = ", ".join("?" for _ in table_cols)
    sql = f"INSERT INTO [{schema}].[{table}] ({cols_sql}) VALUES ({placeholders})"

    rows = list(df.itertuples(index=False, name=None))
    cursor.fast_executemany = True
    cursor.executemany(sql, rows)

def main():
    ap = argparse.ArgumentParser(description="Load ONLY claims_transactions.csv into raw.claims_transactions (resumable).")
    ap.add_argument("--server", required=True)
    ap.add_argument("--database", required=True)
    ap.add_argument("--username", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--csv", required=True, help="Path to claims_transactions.csv")
    ap.add_argument("--schema", default="raw")
    ap.add_argument("--table", default="claims_transactions")
    ap.add_argument("--chunksize", type=int, default=20000)
    ap.add_argument("--commit_every", type=int, default=5, help="Commit every N chunks (default: 5)")
    ap.add_argument("--truncate", action="store_true", help="Truncate the table before loading")
    ap.add_argument("--resume", action="store_true", help="Resume based on existing rowcount by skipping already-inserted rows")
    ap.add_argument("--max_retries", type=int, default=8, help="Retries on network failure")
    args = ap.parse_args()

    print(f"[{datetime.now().isoformat(timespec='seconds')}] Starting single-table load: {args.csv} -> {args.schema}.{args.table}")

    # Connect
    conn = connect(args.server, args.database, args.username, args.password)
    cur = conn.cursor()

    try:
        table_cols = get_table_columns(cur, args.schema, args.table)

        if args.truncate:
            print("Truncating target table...")
            truncate_table(cur, args.schema, args.table)
            conn.commit()

        skip_rows = 0
        if args.resume:
            # Rowcount equals number of already inserted rows since we insert sequentially from file
            already = current_rowcount(cur, args.schema, args.table)
            skip_rows = already
            print(f"Resume ON: table currently has {already} rows. Will skip that many rows in CSV.")

        # Reader (skip rows without reading entire file)
        # NOTE: skiprows uses 0-based lines; we need to keep header, so we skip 1..(skip_rows)
        # If skip_rows=0, this is safe.
        skip = range(1, skip_rows + 1) if skip_rows > 0 else None

        reader = pd.read_csv(
            args.csv,
            chunksize=args.chunksize,
            dtype=str,
            keep_default_na=False,
            low_memory=False,
            skiprows=skip
        )

        total_inserted = 0
        pending_chunks = 0
        chunk_index = 0

        for chunk in reader:
            chunk_index += 1
            chunk = normalize_df(chunk)

            # Retry loop per chunk (reconnect if hotspot drops)
            attempt = 0
            while True:
                try:
                    insert_chunk(cur, args.schema, args.table, chunk, table_cols)
                    pending_chunks += 1
                    total_inserted += len(chunk)

                    if pending_chunks >= args.commit_every:
                        conn.commit()
                        pending_chunks = 0

                    print(f"  chunk {chunk_index}: inserted {len(chunk)} rows (total inserted this run: {total_inserted})")
                    break  # success
                except pyodbc.OperationalError as e:
                    # Network drop / 08S01 etc.
                    attempt += 1
                    if attempt > args.max_retries:
                        raise
                    print(f"  Network/ODBC error on chunk {chunk_index} (attempt {attempt}/{args.max_retries}): {e}")
                    print("  Reconnecting and retrying this chunk in 5 seconds...")
                    time.sleep(5)

                    # Close dead connection safely
                    try:
                        cur.close()
                    except Exception:
                        pass
                    try:
                        conn.close()
                    except Exception:
                        pass

                    # Reconnect
                    conn = connect(args.server, args.database, args.username, args.password)
                    cur = conn.cursor()

        # Final commit
        if pending_chunks > 0:
            conn.commit()

        print(f"[{datetime.now().isoformat(timespec='seconds')}] DONE. Inserted this run: {total_inserted}")

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
