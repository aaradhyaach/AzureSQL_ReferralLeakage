import os
import argparse
import sys
import pandas as pd
import pyodbc
from datetime import datetime
from typing import Dict, Set, List

# Tune these for speed + stability (keeps fast_executemany ON)
CHUNKSIZE_BY_TABLE: Dict[str, int] = {
    "claims_transactions": 20000,
    "observations": 20000,
    "imaging_studies": 10000,
    # everything else falls back to default_chunksize
}

# Commit every N batches to reduce commit overhead (still safe)
COMMIT_EVERY_N_BATCHES_BY_TABLE: Dict[str, int] = {
    "claims_transactions": 5,  # commit every 5 chunks (e.g., 5 * 20k = 100k rows)
    "observations": 5,
    "imaging_studies": 3,
}

# Optional: define a stable load order (parents first). Others append after.
PREFERRED_LOAD_ORDER: List[str] = [
    "organizations.csv",
    "providers.csv",
    "payers.csv",
    "patients.csv",
    "payer_transitions.csv",
    "encounters.csv",
    "conditions.csv",
    "procedures.csv",
    "medications.csv",
    "immunizations.csv",
    "observations.csv",
    "imaging_studies.csv",
    "supplies.csv",
    "devices.csv",
    "allergies.csv",
    "careplans.csv",
    "claims.csv",
    "claims_transactions.csv",
]


def connect(server, database, username, password):
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    conn = pyodbc.connect(conn_str, autocommit=False)

    # IMPORTANT: keep these on the connection for fast inserts.
    # (pyodbc uses ANSI settings; this prevents some driver quirks with large batches)
    try:
        conn.add_output_converter(-150, lambda x: x)  # no-op for datetimeoffset if present
    except Exception:
        pass

    return conn


def get_table_columns(cursor, schema, table):
    cursor.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        (schema, table),
    )
    return [r[0] for r in cursor.fetchall()]


def truncate_table(cursor, schema, table):
    try:
        cursor.execute(f"TRUNCATE TABLE [{schema}].[{table}];")
    except pyodbc.Error:
        cursor.execute(f"DELETE FROM [{schema}].[{table}];")


def normalize_df(df):
    # Replace empty strings with NULLs for SQL insert
    return df.replace({"": None})


def insert_chunk(cursor, schema, table, df, table_cols):
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

    # Build rows efficiently
    rows = list(df.itertuples(index=False, name=None))

    # Keep fast_executemany ON (your requirement)
    cursor.fast_executemany = True
    cursor.executemany(sql, rows)


def load_csv(cursor, conn, schema, table, csv_path, default_chunksize, truncate):
    print(f"\nLoading {csv_path} → {schema}.{table}")

    table_cols = get_table_columns(cursor, schema, table)
    if not table_cols:
        raise RuntimeError(f"Table {schema}.{table} does not exist")

    if truncate:
        truncate_table(cursor, schema, table)
        conn.commit()

    chunksize = CHUNKSIZE_BY_TABLE.get(table, default_chunksize)
    commit_every = COMMIT_EVERY_N_BATCHES_BY_TABLE.get(table, 1)

    total = 0
    pending_batches = 0

    reader = pd.read_csv(
        csv_path,
        chunksize=chunksize,
        dtype=str,
        keep_default_na=False,
        low_memory=False,  # avoids mixed-type inference overhead (we force dtype=str anyway)
    )

    for i, chunk in enumerate(reader, start=1):
        chunk = normalize_df(chunk)
        insert_chunk(cursor, schema, table, chunk, table_cols)

        pending_batches += 1
        total += len(chunk)

        # Commit less frequently to reduce overhead (big speed improvement)
        if pending_batches >= commit_every:
            conn.commit()
            pending_batches = 0

        print(f"  Batch {i}: {len(chunk)} rows (chunksize={chunksize})")

    # Final commit if any batches left uncommitted
    if pending_batches > 0:
        conn.commit()

    print(f"  Total inserted: {total}")


def ordered_csv_files(csv_dir: str) -> List[str]:
    existing = {f for f in os.listdir(csv_dir) if f.lower().endswith(".csv")}
    ordered: List[str] = []
    used: Set[str] = set()

    # add preferred order files first if present
    for f in PREFERRED_LOAD_ORDER:
        if f in existing:
            ordered.append(f)
            used.add(f)

    # then add remaining files alphabetically
    for f in sorted(existing):
        if f not in used:
            ordered.append(f)

    return ordered


def main():
    ap = argparse.ArgumentParser(description="Load all CSVs in a folder into raw schema tables.")
    ap.add_argument("--server", required=True)
    ap.add_argument("--database", required=True)
    ap.add_argument("--username", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--csv_dir", required=True)
    ap.add_argument("--schema", default="raw")
    ap.add_argument("--chunksize", type=int, default=10000, help="Default chunksize (overridden per-table)")
    ap.add_argument("--truncate", action="store_true")
    args = ap.parse_args()

    if not os.path.isdir(args.csv_dir):
        print("CSV directory not found", file=sys.stderr)
        sys.exit(1)

    conn = connect(args.server, args.database, args.username, args.password)
    cursor = conn.cursor()

    csv_files = ordered_csv_files(args.csv_dir)

    if not csv_files:
        print("No CSV files found", file=sys.stderr)
        sys.exit(1)

    print(f"[{datetime.now().isoformat(timespec='seconds')}] Starting load")
    print(f"Default chunksize={args.chunksize}. Overrides: {CHUNKSIZE_BY_TABLE}")

    for file in csv_files:
        table = os.path.splitext(file)[0]
        csv_path = os.path.join(args.csv_dir, file)

        try:
            load_csv(
                cursor,
                conn,
                args.schema,
                table,
                csv_path,
                args.chunksize,
                args.truncate,
            )
        except Exception as e:
            print(f"\nFAILED loading {file}: {e}", file=sys.stderr)
            conn.rollback()
            break

    cursor.close()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
