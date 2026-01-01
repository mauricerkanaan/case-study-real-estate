import sqlite3
from sqlite3 import Error

# ====== Variables (edit these) ======
db_path = "D:/whaw/case-study-real-estate/db/case-study.db"

# Tables to clear (delete all rows)
table_names = [
    # "data_source_SALES",
    # "data_source_LEADS",
    "dwh_SALES",
    "dwh_LEADS",
]

use_vacuum = False  # Optional: reclaim disk space after deleting (can take time)

# ====== Script ======
def main():
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        # Fetch all existing table names once (faster + cleaner)
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        existing_tables = {row[0] for row in cur.fetchall()}

        total_deleted = 0

        for table_name in table_names:
            if table_name not in existing_tables:
                print(f"Skipping: table '{table_name}' does not exist in {db_path}")
                continue

            # Delete all rows (quote table name safely)
            sql = f'DELETE FROM "{table_name}";'
            cur.execute(sql)

            deleted = cur.rowcount  # may be -1 in some SQLite scenarios, but usually works
            if deleted == -1:
                # Fallback to count rows if rowcount isn't reliable
                cur.execute(f'SELECT changes();')
                deleted = cur.fetchone()[0]

            total_deleted += deleted
            print(f"Deleted {deleted} rows from '{table_name}'.")

        conn.commit()
        print(f"\nDone. Total deleted rows across tables: {total_deleted}")

        if use_vacuum:
            conn.execute("VACUUM;")
            print("VACUUM completed.")

    except (Error, ValueError) as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
