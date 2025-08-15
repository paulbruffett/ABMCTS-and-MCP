import csv
import sqlite3
from pathlib import Path


def load_database(csv_path: Path | str) -> sqlite3.Connection:
    """Load a CSV file into an in-memory SQLite database.

    The CSV is imported into a table named ``electric_vehicle_population``.
    All columns are stored as TEXT for simplicity.
    """
    csv_path = Path(csv_path)
    conn = sqlite3.connect(":memory:")
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        columns_sql = ', '.join(f'[{h}] TEXT' for h in headers)
        conn.execute(f"CREATE TABLE electric_vehicle_population ({columns_sql})")
        placeholders = ', '.join('?' * len(headers))
        insert_sql = f"INSERT INTO electric_vehicle_population VALUES ({placeholders})"
        conn.executemany(insert_sql, reader)
    return conn


if __name__ == "__main__":
    default_csv = Path(__file__).parent / "data" / "Electric_Vehicle_Population_Data.csv"
    conn = load_database(default_csv)
    cursor = conn.cursor()
    print("Loaded data into an in-memory SQLite database.")
    print("Type SQL commands to interact with the data. Type 'exit' or 'quit' to leave.")
    while True:
        try:
            query = input("sql> ")
            if query.strip().lower() in {"exit", "quit"}:
                break
            rows = cursor.execute(query)
            for row in rows:
                print(row)
        except Exception as exc:  # pragma: no cover - simple interactive loop
            print(f"Error: {exc}")
