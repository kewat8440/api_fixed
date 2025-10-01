
from fastapi import FastAPI, HTTPException, Query
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

# Database connection
def get_connection():
    return psycopg2.connect(
        dbname="excel_db",
        user="postgres",
        password="12345678",
        host="localhost",
        port="5432"
    )

@app.get("/data/{table_name}")
def get_data(
    table_name: str,
    page: int = Query(1, ge=1),   # Page number (default 1, must be >=1)
    size: int = Query(5, ge=1)    # Page size (default 5, must be >=1)
):
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Calculate offset
        offset = (page - 1) * size

        # Fetch rows
        cursor.execute(f"SELECT * FROM {table_name} LIMIT %s OFFSET %s;", (size, offset))
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        return {
            "table": table_name,
            "page": page,
            "size": size,
            "rows": rows
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
