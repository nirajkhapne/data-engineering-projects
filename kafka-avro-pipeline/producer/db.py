import mysql.connector
import os

def fetch_data(last_ts):
    conn = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB")
    )

    cursor = conn.cursor(dictionary=True)

    cursor.execute(f"""
        SELECT * FROM product
        WHERE last_updated > '{last_ts}'
        ORDER BY last_updated
    """)

    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return rows
