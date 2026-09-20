from datetime import datetime
from typing import Iterator

from configs.settings import settings


def fetch_data(connection, last_updated: datetime, last_id: int, batch_size: int) -> Iterator[list[dict]]:
    """Read source rows in deterministic order using the composite checkpoint."""
    query = f"""
        SELECT ID, name, category, price, last_updated
        FROM `{settings.mysql_table}`
        WHERE (last_updated > %s)
           OR (last_updated = %s AND ID > %s)
        ORDER BY last_updated ASC, ID ASC
        LIMIT %s
    """

    while True:
        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute(query, (last_updated, last_updated, last_id, batch_size))
            rows = cursor.fetchall()
        finally:
            cursor.close()

        if not rows:
            return

        yield rows
        last_updated = rows[-1]["last_updated"]
        last_id = int(rows[-1]["ID"])


def create_connection():
    import mysql.connector

    return mysql.connector.connect(
        host=settings.mysql_host,
        port=settings.mysql_port,
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_db,
    )
