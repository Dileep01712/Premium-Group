import sqlite3


def search_files(query: str):
    conn = sqlite3.connect("index.db")
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
                SELECT original_title, message_id, quality FROM files WHERE files MATCH ?
            """,
            (query + "*",),
        )
        results = cursor.fetchall()
    except sqlite3.OperationalError as e:
        print(f"❌ Error while searching: {e}")
        results = []
    finally:
        conn.close()
    return results
