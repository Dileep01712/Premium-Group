import sqlite3

conn = sqlite3.connect("index.db")
with open("database_dump.txt", "w", encoding="utf-8") as f:
    for line in conn.iterdump():
        f.write(f"{line}\n")
print("Database dumped to database_dump.txt")
