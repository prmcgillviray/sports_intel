import duckdb

DB_PATH = "db/features.duckdb"

def cols(con, table):
    return [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]

con = duckdb.connect(DB_PATH)

before = con.execute("PRAGMA table_info('event_participants')").fetchall()
print("event_participants BEFORE:", before)

names = cols(con, "event_participants")

if "role" not in names:
    con.execute("ALTER TABLE event_participants ADD COLUMN role TEXT;")
    print("Added event_participants.role")

if "is_home" not in names:
    con.execute("ALTER TABLE event_participants ADD COLUMN is_home BOOLEAN;")
    print("Added event_participants.is_home")

after = con.execute("PRAGMA table_info('event_participants')").fetchall()
print("event_participants AFTER:", after)

con.close()
