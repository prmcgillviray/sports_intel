import duckdb

con = duckdb.connect("db/features.duckdb")
cols = con.execute("PRAGMA table_info('participants')").fetchall()
print("BEFORE:", cols)

names = [c[1] for c in cols]
if "team_abbrev" not in names:
    con.execute("ALTER TABLE participants ADD COLUMN team_abbrev TEXT;")
    print("Added participants.team_abbrev")

cols2 = con.execute("PRAGMA table_info('participants')").fetchall()
print("AFTER:", cols2)

con.close()
