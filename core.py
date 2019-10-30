from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.sql import select

engine = create_engine("sqlite:///database.sqlite3", echo=True)
metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
    Column("fullname", String),
)

addresses = Table(
    "addresses",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("user_id", None, ForeignKey("users.id")),
    Column("email_address", String, nullable=False),
)

# users.drop(engine, checkfirst=True)
# users.create(engine, checkfirst=True)
metadata.drop_all(engine, checkfirst=True)
metadata.create_all(engine, checkfirst=True)

conn = engine.connect()

ins = users.insert().values(name="jack", fullname="Jack Jones")
print(str(ins))
print(ins.compile().params)
result = conn.execute(ins)
print(result.inserted_primary_key)

ins = users.insert()
conn.execute(ins, id=2, name="wendy", fullname="Wendy Williams")

conn.execute(
    addresses.insert(),
    [
        {"user_id": 1, "email_address": "jack@yahoo.com"},
        {"user_id": 1, "email_address": "jack@msn.com"},
        {"user_id": 2, "email_address": "www@www.org"},
        {"user_id": 2, "email_address": "wendy@aol.com"},
    ],
)

s = select([users])
result = conn.execute(s)
print(result)
for row in result:
    print(row)

result = conn.execute(s)
row = result.fetchone()
print("name:", row["name"], "; fullname:", row["fullname"])

row = result.fetchone()
print("name:", row[1], "; fullname:", row[2])

for row in conn.execute(s):
    print("name:", row[users.c.name], "; fullname:", row[users.c.fullname])

# result sets with pending rows remaining should be explicitly closed for some database API
result.close()

s = select([users.c.name, users.c.fullname])
result = conn.execute(s)
for row in result:
    print(row)
