from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
)
from sqlalchemy.sql import and_, not_, or_, select, text

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

s = select([users, addresses]).where(users.c.id == addresses.c.user_id)
for row in conn.execute(s):
    print(row)
# "==" operator produces an object thanks to Python __eq__() builtin
print(str(users.c.id == addresses.c.user_id))

print(users.c.id == addresses.c.user_id)
print(users.c.id == 7)
print((users.c.id == 7).compile().params)
print(users.c.id != 7)
print(users.c.name == None)
print("fred" > users.c.name)
print(users.c.id + addresses.c.id)
print(users.c.name + users.c.fullname)
print(users.c.name.op("tiddlywinks")("foo"))

print(
    and_(
        users.c.name.like("j%"),
        users.c.id == addresses.c.user_id,
        or_(
            addresses.c.email_address == "wendy@aol.com",
            addresses.c.email_address == "jack@yahoo.com",
        ),
        not_(users.c.id > 5),
    )
)

print(
    users.c.name.like("j%")
    & (users.c.id == addresses.c.user_id)
    & (
        (addresses.c.email_address == "wendy@aol.com")
        | (addresses.c.email_address == "jack@yahoo.com")
    )
    & ~(users.c.id > 5)
)

s = select(
    [(users.c.fullname + ", " + addresses.c.email_address).label("title")]
).where(
    and_(
        users.c.id == addresses.c.user_id,
        users.c.name.between("m", "z"),
        or_(
            addresses.c.email_address.like("%@aol.com"),
            addresses.c.email_address.like("%@msn.com"),
        ),
    )
)
print(conn.execute(s).fetchall())
print(s)
print(s.compile().params)

s = (
    select([(users.c.fullname + ", " + addresses.c.email_address).label("title")])
    .where(users.c.id == addresses.c.user_id)
    .where(users.c.name.between("m", "z"))
    .where(
        or_(
            addresses.c.email_address.like("%@aol.com"),
            addresses.c.email_address.like("%@msn.com"),
        )
    )
)
print(conn.execute(s).fetchall())
print(s)
print(s.compile().params)

s = text(
    "SELECT users.fullname || ', ' || addresses.email_address AS title "
    "FROM users, addresses "
    "WHERE users.id = addresses.user_id "
    "AND users.name BETWEEN :x AND :y "
    "AND (addresses.email_address LIKE :e1 "
    "OR addresses.email_address LIKE :e2)"
)
print(conn.execute(s, x='m', y='z', e1='%@aol.com', e2='%@msn.com').fetchall())
