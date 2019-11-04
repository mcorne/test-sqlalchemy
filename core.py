from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    bindparam,
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

print("--------------------")
print("Insert Jack")
print("--------------------")
ins = users.insert().values(name="jack", fullname="Jack Jones")
print(str(ins))
print(ins.compile().params)
result = conn.execute(ins)
print(result.inserted_primary_key)

print("--------------------")
print("Insert Wendy")
print("--------------------")
ins = users.insert()
conn.execute(ins, id=2, name="wendy", fullname="Wendy Williams")

print("--------------------")
print("Insert addresses")
print("--------------------")
conn.execute(
    addresses.insert(),
    [
        {"user_id": 1, "email_address": "jack@yahoo.com"},
        {"user_id": 1, "email_address": "jack@msn.com"},
        {"user_id": 2, "email_address": "www@www.org"},
        {"user_id": 2, "email_address": "wendy@aol.com"},
    ],
)

print("--------------------")
print("Select users")
print("--------------------")
s = select([users])
result = conn.execute(s)
print(result)
for row in result:
    print(row)

print("--------------------")
print("Fetch one user (named columns)")
print("--------------------")
result = conn.execute(s)
row = result.fetchone()
print("name:", row["name"], "; fullname:", row["fullname"])

print("--------------------")
print("Fetch one user (indexed columns)")
print("--------------------")
row = result.fetchone()
print("name:", row[1], "; fullname:", row[2])

print("--------------------")
print("Fetch users (column object)")
print("--------------------")
for row in conn.execute(s):
    print("name:", row[users.c.name], "; fullname:", row[users.c.fullname])

# result sets with pending rows remaining should be explicitly closed for some database API
result.close()

print("--------------------")
print("Fetch one user (table column)")
print("--------------------")
s = select([users.c.name, users.c.fullname])
result = conn.execute(s)
for row in result:
    print(row)

print("--------------------")
print("Select (from/where)")
print("--------------------")
s = select([users, addresses]).where(users.c.id == addresses.c.user_id)
for row in conn.execute(s):
    print(row)
# "==" operator produces an object thanks to Python __eq__() builtin
print(str(users.c.id == addresses.c.user_id))

print("--------------------")
print("Operators")
print("--------------------")
print(users.c.id == addresses.c.user_id)
print(users.c.id == 7)
print((users.c.id == 7).compile().params)
print(users.c.id != 7)
print(users.c.name == None)
print("fred" > users.c.name)
print(users.c.id + addresses.c.id)
print(users.c.name + users.c.fullname)
print(users.c.name.op("tiddlywinks")("foo"))

print("--------------------")
print("Conjunctions (and, or, not)")
print("--------------------")
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

print("--------------------")
print("Conjunctions (& | ~)")
print("--------------------")
print(
    users.c.name.like("j%")
    & (users.c.id == addresses.c.user_id)
    & (
        (addresses.c.email_address == "wendy@aol.com")
        | (addresses.c.email_address == "jack@yahoo.com")
    )
    & ~(users.c.id > 5)
)

print("--------------------")
print("Select (where, and, or)")
print("--------------------")
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

print("--------------------")
print("Select (multiple where)")
print("--------------------")
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

print("--------------------")
print("Textual SQL")
print("--------------------")
s = text(
    "SELECT users.fullname || ', ' || addresses.email_address AS title "
    "FROM users, addresses "
    "WHERE users.id = addresses.user_id "
    "AND users.name BETWEEN :x AND :y "
    "AND (addresses.email_address LIKE :e1 "
    "OR addresses.email_address LIKE :e2)"
)
print(conn.execute(s, x="m", y="z", e1="%@aol.com", e2="%@msn.com").fetchall())

print("--------------------")
print("Parameter binding")
print("--------------------")
stmt = text("SELECT * FROM users WHERE users.name BETWEEN :x AND :y")
stmt = stmt.bindparams(x="m", y="z")
print(stmt)

print("--------------------")
print("Parameter binding with type")
print("--------------------")
stmt = text("SELECT * FROM users WHERE users.name BETWEEN :x AND :y")
stmt = stmt.bindparams(bindparam("x", type_=String), bindparam("y", type_=String))
result = conn.execute(stmt, {"x": "m", "y": "z"})
print(result)

print("--------------------")
print("Result columns")
print("--------------------")
stmt = text("SELECT id, name FROM users")
stmt = stmt.columns(users.c.id, users.c.name)
j = stmt.join(addresses, stmt.c.id == addresses.c.user_id)
new_stmt = select([stmt.c.id, addresses.c.id]).select_from(j).where(stmt.c.name == "x")
print(new_stmt)

print("--------------------")
print("Result columns with textual SQL")
print("--------------------")
stmt = text(
    "SELECT users.id, addresses.id, users.id, "
    "users.name, addresses.email_address AS email "
    "FROM users JOIN addresses ON users.id=addresses.user_id "
    "WHERE users.id = 1"
).columns(
    users.c.id,
    addresses.c.id,
    addresses.c.user_id,
    users.c.name,
    addresses.c.email_address,
)
result = conn.execute(stmt)
print(stmt)
row = result.fetchone()
print(row[addresses.c.email_address])
