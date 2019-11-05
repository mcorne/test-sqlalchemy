from sqlalchemy import (Column, ForeignKey, Integer, MetaData, String, Table,
                        and_, bindparam, cast, create_engine, desc, func,
                        select, text)
from sqlalchemy.sql import (and_, except_, func, literal_column, not_, or_,
                            select, table, text, union)

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

engine = create_engine("sqlite:///core.sqlite3", echo=True)
# users.drop(engine, checkfirst=True)
# users.create(engine, checkfirst=True)
metadata.drop_all(engine, checkfirst=True)
metadata.create_all(engine, checkfirst=True)

conn = engine.connect()

print("----------------------------------------")
print("Insert Jack")
print("----------------------------------------")
ins = users.insert().values(name="jack", fullname="Jack Jones")
print(str(ins))
print(ins.compile().params)
result = conn.execute(ins)
print(result.inserted_primary_key)

print("----------------------------------------")
print("Insert Wendy")
print("----------------------------------------")
ins = users.insert()
conn.execute(ins, id=2, name="wendy", fullname="Wendy Williams")

print("----------------------------------------")
print("Insert addresses")
print("----------------------------------------")
conn.execute(
    addresses.insert(),
    [
        {"user_id": 1, "email_address": "jack@yahoo.com"},
        {"user_id": 1, "email_address": "jack@msn.com"},
        {"user_id": 2, "email_address": "www@www.org"},
        {"user_id": 2, "email_address": "wendy@aol.com"},
    ],
)

print("----------------------------------------")
print("Select users")
print("----------------------------------------")
s = select([users])
result = conn.execute(s)
print(result)
for row in result:
    print(row)

print("----------------------------------------")
print("Fetch one user (named columns)")
print("----------------------------------------")
result = conn.execute(s)
row = result.fetchone()
print("name:", row["name"], "; fullname:", row["fullname"])

print("----------------------------------------")
print("Fetch one user (indexed columns)")
print("----------------------------------------")
row = result.fetchone()
print("name:", row[1], "; fullname:", row[2])

print("----------------------------------------")
print("Fetch users (column object)")
print("----------------------------------------")
for row in conn.execute(s):
    print("name:", row[users.c.name], "; fullname:", row[users.c.fullname])

# result sets with pending rows remaining should be explicitly closed for some database API
result.close()

print("----------------------------------------")
print("Fetch one user (table column)")
print("----------------------------------------")
s = select([users.c.name, users.c.fullname])
result = conn.execute(s)
for row in result:
    print(row)

print("----------------------------------------")
print("Select (from/where)")
print("----------------------------------------")
s = select([users, addresses]).where(users.c.id == addresses.c.user_id)
for row in conn.execute(s):
    print(row)
# "==" operator produces an object thanks to Python __eq__() builtin
print(str(users.c.id == addresses.c.user_id))

print("----------------------------------------")
print("Operators")
print("----------------------------------------")
print(users.c.id == addresses.c.user_id)
print(users.c.id == 7)
print((users.c.id == 7).compile().params)
print(users.c.id != 7)
print(users.c.name == None)
print("fred" > users.c.name)
print(users.c.id + addresses.c.id)
print(users.c.name + users.c.fullname)
print(users.c.name.op("tiddlywinks")("foo"))

print("----------------------------------------")
print("Conjunctions (and, or, not)")
print("----------------------------------------")
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

print("----------------------------------------")
print("Conjunctions (& | ~)")
print("----------------------------------------")
print(
    users.c.name.like("j%")
    & (users.c.id == addresses.c.user_id)
    & (
        (addresses.c.email_address == "wendy@aol.com")
        | (addresses.c.email_address == "jack@yahoo.com")
    )
    & ~(users.c.id > 5)
)

print("----------------------------------------")
print("Select (where, and, or)")
print("----------------------------------------")
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

print("----------------------------------------")
print("Select (multiple where)")
print("----------------------------------------")
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

print("----------------------------------------")
print("Textual SQL")
print("----------------------------------------")
s = text(
    "SELECT users.fullname || ', ' || addresses.email_address AS title "
    "FROM users, addresses "
    "WHERE users.id = addresses.user_id "
    "AND users.name BETWEEN :x AND :y "
    "AND (addresses.email_address LIKE :e1 "
    "OR addresses.email_address LIKE :e2)"
)
print(conn.execute(s, x="m", y="z", e1="%@aol.com", e2="%@msn.com").fetchall())

print("----------------------------------------")
print("Parameter binding")
print("----------------------------------------")
stmt = text("SELECT * FROM users WHERE users.name BETWEEN :x AND :y")
stmt = stmt.bindparams(x="m", y="z")
print(stmt)

print("----------------------------------------")
print("Parameter binding with type")
print("----------------------------------------")
stmt = text("SELECT * FROM users WHERE users.name BETWEEN :x AND :y")
stmt = stmt.bindparams(bindparam("x", type_=String), bindparam("y", type_=String))
result = conn.execute(stmt, {"x": "m", "y": "z"})
print(result)

print("----------------------------------------")
print("Result columns")
print("----------------------------------------")
stmt = text("SELECT id, name FROM users")
stmt = stmt.columns(users.c.id, users.c.name)
j = stmt.join(addresses, stmt.c.id == addresses.c.user_id)
new_stmt = select([stmt.c.id, addresses.c.id]).select_from(j).where(stmt.c.name == "x")
print(new_stmt)

print("----------------------------------------")
print("Result columns with textual SQL")
print("----------------------------------------")
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

print("----------------------------------------")
print("Fragments (text)")
print("----------------------------------------")
s = select([text("users.fullname || ', ' || addresses.email_address AS title")]
    ).where(
        and_(
            text("users.id = addresses.user_id"),
            text("users.name BETWEEN 'm' AND 'z'"),
            text(
                "(addresses.email_address LIKE :x "
                "OR addresses.email_address LIKE :y)")
        )
    ).select_from(text('users, addresses'))
print(conn.execute(s, x='%@aol.com', y='%@msn.com').fetchall())

print("----------------------------------------")
print("Fragments (table, column)")
print("----------------------------------------")
s = select([
        literal_column("users.fullname", String) +
        ', ' +
        literal_column("addresses.email_address").label("title")
    ]).where(
        and_(
            literal_column("users.id") == literal_column("addresses.user_id"),
            text("users.name BETWEEN 'm' AND 'z'"),
            text(
                "(addresses.email_address LIKE :x OR "
                "addresses.email_address LIKE :y)")
        )
    ).select_from(table('users')).select_from(table('addresses'))
print(conn.execute(s, x='%@aol.com', y='%@msn.com').fetchall())

print("----------------------------------------")
print("Group and order")
print("----------------------------------------")
stmt = select([
        addresses.c.user_id,
        func.count(addresses.c.id).label('num_addresses')
    ]).group_by("user_id"
    ).order_by("user_id", desc("num_addresses"))

print(conn.execute(stmt).fetchall())

print("----------------------------------------")
print("Alias")
print("----------------------------------------")
a1 = addresses.alias()
a2 = addresses.alias()
s = select([users]).\
        where(and_(
            users.c.id == a1.c.user_id,
            users.c.id == a2.c.user_id,
            a1.c.email_address == 'jack@msn.com',
            a2.c.email_address == 'jack@yahoo.com'
        ))
print(conn.execute(s).fetchall())

print("----------------------------------------")
print("Join")
print("----------------------------------------")
print(users.join(addresses))
print(users.join(addresses, addresses.c.email_address.like(users.c.name + '%')))

print("----------------------------------------")
print("Select with join")
print("----------------------------------------")
s = select([users.c.fullname]).select_from(users.join(addresses, addresses.c.email_address.like(users.c.name + '%')))
print(conn.execute(s).fetchall())

print("----------------------------------------")
print("Outer join")
print("----------------------------------------")
s = select([users.c.fullname]).select_from(users.outerjoin(addresses))
print(s)

print("----------------------------------------")
print("Parameter binding with type")
print("----------------------------------------")
s = select([users, addresses]).where(
        or_(
          users.c.name.like(
                 bindparam('name', type_=String) + text("'%'")),
          addresses.c.email_address.like(
                 bindparam('name', type_=String) + text("'@%'"))
        )
     ).select_from(users.outerjoin(addresses)
     ).order_by(addresses.c.id)
print(conn.execute(s, name='jack').fetchall())

print("----------------------------------------")
print("Function")
print("----------------------------------------")
print(func.now())
print(func.concat('x', 'y'))
print(func.current_timestamp())
print(conn.execute(select([func.max(addresses.c.email_address, type_=String).label('maxemail')])).scalar())

print("----------------------------------------")
print("Cast")
print("----------------------------------------")
s = select([cast(users.c.id, String)])
print(conn.execute(s).fetchall())

print("----------------------------------------")
print("Union")
print("----------------------------------------")
u = union(
    addresses.select().where(addresses.c.email_address == 'foo@bar.com'),
    addresses.select().where(addresses.c.email_address.like('%@yahoo.com')),
    ).order_by(addresses.c.email_address)
print(conn.execute(u).fetchall())

print("----------------------------------------")
print("Update")
print("----------------------------------------")
stmt = users.update().values(fullname="Fullname: " + users.c.name)
print(conn.execute(stmt))

print("----------------------------------------")
print("Inserts")
print("----------------------------------------")
stmt = users.insert().values(name=bindparam('_name') + " .. name")
conn.execute(stmt, [
    {'id':4, '_name':'name1'},
    {'id':5, '_name':'name2'},
    {'id':6, '_name':'name3'},
])

print("----------------------------------------")
print("Update where")
print("----------------------------------------")
stmt = users.update().where(users.c.name == 'jack').values(name='ed')
print(conn.execute(stmt))

print("----------------------------------------")
print("Updates")
print("----------------------------------------")
stmt = users.update().where(users.c.name == bindparam('oldname')).values(name=bindparam('newname'))
print(conn.execute(stmt, [
    {'oldname':'jack', 'newname':'ed'},
    {'oldname':'wendy', 'newname':'mary'},
    {'oldname':'jim', 'newname':'jake'},
]))

print("----------------------------------------")
print("Deletes")
print("----------------------------------------")
stmt = users.delete().where(users.c.name.like("name%"))
result = conn.execute(stmt)
print(result)
print(result.rowcount)
