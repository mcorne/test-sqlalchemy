from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey

engine = create_engine("sqlite:///database.sqlite3", echo=True)
metadata = MetaData()
users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
    Column("fullname", String),
)

users.drop(engine, checkfirst=True)
users.create(engine, checkfirst=True)
# metadata.create_all(checkfirst=True)

ins = users.insert().values(name="jack", fullname="Jack Jones")
print(str(ins))
print(ins.compile().params)

conn = engine.connect()
result = conn.execute(ins)
