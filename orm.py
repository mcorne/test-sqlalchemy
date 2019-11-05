from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import aliased, sessionmaker

Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    fullname = Column(String)
    nickname = Column(String)

    def __repr__(self):
        return "<User(name='%s', fullname='%s', nickname='%s')>" % (
            self.name,
            self.fullname,
            self.nickname,
        )


engine = create_engine("sqlite:///orm.sqlite3", echo=True)

Base.metadata.drop_all(engine, checkfirst=True)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

print("----------------------------------------")
print("Add")
print("----------------------------------------")
ed_user = User(name="ed", fullname="Ed Jones", nickname="edsnickname")
session.add(ed_user)
session.commit()

print("----------------------------------------")
print("ID")
print("----------------------------------------")
print(ed_user.id)

print("----------------------------------------")
print("Add many")
print("----------------------------------------")
session.add_all(
    [
        User(name="wendy", fullname="Wendy Williams", nickname="windy"),
        User(name="mary", fullname="Mary Contrary", nickname="mary"),
        User(name="fred", fullname="Fred Flintstone", nickname="freddy"),
    ]
)
session.commit()

print("----------------------------------------")
print("Rollback")
print("----------------------------------------")
ed_user.name = "Edwardo"
fake_user = User(name="fakeuser", fullname="Invalid", nickname="12345")
session.add(fake_user)
session.rollback()
print(ed_user.name)
print(session.query(User).filter(User.name.in_(["ed", "fakeuser"])).all())

print("----------------------------------------")
print("Query users")
print("----------------------------------------")
for instance in session.query(User).order_by(User.id):
    print(instance.name, instance.fullname)

print("----------------------------------------")
print("Query with columns")
print("----------------------------------------")
for name, fullname in session.query(User.name, User.fullname):
    print(name, fullname)

print("----------------------------------------")
print("Query tuples")
print("----------------------------------------")
for row in session.query(User, User.name).all():
    print(row.User, row.name)

print("----------------------------------------")
print("Query with label")
print("----------------------------------------")
for row in session.query(User.name.label("name_label")).all():
    print(row.name_label)

print("----------------------------------------")
print("Query with alias")
print("----------------------------------------")
user_alias = aliased(User, name="user_alias")
for row in session.query(user_alias, user_alias.name).all():
    print(row.user_alias)

print("----------------------------------------")
print("Query with order and limit")
print("----------------------------------------")
for u in session.query(User).order_by(User.id)[1:3]:
    print(u)

print("----------------------------------------")
print("Query with filter")
print("----------------------------------------")
for (name,) in session.query(User.name).filter_by(fullname="Ed Jones"):
    print(name)

print("----------------------------------------")
print("Query with filters")
print("----------------------------------------")
for user in (
    session.query(User).filter(User.name == "ed").filter(User.fullname == "Ed Jones")
):
    print(user)

print("----------------------------------------")
print("Operators")
print("----------------------------------------")
print(session.query(User).filter(User.name == "ed"))
print(session.query(User).filter(User.name != "ed"))
print(session.query(User).filter(User.name.like("%ed%")))
print(session.query(User).filter(User.name.ilike("%ed%")))
print(session.query(User).filter(User.name.in_(["ed", "wendy", "jack"])))
print(session.query(User).filter(~User.name.in_(["ed", "wendy", "jack"])))
