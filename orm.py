from sqlalchemy import (Column, ForeignKey, Integer, String, and_,
                        create_engine, func, or_, text)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import aliased, relationship, sessionmaker
from sqlalchemy.sql import exists

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


class Address(Base):
    __tablename__ = "addresses"

    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"))

    user = relationship("User", back_populates="addresses")

    def __repr__(self):
        return "<Address(email_address='%s')>" % self.email_address


User.addresses = relationship("Address", order_by=Address.id, back_populates="user")


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
print("id = " + str(ed_user.id))

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
print("name = " + ed_user.name)
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
print("----------------------------------------")
print(session.query(User).filter(User.name != "ed"))
print("----------------------------------------")
print(session.query(User).filter(User.name.like("%ed%")))
print("----------------------------------------")
print(session.query(User).filter(User.name.ilike("%ed%")))
print("----------------------------------------")
print(session.query(User).filter(User.name.in_(["ed", "wendy", "jack"])))
print("----------------------------------------")
print(session.query(User).filter(~User.name.in_(["ed", "wendy", "jack"])))
print("----------------------------------------")
print(session.query(User).filter(User.name == None))
print("----------------------------------------")
print(session.query(User).filter(User.name != None))
print("----------------------------------------")
print(session.query(User).filter(and_(User.name == "ed", User.fullname == "Ed Jones")))
print("----------------------------------------")
print(session.query(User).filter(or_(User.name == "ed", User.name == "wendy")))
print("----------------------------------------")
print(session.query(User).filter(User.name.match("wendy")))

print("----------------------------------------")
print("Lists and scalars")
print("----------------------------------------")
query = session.query(User).filter(User.name.like("%ed")).order_by(User.id)
print("all = " + str(query.all()))
print("----------------------------------------")
print("first = " + str(query.first()))
print("----------------------------------------")
print("one = " + str(query.limit(1).one()))
print("----------------------------------------")
query = session.query(User.id).filter(User.name == "ed").order_by(User.id)
print("scalar = " + str(query.scalar()))

print("----------------------------------------")
print("Textual SQL")
print("----------------------------------------")
for user in session.query(User).filter(text("id<224")).order_by(text("id")).all():
    print(user.name)

print("----------------------------------------")
print("Parameter binding")
print("----------------------------------------")
print(
    session.query(User)
    .filter(text("id<:value and name=:name"))
    .params(value=224, name="fred")
    .order_by(User.id)
    .one()
)

print("----------------------------------------")
print("From statement")
print("----------------------------------------")
print(
    session.query(User)
    .from_statement(text("SELECT * FROM users where name=:name"))
    .params(name="ed")
    .all()
)

print("----------------------------------------")
print("Statement columns")
print("----------------------------------------")
stmt = text("SELECT name, id, fullname, nickname FROM users where name=:name")
stmt = stmt.columns(User.name, User.id, User.fullname, User.nickname)
print(session.query(User).from_statement(stmt).params(name="ed").all())

print("----------------------------------------")
print("Returned columns")
print("----------------------------------------")
stmt = text("SELECT name, id FROM users where name=:name")
stmt = stmt.columns(User.name, User.id)
print(session.query(User.id, User.name).from_statement(stmt).params(name="ed").all())

print("----------------------------------------")
print("Counting")
print("----------------------------------------")
print(session.query(User).filter(User.name.like("%ed")).count())
print("----------------------------------------")
print(session.query(func.count("*")).select_from(User).scalar())
print("----------------------------------------")
print(session.query(func.count(User.id)).scalar())

print("----------------------------------------")
print("Counting group")
print("----------------------------------------")
print(session.query(func.count(User.name), User.name).group_by(User.name).all())

print("----------------------------------------")
print("Add user addresses")
print("----------------------------------------")
jack = User(name='jack', fullname='Jack Bean', nickname='gjffdd')
jack.addresses = [
    Address(email_address='jack@google.com'),
    Address(email_address='j25@yahoo.com')]
session.add(jack)
session.commit()

print("----------------------------------------")
print("Query user addresses")
print("----------------------------------------")
jack = session.query(User).filter_by(name='jack').one()
print(jack)
print("----------------------------------------")
print(jack.addresses)

print("----------------------------------------")
print("Query both users and addresses")
print("----------------------------------------")
for u, a in session.query(User, Address).filter(User.id==Address.user_id).filter(Address.email_address=='jack@google.com').all():
    print(u)
    print(a)

print("----------------------------------------")
print("Query with join")
print("----------------------------------------")
print(session.query(User).join(Address).filter(Address.email_address=='jack@google.com').all())
print("----------------------------------------")
print(session.query(User).join(Address, User.id==Address.user_id))
print("----------------------------------------")
print(session.query(Address).join(User.addresses))
print("----------------------------------------")
print(session.query(User).join(Address, User.addresses))
print("----------------------------------------")
print(session.query(User).join('addresses'))
print("----------------------------------------")
print(session.query(User).outerjoin(User.addresses))

print("----------------------------------------")
print("Joins and alias")
print("----------------------------------------")
adalias1 = aliased(Address)
adalias2 = aliased(Address)
for username, email1, email2 in \
    session.query(User.name, adalias1.email_address, adalias2.email_address).\
        join(adalias1, User.addresses).\
        join(adalias2, User.addresses).\
        filter(adalias1.email_address=='jack@google.com').\
        filter(adalias2.email_address=='j25@yahoo.com'):
    print(username, email1, email2)

print("----------------------------------------")
print("Subqueries")
print("----------------------------------------")
stmt = session.query(Address.user_id, func.count('*').label('address_count')).group_by(Address.user_id).subquery()
for u, count in session.query(User, stmt.c.address_count).outerjoin(stmt, User.id==stmt.c.user_id).order_by(User.id):
    print(u, count)

print("----------------------------------------")
print("Selecting from subqueries")
print("----------------------------------------")
stmt = session.query(Address).filter(Address.email_address != 'j25@yahoo.com').subquery()
adalias = aliased(Address, stmt)
for user, address in session.query(User, adalias).join(adalias, User.addresses):
    print(user)
    print(address)

print("----------------------------------------")
print("Exists")
print("----------------------------------------")
stmt = exists().where(Address.user_id==User.id)
for name, in session.query(User.name).filter(stmt):
    print(name)