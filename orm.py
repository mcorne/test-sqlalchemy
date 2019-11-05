from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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
print("Add user")
print("----------------------------------------")
ed_user = User(name="ed", fullname="Ed Jones", nickname="edsnickname")
session.add(ed_user)
session.commit()

print("----------------------------------------")
print("Get user ID")
print("----------------------------------------")
print(ed_user.id)

print("----------------------------------------")
print("Add users")
print("----------------------------------------")
session.add_all(
    [
        User(name="wendy", fullname="Wendy Williams", nickname="windy"),
        User(name="mary", fullname="Mary Contrary", nickname="mary"),
        User(name="fred", fullname="Fred Flintstone", nickname="freddy"),
    ]
)

print("----------------------------------------")
print("Rollback")
print("----------------------------------------")
ed_user.name = "Edwardo"
fake_user = User(name="fakeuser", fullname="Invalid", nickname="12345")
session.add(fake_user)
session.rollback()
print(ed_user.name)

print("----------------------------------------")
print("Query user")
print("----------------------------------------")
print(session.query(User).filter(User.name.in_(["ed", "fakeuser"])).all())

