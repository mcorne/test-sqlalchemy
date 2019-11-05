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

print("----------------------------------------")
print("Add user")
print("----------------------------------------")
session = Session()
ed_user = User(name="ed", fullname="Ed Jones", nickname="edsnickname")
session.add(ed_user)
session.commit()

print("----------------------------------------")
print("Get user ID")
print("----------------------------------------")
print(ed_user.id)
