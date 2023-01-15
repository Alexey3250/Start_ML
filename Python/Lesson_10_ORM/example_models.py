class Facility:
    facid = Column(Integer)
    name = Column(String)
    ...


class Member:
    memid = Column(Integer)
    surname = Column(String)


from sqlalchemy import select

stmt = select(User).where(User.name == "spongebob")
