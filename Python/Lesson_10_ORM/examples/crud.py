from examples.simple_model import SessionLocal, User

if __name__ == "__main__":
    user = User(name="aleksei", surname="random", age=18)
    session = SessionLocal()
    for user in (
        session.query(User)
        .filter(User.name == "aleksei")
        .filter(User.age == 18)
        .limit(2)
        .all()
    ):
        print(user.id)
