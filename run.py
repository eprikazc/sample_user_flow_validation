import sqlalchemy as sa

from app.helpers import populate_deals
from app.models import Base, validate_price


if __name__ == '__main__':
    engine = sa.create_engine('sqlite:///:memory:')
    # engine = sa.create_engine('postgres://guy:guy@localhost/guy')
    Session = sa.orm.sessionmaker(bind=engine)
    Session.configure(bind=engine)
    session = Session()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    populate_deals(session, 'data.csv')
    print(validate_price(
        session,
        group_id=4,
        industry_id=31,
        volume=0,
        location=250,
        item_date=400,
        price=200))
