import csv
import sqlalchemy as sa

from math import fabs
from statistics import mean, stdev
from decimal import Decimal

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_defaults import Column

from .helpers import populate_deals

Base = declarative_base()


class Deal(Base):
    __lazy_options__ = {}
    __tablename__ = 'deal'
    id = Column(sa.Integer, primary_key=True)
    material_group_id = Column(sa.Integer, nullable=False)
    user_industry_id = Column(sa.Integer, nullable=False)
    bracket_id = Column(sa.Integer, nullable=False)
    location = Column(sa.Float, nullable=False)
    date = Column(sa.Float, nullable=False)
    price = Column(sa.Numeric, nullable=False)


def validate_price(session, group_id, industry_id, volume, location, item_date, price):
    price_query = session.query(Deal.price)
    if session.query(Deal.id) \
            .filter(
                Deal.material_group_id == group_id,
                Deal.user_industry_id == industry_id,
                Deal.bracket_id == volume).count() >= 30:
        price_query = price_query.filter(
            Deal.material_group_id == group_id,
            Deal.user_industry_id == industry_id,
            Deal.bracket_id == volume)
    elif session.query(Deal.id) \
            .filter(
                Deal.material_group_id == group_id,
                Deal.user_industry_id == industry_id).count() >= 30:
        price_query = price_query.filter(
            Deal.material_group_id == group_id,
            Deal.user_industry_id == industry_id)
    else:
        price_query = price_query.filter(
            Deal.material_group_id == group_id)
    price_query = price_query.order_by(
        sa.func.abs(Deal.location-location),
        sa.func.abs(Deal.date-item_date),
    ).limit(30)
    return stat_analysis(price, [row[0] for row in price_query])


def stat_analysis(target_price, selected_prices):
    res = {
        'less_than_30': len(selected_prices) < 30,
        'valid': False
    }
    if fabs(target_price - mean(selected_prices)) < stdev(selected_prices):
        res['valid'] = True
    return res


if __name__ == '__main__':
    engine = sa.create_engine('sqlite:///:memory:')
    # engine = sa.create_engine('postgres://guy:guy@localhost/guy')
    Session = sa.orm.sessionmaker(bind=engine)
    Session.configure(bind=engine)
    session = Session()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    populate_deals(session, '../data.csv')
    print(validate_price(session, 4, 31, 0, 250, 400, Decimal(200)))
