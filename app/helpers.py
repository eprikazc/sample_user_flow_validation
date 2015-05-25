import csv

from .models import Deal


def populate_deals(session, csv_file_name):
    '''Populates deals table from csv file'''
    deals = []
    with open(csv_file_name) as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            deals.append(Deal(
                material_group_id=row['Vegtable group #'],
                user_industry_id=row['Industry #'],
                bracket_id=row['Volume bracket'],
                location=row['Location'],
                date=row['Date'],
                price=row['Price'],
            ))
    session.add_all(deals)
    session.commit()