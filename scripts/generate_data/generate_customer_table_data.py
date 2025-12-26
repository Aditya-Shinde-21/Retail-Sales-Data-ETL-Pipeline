import random
from faker import Faker
from datetime import datetime

def get_insert_statements():
    fake = Faker('en_IN')
    insert_statements = []
    for _ in range(500):
        first_name = fake.first_name()
        last_name = fake.last_name()
        address = 'Delhi'
        pincode = random.choice(['122009', '110011', '201301', '400001'])
        phone_number = '91' + ''.join([str(random.randint(0, 9)) for _ in range(10)])
        joining_date = fake.date_between_dates(date_start=datetime(2020, 1, 1),
                                               date_end=datetime(2024, 12, 31)).strftime('%Y-%m-%d')

        insert_statements.append(
            f"INSERT INTO customer (first_name, last_name, address, pincode, phone_number, customer_joining_date) VALUES ('{first_name}', '{last_name}', '{address}', '{pincode}', '{phone_number}', '{joining_date}');")

    return insert_statements
