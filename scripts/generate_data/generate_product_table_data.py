import json
from pathlib import Path
import random
from faker import Faker
from datetime import datetime, timedelta

def get_insert_statements():
    # retail_products.json contains product_name: price key-value pairs
    with open("filepath\\to\\retail_products.json", "r") as f:
        products = json.load(f)

    fake = Faker('en_IN')
    insert_statements = []
    for product, price in products.items():
        name = product
        current_price = price
        old_price = round(random.uniform(0.9 * current_price, 1.1 * current_price), 2)

        created_date = fake.date_time_between(
            start_date="-2y", end_date="now"
        ).strftime("%Y-%m-%d %H:%M:%S")

        updated_date = fake.date_time_between(
            start_date=datetime.strptime(created_date, "%Y-%m-%d %H:%M:%S"),
            end_date="now"
        ).strftime("%Y-%m-%d %H:%M:%S")

        expiry_date = fake.date_between(
            start_date="+30d", end_date="+2y"
        ).strftime("%Y-%m-%d")
        insert_statements.append(f"INSERT INTO product (name, current_price, old_price, created_date, updated_date, expiry_date) VALUES ('{name}', '{current_price}', '{old_price}', '{created_date}', '{updated_date}', '{expiry_date}');")


    return insert_statements
