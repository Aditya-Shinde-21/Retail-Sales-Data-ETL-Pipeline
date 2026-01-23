import json
import csv
import os
import random
from datetime import datetime, timedelta

with open("D:\DE_Project_Files\products_id_price.json", "r") as f:
    product_data = json.load(f)

id_price_dict = {
    item["id"]: item["current_price"]
    for item in product_data
}

sales_persons = {
    121: [1, 2, 3, 4],
    122: [6, 7, 8, 9],
    123: [11, 12, 13, 14],
    124: [16, 17, 18, 19]
}

start_date = datetime(2021, 1, 1)
end_date = datetime(2025, 12, 31)

file_location = "D:\\DE_Project_Files\\sales_data_to_s3\\"
csv_file_path = os.path.join(file_location, "sales_data_with_extra_columns.csv")

with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["customer_id", "store_id", "product_id", "sales_person_id", "sales_date", "price", "quantity", "total_cost", "payment_mode"])

    for r in range(1, 10000):
        customer_id = random.randint(1, 500)
        store_id = random.randint(121, 124)
        product_id = random.randint(1, 88)
        price = id_price_dict[product_id]
        sales_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        total_cost = round(price * quantity, 2)
        payment_mode = random.choice(["cash", "UPI", "Debit Card", "Credit Card"])

        csvwriter.writerow([customer_id, store_id, product_id, sales_person_id, sales_date.strftime("%Y-%m-%d"), price, quantity, total_cost, payment_mode])

print("CSV file generated successfully.")