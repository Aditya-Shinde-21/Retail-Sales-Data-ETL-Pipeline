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
csv_file_path = os.path.join(file_location, "sales_data.csv")

with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["customer_id", "store_id", "product_id", "sales_person_id", "sales_date", "price", "quantity", "total_cost"])

    for _ in range(1, 10000001):
        customer_id = random.randint(1, 1000)
        store_id = random.randint(121, 124)
        product_id = random.randint(1, 500)
        price = id_price_dict[product_id]
        sales_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        total_cost = round(price * quantity, 2)

        csvwriter.writerow([customer_id, store_id, product_id, sales_person_id, sales_date.strftime("%Y-%m-%d"), price, quantity, total_cost])

    # Manually generating invalid records
    csvwriter.writerow(["", 122, 5, 6, "2022-10-22", 55, 2, 110])
    csvwriter.writerow([501, 122, 5, 6, "2022-10-22", "", 2, 110])
    csvwriter.writerow([502, 122, "", 6, "2022-10-22", 55, 2, 110])
    csvwriter.writerow([503, 122, 5, "", "2022-10-22", 55, 2, 110])
    csvwriter.writerow([504, 122, 5, 6, "", 55, 2, 110])
    csvwriter.writerow([504, 122, 5, 6, "2022-10-22", 55, "", 110])
    csvwriter.writerow([504, 122, 5, 6, "2022-10-22", 55, -4, 110])
    csvwriter.writerow([504, 122, 5, 6, "2022-10-22", -3, 2, 110])

    # Manually generating bad records
    csvwriter.writerow(["", 122, 5, 6, "2022-10-22", 55, 2, 110, 67])
    csvwriter.writerow([501, 122, 5, 6, "22-10-10", "", 2, 110])
    csvwriter.writerow([502, 122, "", 6, "2022-10-22", 55, 2, 110, 43, 88])
    csvwriter.writerow(["cus4", 122, 5, "", "2022-10-22", 55, 2, 110])
    csvwriter.writerow([504, "str4", 5, 6, "", 55, 2, 110])
    csvwriter.writerow([504, 122, "pro6", 6, "2022-10-22", 55, "", 110])
    csvwriter.writerow([504, 122, 5, "sp22", "2022-10-22", 55, -4, 110])
    csvwriter.writerow([504, 122, 5, 6, "2022-10-22", "fiftyfive", 2, 110])

print("CSV file generated successfully.")