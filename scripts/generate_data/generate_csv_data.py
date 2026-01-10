import csv
import os
import random
from datetime import datetime, timedelta

products = {
    1: 65, 2: 52, 3: 50, 4: 22, 5: 37, 6: 180, 7: 220, 8: 165, 9: 620, 10: 275,
    11: 140, 12: 120, 13: 135, 14: 90, 15: 75, 16: 60, 17: 45, 18: 110, 19: 95, 20: 130,
    21: 85, 22: 55, 23: 40, 24: 70, 25: 160, 26: 210, 27: 190, 28: 115, 29: 98, 30: 150,
    31: 125, 32: 175, 33: 90, 34: 105, 35: 140, 36: 155, 37: 165, 38: 185, 39: 200, 40: 225,
    41: 250, 42: 275, 43: 300, 44: 325, 45: 350, 46: 375, 47: 400, 48: 420, 49: 450, 50: 480,
    51: 510, 52: 540, 53: 560, 54: 590, 55: 620, 56: 650, 57: 680, 58: 710, 59: 740, 60: 770,
    61: 800, 62: 820, 63: 850, 64: 880, 65: 910, 66: 940, 67: 970, 68: 1000, 69: 1025, 70: 1050,
    71: 1080, 72: 1100, 73: 1125, 74: 1150, 75: 1180, 76: 1200, 77: 1225, 78: 1250, 79: 1280, 80: 1300,
    81: 85, 82: 95, 83: 110, 84: 130, 85: 90, 86: 160, 87: 140, 88: 120, 89: 48, 90: 42,
    91: 78, 92: 320, 93: 60, 94: 620, 95: 165, 96: 95, 97: 110, 98: 55, 99: 140, 100: 155
}

sales_persons = {
    121: [1, 2, 3, 4],
    122: [6, 7, 8, 9],
    123: [11, 12, 13, 14],
    124: [16, 17, 18, 19]
}

start_date = datetime(2021, 1, 1)
end_date = datetime(2025, 12, 31)

file_location = "filepath_to_folder"
csv_file_path = os.path.join(file_location, "sales_data.csv")

with open(csv_file_path, "w", newline="") as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(["customer_id", "store_id", "product_id", "sales_person_id", "sales_date", "price", "quantity", "total_cost"])

    for r in range(1, 1000001):
        customer_id = random.randint(1, 500)
        store_id = random.randint(121, 124)
        product_id = random.randint(1, 100)
        price = products[product_id]
        sales_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        sales_person_id = random.choice(sales_persons[store_id])
        quantity = random.randint(1, 10)
        total_cost = price * quantity

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
    
print("CSV file generated successfully.")

