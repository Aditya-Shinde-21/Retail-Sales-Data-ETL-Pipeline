import json
from pathlib import Path
import mysql.connector
from scripts.generate_data import generate_product_table_data
from scripts.generate_data import generate_customer_table_data

config_path = "D:\DE_Project_Files\mysql_connector.json"
config_file = Path(config_path)

if not config_file.exists():
    raise FileNotFoundError(f"Config file not found: {config_path}")

with config_file.open("r", encoding="utf-8") as f:
    config = json.load(f)

connection = mysql.connector.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
            autocommit=False,
            raise_on_warnings=True,
            connection_timeout=30,
            charset="utf8mb4",
            collation="utf8mb4_unicode_ci"
        )
cursor = connection.cursor()


# Write generated customer data to MySQL database
statements = generate_customer_table_data.get_insert_statements()
for statement in statements:
    cursor.execute(statement)
    connection.commit()
print("Customer table created successfully.")

# Write generated product data to MySQL database
statements = generate_product_table_data.get_insert_statements()
for statement in statements:
    cursor.execute(statement)
    connection.commit()
print("Product table created successfully.")

print("Customer table:")
cursor.execute("SELECT * FROM customer LIMIT 21;")
query = cursor.fetchall()
for row in query:
    print(*row, sep=' | | ')

print("Product table:")
cursor.execute("SELECT * FROM product LIMIT 21;")
query = cursor.fetchall()
for row in query:
    print(*row, sep=' | | ')

cursor.close()
connection.close()