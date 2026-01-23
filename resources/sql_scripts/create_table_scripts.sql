--staging_table
CREATE TABLE staging_table (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255),
    file_location VARCHAR(255),
    created_date TIMESTAMP ,
    updated_date TIMESTAMP ,
    status VARCHAR(1)
);

--Customer table
CREATE TABLE customer (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    address VARCHAR(255),
    pincode VARCHAR(10),
    phone_number VARCHAR(13),
    customer_joining_date DATE
);

--store table
CREATE TABLE store (
    id INT PRIMARY KEY,
    address VARCHAR(255),
    store_pincode VARCHAR(10),
    store_manager_name VARCHAR(100),
    store_opening_date DATE
);

--data of store table
INSERT INTO store (id, address, store_pincode, store_manager_name, store_opening_date)
VALUES
    (121,'Delhi', '122009', 'Sahil Dhariwal', '2017-01-15'),
    (122,'Delhi', '110011', 'Ankit Kumar', '2018-08-10'),
    (123,'Delhi', '201301', 'Sagar Yadav', '2019-01-20'),
    (124,'Delhi', '400001', 'Pooja Joshi', '2019-05-05');


-- product table
CREATE TABLE product (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    current_price DECIMAL(10, 2),
    old_price DECIMAL(10, 2),
    created_date TIMESTAMP ,
    updated_date TIMESTAMP ,
    expiry_date DATE
);


--sales team table
CREATE TABLE sales_team (
    id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    manager_id INT,
    is_manager CHAR(1),
    address VARCHAR(255),
    pincode VARCHAR(10),
    joining_date DATE
);


--sales team data
INSERT INTO sales_team (first_name, last_name, manager_id, is_manager, address, pincode, joining_date)
VALUES
    ('Rahul', 'Verma', 5, 'N', 'Delhi', '122009', '2017-01-15'),
    ('Priya', 'Singh', 5, 'N', 'Delhi', '122009', '2017-01-15'),
    ('Amit', 'Sharma', 5, 'N', 'Delhi', '122009', '2017-01-15'),
    ('Sumit', 'Mittal', 5, 'N', 'Delhi', '122009', '2017-01-15'),
    ('Sahil', 'Dhariwal', null, 'Y', 'Delhi', '122009', '2017-01-15'),
    ('Sneha', 'Gupta', 10, 'N', 'Delhi', '110011', '2018-08-10'),
    ('Neha', 'Kumar', 10, 'N', 'Delhi', '110011', '2018-08-10'),
    ('Vijay', 'Yadav', 10, 'N', 'Delhi', '110011', '2018-08-10'),
    ('Sakshi', 'Jain', 10, 'N', 'Delhi', '110011', '2018-08-10'),
    ('Ankit', 'Kumar', null, 'Y', 'Delhi', '110011', '2018-08-10'),
    ('Anita', 'Malhotra', 15, 'N', 'Delhi', '201301', '2019-01-20'),
    ('Alok', 'Rajput', 15, 'N', 'Delhi', '201301', '2019-01-20'),
    ('Monica', 'Jain', 15, 'N', 'Delhi', '201301', '2019-01-20'),
    ('Rajesh', 'Gupta', 15, 'N', 'Delhi', '201301', '2019-01-20'),
    ('Sagar', 'Yadav', null, 'Y', 'Delhi', '201301', '2019-01-20'),
    ('Taufiq', 'Ahmed', 20, 'N', 'Delhi', '400001', '2019-05-05'),
    ('Manish', 'Kumar', 20, 'N', 'Delhi', '400001', '2019-05-05'),
    ('Alex', 'Varghese', 20, 'N', 'Delhi', '400001', '2019-05-05'),
    ('Shashank', 'Mishra', 20, 'N', 'Delhi', '400001', '2019-05-05'),
    ('Pooja', 'Joshi', null, 'Y', 'Delhi', '400001', '2019-05-05');

-- fact table sales
CREATE TABLE sales (
    customer_id INT,
    store_id INT,
    sales_person_id INT,
    product_id INT,
    price DECIMAL(10, 2),
    quantity INT,
    total_cost DECIMAL(12, 2),
    sales_date DATE,

    -- Foreign key constraints
    CONSTRAINT fk_customer
        FOREIGN KEY (customer_id) REFERENCES customer(customer_id),

    CONSTRAINT fk_store
        FOREIGN KEY (store_id) REFERENCES store(id),

    CONSTRAINT fk_sales_person
        FOREIGN KEY (sales_person_id) REFERENCES sales_team(id),

    CONSTRAINT fk_product
        FOREIGN KEY (product_id) REFERENCES product(id)
);


--Customer monthly sales table
CREATE TABLE customers_monthly_sales (
    customer_id INT ,
    full_name VARCHAR(100),
    address VARCHAR(200),
    phone_number VARCHAR(20),
    sales_date_year INT,
    sales_date_month INT,
    total_sales DECIMAL(10, 2)
);


#--sales person incentive table
CREATE TABLE sales_person_monthly_incentive (
    store_id INT,
    sales_person_id INT,
    full_name VARCHAR(255),
    sales_date_year INT,
    sales_date_month INT,
    total_sales DECIMAL(10, 2),
    incentive DECIMAL(10, 2)
);

--s3 bucket table
CREATE TABLE s3_bucket_info (
    id INT AUTO_INCREMENT PRIMARY KEY,
    bucket_name VARCHAR(255),
    access_key_id VARCHAR(255),
    secret_key VARCHAR(255),
    created_date TIMESTAMP ,
    updated_date TIMESTAMP ,
    status VARCHAR(20)
);