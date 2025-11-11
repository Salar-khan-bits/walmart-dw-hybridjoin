Drop database if exists salarkhan;
create database salarkhan;
use salarkhan;


DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_supplier;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_month INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter_number INT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE dim_customer (
    customer_id BIGINT PRIMARY KEY,
    gender CHAR(1),
    age_range VARCHAR(10),
    occupation_code INT,
    city_category CHAR(1),
    stay_in_city_years VARCHAR(5),
    marital_status INT
);

CREATE TABLE dim_store (
    store_id INT PRIMARY KEY,
    store_name VARCHAR(100) NOT NULL,
    city_category CHAR(1)
);

CREATE TABLE dim_supplier (
    supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR(150) NOT NULL
);

CREATE TABLE dim_product (
    product_id VARCHAR(20) PRIMARY KEY,
    product_category VARCHAR(100),
    unit_price NUMERIC(10,2),
    store_id INT REFERENCES dim_store(store_id),
    supplier_id INT REFERENCES dim_supplier(supplier_id)
);

CREATE TABLE fact_sales (
    order_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL REFERENCES dim_customer(customer_id),
    product_id VARCHAR(20) NOT NULL REFERENCES dim_product(product_id),
    store_id INT NOT NULL REFERENCES dim_store(store_id),
    supplier_id INT NOT NULL REFERENCES dim_supplier(supplier_id),
    date_id DATE NOT NULL REFERENCES dim_date(date_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(10,2) NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id, customer_id, product_id)
);

CREATE INDEX idx_fact_sales_date ON fact_sales(date_id);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_id);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_id);
CREATE INDEX idx_fact_sales_store_supplier ON fact_sales(store_id, supplier_id);
CREATE INDEX idx_fact_sales_supplier ON fact_sales(supplier_id);

CREATE INDEX idx_dim_customer_city ON dim_customer(city_category);
CREATE INDEX idx_dim_customer_gender_age ON dim_customer(gender, age_range);
CREATE INDEX idx_dim_product_category ON dim_product(product_category);
CREATE INDEX idx_dim_store_name ON dim_store(store_name);

Select * from fact_sales;
