"""
E-commerce Analytics Pipeline
Author: Khurshid Shaik
Description: ETL pipeline for Brazilian e-commerce data analysis
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import warnings

warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found. Please set it in your .env file.")

def load_and_clean_data():
    """Load and clean Kaggle datasets"""
    orders = pd.read_csv('data/olist_orders_dataset.csv')
    order_items = pd.read_csv('data/olist_order_items_dataset.csv')
    customers = pd.read_csv('data/olist_customers_dataset.csv')
    products = pd.read_csv('data/olist_products_dataset.csv')
    sellers = pd.read_csv('data/olist_sellers_dataset.csv')
    payments = pd.read_csv('data/olist_order_payments_dataset.csv')
    reviews = pd.read_csv('data/olist_order_reviews_dataset.csv')

    # Clean customers
    customers_clean = customers.drop_duplicates(subset=['customer_id'])

    # Clean products
    products_clean = products.copy()
    products_clean['product_category_name'] = products_clean['product_category_name'].fillna('unknown')
    products_clean['product_weight_g'] = products_clean['product_weight_g'].fillna(
        products_clean['product_weight_g'].median()
    )

    # Clean sellers
    sellers_clean = sellers.drop_duplicates(subset=['seller_id'])

    # Create fact table
    fact = order_items.merge(orders, on='order_id', how='inner')

    payments_agg = payments.groupby('order_id').agg({
        'payment_type': 'first',
        'payment_installments': 'max',
        'payment_value': 'sum'
    }).reset_index()
    fact = fact.merge(payments_agg, on='order_id', how='left')

    reviews_agg = reviews.groupby('order_id')['review_score'].mean().reset_index()
    fact = fact.merge(reviews_agg, on='order_id', how='left')

    # Convert dates
    date_cols = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for col in date_cols:
        if col in fact.columns:
            fact[col] = pd.to_datetime(fact[col], errors='coerce')

    # Filter for valid statuses
    fact = fact[fact['order_status'].isin(['delivered', 'shipped', 'invoiced'])]

    return customers_clean, products_clean, sellers_clean, fact

def create_database_schema(engine):
    """Create database schema and tables"""
    schema_sql = text("""
    CREATE SCHEMA IF NOT EXISTS ecommerce;

    DROP TABLE IF EXISTS ecommerce.fact_orders CASCADE;
    DROP TABLE IF EXISTS ecommerce.dim_customers CASCADE;
    DROP TABLE IF EXISTS ecommerce.dim_products CASCADE;
    DROP TABLE IF EXISTS ecommerce.dim_sellers CASCADE;

    CREATE TABLE ecommerce.dim_customers (
        customer_id VARCHAR(50) PRIMARY KEY,
        customer_unique_id VARCHAR(50),
        customer_zip_code_prefix VARCHAR(10),
        customer_city VARCHAR(100),
        customer_state VARCHAR(10)
    );

    CREATE TABLE ecommerce.dim_products (
        product_id VARCHAR(50) PRIMARY KEY,
        product_category_name VARCHAR(100),
        product_weight_g DECIMAL(10,2),
        product_length_cm DECIMAL(10,2),
        product_height_cm DECIMAL(10,2),
        product_width_cm DECIMAL(10,2)
    );

    CREATE TABLE ecommerce.dim_sellers (
        seller_id VARCHAR(50) PRIMARY KEY,
        seller_zip_code_prefix VARCHAR(10),
        seller_city VARCHAR(100),
        seller_state VARCHAR(10)
    );

    CREATE TABLE ecommerce.fact_orders (
        order_id VARCHAR(50),
        order_item_id INTEGER,
        customer_id VARCHAR(50),
        product_id VARCHAR(50),
        seller_id VARCHAR(50),
        order_status VARCHAR(50),
        order_purchase_timestamp TIMESTAMP,
        order_approved_at TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        price DECIMAL(10,2),
        freight_value DECIMAL(10,2),
        payment_type VARCHAR(50),
        payment_value DECIMAL(10,2),
        review_score DECIMAL(3,2),
        PRIMARY KEY (order_id, order_item_id)
    );

    CREATE INDEX idx_orders_customer ON ecommerce.fact_orders(customer_id);
    CREATE INDEX idx_orders_date ON ecommerce.fact_orders(order_purchase_timestamp);
    """)
    with engine.begin() as conn:
        conn.execute(schema_sql)

def insert_data(engine, customers, products, sellers, fact):
    """Insert data into PostgreSQL tables"""
    customers.to_sql("dim_customers", engine, schema="ecommerce", if_exists="append", index=False)
    products.to_sql("dim_products", engine, schema="ecommerce", if_exists="append", index=False)
    sellers.to_sql("dim_sellers", engine, schema="ecommerce", if_exists="append", index=False)
    fact.to_sql("fact_orders", engine, schema="ecommerce", if_exists="append", index=False)
    print("Data inserted successfully")

if __name__ == "__main__":
    engine = create_engine(DATABASE_URL)
    try:
        customers, products, sellers, fact = load_and_clean_data()
        create_database_schema(engine)
        insert_data(engine, customers, products, sellers, fact)
        print("Pipeline completed successfully")
    except Exception as e:
        print(f"Pipeline failed: {e}")
