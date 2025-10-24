##Dataset
This project uses the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

Download the 7 CSV files from Kaggle and place them inside the `data/` folder:
- olist_orders_dataset.csv  
- olist_order_items_dataset.csv  
- olist_customers_dataset.csv  
- olist_products_dataset.csv  
- olist_sellers_dataset.csv  
- olist_order_payments_dataset.csv  
- olist_order_reviews_dataset.csv  


E-commerce Analytics Pipeline
  End-to-end data analytics pipeline for Brazilian e-commerce dataset (100K+ orders, $20M+ revenue).
  
Project Overview
  Built a complete ETL pipeline that extracts, transforms, and loads real e-commerce data into PostgreSQL, enabling business intelligence through SQL analytics and interactive dashboards.
  
Key Features
  ⦁	ETL Pipeline: Automated data cleaning and transformation of 7 CSV files
  ⦁	Dimensional Modeling: Fact and dimension tables with proper indexing
  ⦁	Advanced Analytics: RFM segmentation, revenue trends, delivery performance
  ⦁	Data Quality: Handled nulls, duplicates, and data type conversions
  ⦁	Scalable Architecture: Cloud PostgreSQL database (Neon.tech)
  
Business Insights
  ⦁	97,896 orders analyzed across 2 years
  ⦁	$20M+ revenue processed
  ⦁	Customer segmentation identifying Champions, Loyal, and At-Risk customers
  ⦁	Product performance analysis across 71 categories
  ⦁	Delivery metrics tracking on-time performance
  
Tech Stack
  ⦁	Python: pandas, SQLAlchemy, Plotly
  ⦁	Database: PostgreSQL (Neon.tech)
  ⦁	Visualization: Plotly, Streamlit
  ⦁	Tools: Google Colab, Git/GitHub
