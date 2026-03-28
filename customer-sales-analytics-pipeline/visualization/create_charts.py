import pandas as pd
from matplotlib import pyplot as plt
from sqlalchemy import create_engine
import os

output_path = "/opt/airflow/customer-sales-analytics-pipeline/output"
os.makedirs(output_path, exist_ok=True)

# Connect to database
engine = create_engine("sqlite:///sales_pipeline.db")

# Load data
sales = pd.read_sql("SELECT * FROM sales", engine)

# Revenue by Region
region_sales = sales.groupby("Region")["Revenue"].sum()

plt.figure()
region_sales.plot(kind="bar")
plt.title("Revenue by Region")
plt.ylabel("Revenue")
plt.xlabel("Region")
plt.tight_layout()
plt.savefig(f"{output_path}/revenue_by_region.png")
plt.close()

# Monthly Revenue Trend
monthly_sales = sales.groupby("Month")["Revenue"].sum()

plt.figure()
monthly_sales.plot(kind="line", marker="o")
plt.title("Monthly Revenue Trend")
plt.ylabel("Revenue")
plt.xlabel("Month")
plt.tight_layout()
plt.savefig(f"{output_path}/monthly_sales_trend.png")
plt.close()

# Top Products
top_products = sales.groupby("Product")["Revenue"].sum().sort_values(ascending=False).head(5)

plt.figure()
top_products.plot(kind="bar")
plt.title("Top 5 Products by Revenue")
plt.ylabel("Revenue")
plt.xlabel("Product")
plt.tight_layout()
plt.savefig(f"{output_path}/top_products.png")
plt.close()

print("Charts created successfully")
