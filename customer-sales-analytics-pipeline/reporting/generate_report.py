import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("sqlite:///sales_pipeline.db")

# Load data
sales = pd.read_sql("SELECT * FROM sales", engine)

# Top Customers
top_customers = sales.groupby("Customer_ID")["Revenue"].sum().sort_values(ascending=False).head(5)

# Revenue by Region
region_sales = sales.groupby("Region")["Revenue"].sum().sort_values(ascending=False)

# Monthly Sales
monthly_sales = sales.groupby("Month")["Revenue"].sum()

# Generate report text
report = []

report.append("=== SALES ANALYTICS REPORT ===\n")

report.append("Top 5 Customers by Revenue:\n")
report.append(top_customers.to_string())
report.append("\n\n")

report.append("Revenue by Region:\n")
report.append(region_sales.to_string())
report.append("\n\n")

report.append("Monthly Revenue Trend:\n")
report.append(monthly_sales.to_string())
report.append("\n\n")

# Save report
with open("/opt/airflow/customer-sales-analytics-pipeline/output/analytics_report.txt", "w") as f:
    f.write("\n".join(report))

print("Report generated successfully")
