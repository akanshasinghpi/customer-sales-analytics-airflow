import pandas as pd
from logger_config import logger

def transform_data(df):

    df = df.fillna(0)

    df["Date"] = pd.to_datetime(df["Date"])

    df["Revenue"] = df["Sales"]

    df["Month"] = df["Date"].dt.month

    customer_metrics = df.groupby("Customer_ID").agg({
        "Revenue": "sum",
        "Order_ID": "count"
    }).reset_index()

    customer_metrics.rename(columns={
        "Revenue": "Total_Revenue",
        "Order_ID": "Total_Orders"
    }, inplace=True)

    logger.info("Data transformed successfully")

    return df, customer_metrics
