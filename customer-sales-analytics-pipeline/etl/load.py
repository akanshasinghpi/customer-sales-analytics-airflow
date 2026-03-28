from sqlalchemy import create_engine
from logger_config import logger

def load_data(df, customer_metrics):

    engine = create_engine("sqlite:///sales_pipeline.db")

    df.to_sql("sales", engine, if_exists="replace", index=False)

    customer_metrics.to_sql(
        "customer_metrics",
        engine,
        if_exists="replace",
        index=False
    )

    logger.info("Data loaded into database")
