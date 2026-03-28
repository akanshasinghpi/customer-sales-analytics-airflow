from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from logger_config import logger

logger.info("Pipeline started")

data = extract_data("data/raw/sales_data.csv")

clean_data, customer_metrics = transform_data(data)

load_data(clean_data, customer_metrics)

logger.info("Pipeline completed successfully")

import subprocess

subprocess.run(["python3", "reporting/generate_report.py"])
