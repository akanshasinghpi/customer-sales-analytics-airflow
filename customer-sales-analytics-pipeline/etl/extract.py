import pandas as pd
from logger_config import logger

def extract_data(file_path):

    df = pd.read_csv(file_path)

    logger.info("Data extracted successfully")
    logger.info(f"Dataset shape: {df.shape}")

    return df
