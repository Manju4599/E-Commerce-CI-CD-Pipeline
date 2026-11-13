import logging

from src.extract import extract_tables
from src.transform import run_transformation
from src.s3_ingestion import (upload_bronze_files,  upload_silver_orders)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

def main():
    logging.info("Pipeline Started !")
    
    try:
        extract_tables()
        logging.info("Exctraction completed !")
        
        upload_bronze_files()
        logging.info("Bronze INgestion completed !")
        
        run_transformation()
        logging.info("Transformation and validation completed !")
        
        upload_silver_orders()
        logging.info("Silver ingestion completed !")
        
        logging.info("Pipeline completed succesfully !")
        
    except Exception:
        
        logging.info("Pipeline Failed !")
        raise
    
if __name__ == "__main__":
    main()