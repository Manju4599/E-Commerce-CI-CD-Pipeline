
import logging

from src.logging_config import configure_logging
from src.extract import extract_tables
from src.transform import run_transformation
from src.s3_ingestion import upload_bronze_files, upload_silver_orders


logger = logging.getLogger(__name__)


def main():
    configure_logging()

    logger.info("Pipeline started")

    try:
        extract_tables()
        logger.info("Extraction completed")

        upload_bronze_files()
        logger.info("Bronze ingestion completed")

        run_transformation()
        logger.info("Transformation and validation completed")

        upload_silver_orders()
        logger.info("Silver ingestion completed")

        logger.info("Pipeline completed successfully")

    except Exception:
        logger.exception("Pipeline failed")
        raise


if __name__ == "__main__":
    main()
