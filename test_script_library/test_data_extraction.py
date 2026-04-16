import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging
import oracledb
import os
import sys
import pytest
import inspect

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# logging configuration
from common_utilities.utilities import verify_expected_result_as_file_to_actual_result_as_database_table, \
verify_expected_result_as_database_to_actual_result_as_database_table

from config.etl_configuration import *

logging.basicConfig(
    filename="logs/etl_process.log",
    filemode='a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

'''
# database connection
oracle_conn = create_engine(f"oracle+cx_oracle://{ORACLE_USER}:{ORACLE_PASSWORD}@{ORACLE_HOST}:{ORACLE_PORT}/{ORACLE_SERVICE}")
mysql_conn = create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}")
'''

class TestDataExtraction:
    @pytest.mark.data_extraction
    @pytest.mark.smoke_test
    @pytest.mark.xfail
    def test_data_extraction_from_sales_data_to_stage(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            actual_query = """select * from stag_sales"""
            verify_expected_result_as_file_to_actual_result_as_database_table(test_case_name,"test_data/sales_data.csv","csv",actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while sales data extraction checks..")

    @pytest.mark.data_extraction
    @pytest.mark.smoke_test
    def test_data_extraction_from_product_data_to_stage(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            actual_query = """select * from stag_product"""
            verify_expected_result_as_file_to_actual_result_as_database_table(test_case_name,"test_data/product_data.csv","csv",actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while product data extraction checks..")

    @pytest.mark.data_extraction
    def test_data_extraction_from_supplier_data_to_stage(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            actual_query = """select * from stag_supplier"""
            verify_expected_result_as_file_to_actual_result_as_database_table(test_case_name,"test_data/supplier_data.json","json",actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while supplier data extraction checks..")

    @pytest.mark.data_extraction
    def test_data_extraction_from_inventory_data_to_stage(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            actual_query = """select * from stag_inventory"""
            verify_expected_result_as_file_to_actual_result_as_database_table(test_case_name,"test_data/inventory_data.xml","xml",actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while inventory data extraction checks..")

    @pytest.mark.data_extraction
    def test_data_extraction_from_stores_data_to_stage(self,connect_to_mysql_database,connect_to_oracle_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            expected_query = """select * from stores"""
            actual_query = """select * from stag_stores"""
            verify_expected_result_as_database_to_actual_result_as_database_table(test_case_name,expected_query,connect_to_oracle_database,actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while stores data extraction checks..")