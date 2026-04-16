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


class TestDataTransformation:
    @pytest.mark.data_transformation
    @pytest.mark.regression_test
    @pytest.mark.smoke_test
    def test_data_transformation_filter_sales(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            expected_query = """select * from stag_sales where sale_date>='2025-09-10'"""
            actual_query = """select * from filtered_sales"""
            verify_expected_result_as_database_to_actual_result_as_database_table(test_case_name,expected_query,connect_to_mysql_database,actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while sales data extraction checks..")

    @pytest.mark.data_transformation
    @pytest.mark.smoke_test
    def test_data_transformation_Router_High_sales(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            expected_query = """select * from filtered_sales where region='High'"""
            actual_query = """select * from high_sales"""
            verify_expected_result_as_database_to_actual_result_as_database_table(test_case_name,expected_query,connect_to_mysql_database,actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while Router sales data extraction checks..")

    @pytest.mark.data_transformation
    @pytest.mark.smoke_test
    def test_data_transformation_Router_Low_sales(self,connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            expected_query = """select * from filtered_sales where region='Low'"""
            actual_query = """select * from low_sales"""
            verify_expected_result_as_database_to_actual_result_as_database_table(test_case_name,expected_query,connect_to_mysql_database,actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while sales data Router transformation checks..")


  # Assignment to be completed ....
    def test_data_transformation_Aggregator_sales(self, connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            expected_query = """select product_id,year(sale_date) as year,month(sale_date) as month,sum(price*quantity) as total_sales  
                        from filtered_sales group by product_id,year(sale_date) ,month(sale_date)"""
            actual_query = """monthly_sales_summary_source"""
            verify_expected_result_as_database_to_actual_result_as_database_table(test_case_name,expected_query,connect_to_mysql_database,actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while Aggregator sales transformation checks..")

    def test_data_transformation_Joiner(self, connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            expected_query = """select fs.sales_id,fs.quantity,fs.price,fs.quantity*fs.price as sales_amount,fs.sale_date,
                        p.product_id,p.product_name,s.store_id,s.store_name from filtered_sales as fs inner join 
                        stag_product as p on fs.product_id = p.product_id inner join stag_stores as s on fs.store_id = s.store_id"""
            actual_query = """sales_with_details"""
            verify_expected_result_as_database_to_actual_result_as_database_table(test_case_name, expected_query,connect_to_mysql_database,actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while sales data Router transformation checks..")

    def test_data_transformation_Aggretor_inventory_(self, connect_to_mysql_database):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            expected_query = """select store_id,sum(quantity_on_hand) as total_inventory from stag_inventory group by store_id"""
            actual_query = """select * from aggregated_inventory_level"""
            verify_expected_result_as_database_to_actual_result_as_database_table(test_case_name, expected_query,connect_to_mysql_database,actual_query,connect_to_mysql_database)
        except Exception as e:
            logger.error(f"error while sales data Router transformation checks..")