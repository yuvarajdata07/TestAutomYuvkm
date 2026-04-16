import inspect
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging
import pytest
import pytest_check as check


# logging configuration
from common_utilities.utilities import get_actual_table_schema,  verify_expected_result_as_file_to_actual_result_as_database_table
from config.etl_configuration import *

logging.basicConfig(
    filename="logs/etl_process.log",
    filemode='a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

class TestSchemaValidation:

    def test_mysql_table_list_exists(self,connect_to_mysql_database):
        expected_table_list = ['fact_inventory','fact_sales','monthly_sales_summary','inventory_levels_by_store']
        try:
            missing_table_list = database_tables_exists(connect_to_mysql_database, expected_table_list, 'edw_retail_reporting')
            assert len(missing_table_list) == 0,f"missing table list is {missing_table_list} - Please check"
        except Exception as e:
            logger.error(f"error while table existence check checks..")


    # Create a utility function to perform below checks using SoftAssertion
    def test_data_types_of_columns_in_the_fact_inventory(self, connect_to_mysql_database):
        expected_schema = {
            "product_id": "int",
            "store_id": "int",
            "quantity_on_hand": "int",
            "last_updated": "date"
        }

        test_case_name = inspect.currentframe().f_code.co_name

        try:
            logger.info(f"Starting execution of: {test_case_name}")

            # 2. Get Actual Schema from Database
            actual_schema = get_actual_table_schema(
                connect_to_mysql_database,
                'fact_inventory',
                'edw_retail_reporting'
            )

            # 3. Perform Soft Assertions using a loop
            for column, expected_type in expected_schema.items():
                actual_type = actual_schema.get(column)

                # check.equal allows the test to continue even if one column fails
                check.equal(
                    actual_type,
                    expected_type,
                    f"Data type mismatch for '{column}': Expected {expected_type}, but found {actual_type}"
                )

        except Exception as e:
            logger.error(f"Error occurred during data type check for fact_inventory: {str(e)}")
            pytest.fail(f"Execution failed due to technical error: {e}")


    def test_data_types_of_columns_in_the_fact_sales(self, connect_to_mysql_database):
        expected_schema = {
            "sales_id": "int",
            "product_id": "int",
            "store_id": "int",
            "quantity": "int",
            "total_sales": "decimal(10, 2)",
            "sale_date": "date"
        }

        test_case_name = inspect.currentframe().f_code.co_name

        try:
            logger.info(f"Starting execution of: {test_case_name}")

            # 2. Get Actual Schema from Database
            actual_schema = get_actual_table_schema(
                connect_to_mysql_database,
                'fact_sales',
                'edw_retail_reporting'
            )

            # 3. Perform Soft Assertions using a loop
            for column, expected_type in expected_schema.items():
                actual_type = actual_schema.get(column)

                # check.equal allows the test to continue even if one column fails
                check.equal(
                    actual_type,
                    expected_type,
                    f"Data type mismatch for '{column}': Expected {expected_type}, but found {actual_type}"
                )

        except Exception as e:
            logger.error(f"Error occurred during data type check for fact_sales: {str(e)}")
            pytest.fail(f"Execution failed due to technical error: {e}")



    def test_data_types_of_columns_in_the_monthly_sales_summary(self, connect_to_mysql_database):
        expected_schema = {
            "product_id": "int",
            "month": "int",
            "year": "int",
            "total_sales": "decimal(10, 2)"
        }

        test_case_name = inspect.currentframe().f_code.co_name

        try:
            logger.info(f"Starting execution of: {test_case_name}")

            # 2. Get Actual Schema from Database
            actual_schema = get_actual_table_schema(
                connect_to_mysql_database,
                'monthly_sales_summary',
                'edw_retail_reporting'
            )

            # 3. Perform Soft Assertions using a loop
            for column, expected_type in expected_schema.items():
                actual_type = actual_schema.get(column)

                # check.equal allows the test to continue even if one column fails
                check.equal(
                    actual_type,
                    expected_type,
                    f"Data type mismatch for '{column}': Expected {expected_type}, but found {actual_type}"
                )

        except Exception as e:
            logger.error(f"Error occurred during data type check for monthly_sales_summary: {str(e)}")
            pytest.fail(f"Execution failed due to technical error: {e}")


    def test_data_types_of_columns__in_the_inventory_levels_by_store(self, connect_to_mysql_database):
        #1. Get expected Schema from mapping document
        expected_schema = {
            "store_id": "int",
            "total_inventory": "int"
        }

        test_case_name = inspect.currentframe().f_code.co_name

        try:
            logger.info(f"Starting execution of: {test_case_name}")

            # 2. Get Actual Schema from Database
            actual_schema = get_actual_table_schema(
                connect_to_mysql_database,
                'inventory_levels_by_store',
                'edw_retail_reporting'
            )

            # 3. Perform Soft Assertions using a loop
            for column, expected_type in expected_schema.items():
                actual_type = actual_schema.get(column)

                # check.equal allows the test to continue even if one column fails
                check.equal(
                    actual_type,
                    expected_type,
                    f"Data type mismatch for '{column}': Expected {expected_type}, but found {actual_type}"
                )

        except Exception as e:
            logger.error(f"Error occurred during data type check for inventory_levels_by_store: {str(e)}")
            pytest.fail(f"Execution failed due to technical error: {e}")

    '''
    # Create a utility function to perform below checks
    def test_column_name_in_the_fact_sales(self, connect_to_mysql_database,table_name):
        pass

    def test_column_name_in_the_fact_inventory(self, connect_to_mysql_database,table_name):
        pass

    def test_column_name_in_the_monthly_sales_summary(self, connect_to_mysql_database,table_name):
        pass

    def test_column_name_in_the_inventory_levels_by_store(self, connect_to_mysql_database,table_name):
        pass
    '''