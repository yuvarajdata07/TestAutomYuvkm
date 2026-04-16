import inspect
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import logging
import pytest_check as check
import pytest
import os
import sys
from sqlalchemy import text


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# logging configuration
from common_utilities.utilities import verify_expected_result_as_file_to_actual_result_as_database_table, \
verify_expected_result_as_database_to_actual_result_as_database_table, check_for_duplicates_across_the_file, \
check_for_duplicates_for_specific_column_in_file, check_for_file_existence, check_for_file_size, check_for_null_values_for_specific_column_in_file, \
check_for_null_values_in_file, check_referntial_integrity, get_duplicate_counts_by_column, get_duplicate_row_count, get_duplicate_pk_details


from config.etl_configuration import *

logging.basicConfig(
    filename="logs/etl_process.log",
    filemode='a',
    format='%(asctime)s-%(levelname)s-%(message)s',
    level =logging.INFO
)
logger = logging.getLogger(__name__)

class TestDataQuality:

       def test_data_quality_duplicate_check_sales_data_csv_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               duplicate_status = check_for_duplicates_across_the_file("test_data/sales_data.csv","csv")
               assert duplicate_status == True,"There are duplicates in sales_data file"
           except Exception as e:
               logger.error(f"error while duplicate check..")
               pytest.fail(f"error while duplicate check..")


       def test_data_quality_duplicate_check_product_data_csv_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               duplicate_status = check_for_duplicates_across_the_file("test_data/product_data.csv","csv")
               assert duplicate_status == True,"There are duplicates in product_data file"
           except Exception as e:
               logger.error(f"error while duplicate check..")
               pytest.fail(f"error while duplicate check..")

       def test_data_quality_duplicate_check_supplier_data_json_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               duplicate_status = check_for_duplicates_across_the_file("test_data/supplier_data.json","json")
               assert duplicate_status == True,"There are duplicates in supplier_data file"
           except Exception as e:
               logger.error(f"error while duplicate check..")
               pytest.fail(f"error while duplicate check..")

       def test_data_quality_duplicate_check_inventory_data_xml_file(self):
        try:
            test_case_name = inspect.currentframe().f_code.co_name
            duplicate_status = check_for_duplicates_across_the_file("test_data/inventory_data.xml","xml")
            assert duplicate_status == True,"There are duplicates in inventory_data file"
        except Exception as e:
            logger.error(f"error while duplicate check..")
            pytest.fail(f"error while duplicate check..")



       # use soft assertion to perform the checks on every columns
       def test_data_quality_duplicate_check_product_id_in_sales_data_csv_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               # test_script_library\test_data_quality.py
               duplicate_status_product_id = check_for_duplicates_for_specific_column_in_file(
                   "test_data/sales_data.csv", "csv", "product_id")

               # We want 0 duplicates, so status should be False
               assert duplicate_status_product_id == False, "Integrity Failure: Duplicates found in product_id"
               # Add assertion for all the columns

           except Exception as e:
               logger.error(f"error while duplicate check..")
               pytest.fail(f"error while duplicate check..")

       
       def test_data_quality_duplicate_check_product_id_in_product_data_csv_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               duplicate_status_product_id = check_for_duplicates_for_specific_column_in_file(
                   "test_data/product_data.csv", "csv", "product_id")
               assert duplicate_status_product_id == True, "There are duplicates in product_id column in product_data file"
               # Add assertion for all the columns

           except Exception as e:
               logger.error(f"error while duplicate check..")
               pytest.fail(f"error while duplicate check..")

       def test_data_quality_duplicate_check_supplier_id_in_supplier_data_json_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               duplicate_status_supplier_id = check_for_duplicates_for_specific_column_in_file(
                   "test_data/supplier_data.json", "json", "supplier_id")
               assert duplicate_status_supplier_id == True, "There are duplicates in supplier_id column in supplier_data file"
               # Add assertion for all the columns

           except Exception as e:
               logger.error(f"error while duplicate check..")
               pytest.fail(f"error while duplicate check..")
            

       def test_data_quality_duplicate_check_product_id_in_inventory_data_xml_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               duplicate_status_product_id = check_for_duplicates_for_specific_column_in_file(
                    "test_data/inventory_data.xml", "xml", "product_id")
               assert duplicate_status_product_id == True, "There are duplicates in product_id column in Inventory_data file"
           # Add assertion for all the columns

           except Exception as e:
               logger.error(f"error while duplicate check..")
           pytest.fail(f"error while duplicate check..")


       @pytest.mark.data_quality
       def test_data_quality_duplicate_check_product_id_in_fact_sales(self, connect_to_mysql_database):
           table = 'fact_sales'
           schema = 'edw_retail_reporting'
           column = 'product_id'

           try:
               logger.info(f"Checking for duplicates in {column} for table {table}")

               # 1. Fetch duplicates from DB
               duplicates = get_duplicate_counts_by_column(
                   connect_to_mysql_database,
                   table,
                   schema,
                   column
               )

               # 2. Validation Logic
               # If your business logic says product_id MUST be unique (unlikely for sales),
               # then len(duplicates) should be 0.
               # If you are just profiling, you might just log the results.

               duplicate_found = len(duplicates) > 0

               if duplicate_found:
                   # Log the first 5 examples of duplicates for debugging
                   examples = duplicates[:5]
                   logger.warning(f"Duplicates found in {column}. Examples: {examples}")

               # Example Assertion: Asserting that no product appears more than 100 times
               # (Adjust this threshold based on your specific business requirements)
               for prod_id, count in duplicates:
                   check.less_equal(
                       count,
                       500,
                       f"Product ID {prod_id} has excessive duplicates: {count} entries found."
                   )

           except Exception as e:
               logger.error(f"Error during duplicate check on {column}: {str(e)}")
               pytest.fail(f"Test failed due to technical error: {e}")

       @pytest.mark.data_quality
       def test_data_quality_duplicate_check_product_id_in_fact_inventory(self, connect_to_mysql_database):
           table = 'fact_inventory'
           schema = 'edw_retail_reporting'
           column = 'product_id'

           try:
               logger.info(f"Checking for duplicates in {column} for table {table}")

               # 1. Fetch duplicates from DB
               duplicates = get_duplicate_counts_by_column(
                   connect_to_mysql_database,
                   table,
                   schema,
                   column
               )

               # 2. Validation Logic
               # If your business logic says product_id MUST be unique (unlikely for sales),
               # then len(duplicates) should be 0.
               # If you are just profiling, you might just log the results.

               duplicate_found = len(duplicates) > 0

               if duplicate_found:
                   # Log the first 5 examples of duplicates for debugging
                   examples = duplicates[:5]
                   logger.warning(f"Duplicates found in {column}. Examples: {examples}")

               # Example Assertion: Asserting that no product appears more than 100 times
               # (Adjust this threshold based on your specific business requirements)
               for prod_id, count in duplicates:
                   check.less_equal(
                       count,
                       500,
                       f"Product ID {prod_id} has excessive duplicates: {count} entries found."
                   )

           except Exception as e:
               logger.error(f"Error during duplicate check on {column}: {str(e)}")
               pytest.fail(f"Test failed due to technical error: {e}")

       @pytest.mark.data_quality
       def test_data_quality_duplicate_check_product_id_in_monthly_sales_summary(self, connect_to_mysql_database):
           table = 'monthly_sales_summary'
           schema = 'edw_retail_reporting'
           column = 'product_id'

           try:
               logger.info(f"Checking for duplicates in {column} for table {table}")

               # 1. Fetch duplicates from DB
               duplicates = get_duplicate_counts_by_column(
                   connect_to_mysql_database,
                   table,
                   schema,
                   column
               )

               # 2. Validation Logic
               # If your business logic says product_id MUST be unique (unlikely for sales),
               # then len(duplicates) should be 0.
               # If you are just profiling, you might just log the results.

               duplicate_found = len(duplicates) > 0

               if duplicate_found:
                   # Log the first 5 examples of duplicates for debugging
                   examples = duplicates[:5]
                   logger.warning(f"Duplicates found in {column}. Examples: {examples}")

               # Example Assertion: Asserting that no product appears more than 100 times
               # (Adjust this threshold based on your specific business requirements)
               for prod_id, count in duplicates:
                   check.less_equal(
                       count,
                       500,
                       f"Product ID {prod_id} has excessive duplicates: {count} entries found."
                   )

           except Exception as e:
               logger.error(f"Error during duplicate check on {column}: {str(e)}")
               pytest.fail(f"Test failed due to technical error: {e}")

       @pytest.mark.data_quality
       def test_data_quality_duplicate_check_product_id_in_inventory_levels_by_store(self, connect_to_mysql_database):
           table = 'inventory_levels_by_store'
           schema = 'edw_retail_reporting'
           column = 'store_id'

           try:
               logger.info(f"Checking for duplicates in {column} for table {table}")

               # 1. Fetch duplicates from DB
               duplicates = get_duplicate_counts_by_column(
                   connect_to_mysql_database,
                   table,
                   schema,
                   column
               )

               # 2. Validation Logic
               # If your business logic says store_id MUST be unique (unlikely for sales),
               # then len(duplicates) should be 0.
               # If you are just profiling, you might just log the results.

               duplicate_found = len(duplicates) > 0

               if duplicate_found:
                   # Log the first 5 examples of duplicates for debugging
                   examples = duplicates[:5]
                   logger.warning(f"Duplicates found in {column}. Examples: {examples}")

               # Example Assertion: Asserting that no product appears more than 100 times
               # (Adjust this threshold based on your specific business requirements)
               for store_id, count in duplicates:
                   check.less_equal(
                       count,
                       500,
                       f"store ID {store_id} has excessive duplicates: {count} entries found."
                   )

           except Exception as e:
               logger.error(f"Error during duplicate check on {column}: {str(e)}")
               pytest.fail(f"Test failed due to technical error: {e}")

       @pytest.mark.data_quality
       def test_data_quality_duplicate_check_in_fact_sales(self, connect_to_mysql_database):
           table = 'fact_sales'
           schema = 'edw_retail_reporting'
           pk = 'sales_id'

           try:
               logger.info(f"Starting Duplicate Validation for {table}")

               # 1. Full Row Duplicate Check
               row_dupes = get_duplicate_row_count(connect_to_mysql_database, table, schema)
               check.equal(row_dupes, 0, f"Critical: Found {row_dupes} identical rows in {table}!")

               # 2. Primary Key Uniqueness Check
               pk_dupes = get_duplicate_pk_details(connect_to_mysql_database, table, schema, pk)

               # Validation: The list of duplicates should be empty
               check.equal(len(pk_dupes), 0, f"Integrity Failure: {pk} is not unique! Duplicated IDs: {pk_dupes}")

               if row_dupes == 0 and len(pk_dupes) == 0:
                   logger.info("Duplicate check passed: Table is clean.")

           except Exception as e:
               logger.error(f"Duplicate check failed technical execution: {str(e)}")
               pytest.fail(f"Execution error: {e}")

       @pytest.mark.data_quality
       def test_data_quality_duplicate_check_in_fact_inventory(self, connect_to_mysql_database):
           table = 'fact_inventory'
           schema = 'edw_retail_reporting'
           pk = 'product_id, store_id'

           try:
               logger.info(f"Starting Duplicate Validation for {table}")

               # 1. Full Row Duplicate Check
               row_dupes = get_duplicate_row_count(connect_to_mysql_database, table, schema)
               check.equal(row_dupes, 0, f"Critical: Found {row_dupes} identical rows in {table}!")

               # 2. Primary Key Uniqueness Check
               pk_dupes = get_duplicate_pk_details(connect_to_mysql_database, table, schema, pk)

               # Validation: The list of duplicates should be empty
               check.equal(len(pk_dupes), 0, f"Integrity Failure: {pk} is not unique! Duplicated IDs: {pk_dupes}")

               if row_dupes == 0 and len(pk_dupes) == 0:
                   logger.info("Duplicate check passed: Table is clean.")

           except Exception as e:
               logger.error(f"Duplicate check failed technical execution: {str(e)}")
               pytest.fail(f"Execution error: {e}")

       @pytest.mark.data_quality
       def test_data_quality_duplicate_check_in_monthly_sales_summary(self, connect_to_mysql_database):
           table = 'monthly_sales_summary'
           schema = 'edw_retail_reporting'
           pk = 'product_id'

           try:
               logger.info(f"Starting Duplicate Validation for {table}")

               # 1. Full Row Duplicate Check
               row_dupes = get_duplicate_row_count(connect_to_mysql_database, table, schema)
               check.equal(row_dupes, 0, f"Critical: Found {row_dupes} identical rows in {table}!")

               # 2. Primary Key Uniqueness Check
               pk_dupes = get_duplicate_pk_details(connect_to_mysql_database, table, schema, pk)

               # Validation: The list of duplicates should be empty
               check.equal(len(pk_dupes), 0, f"Integrity Failure: {pk} is not unique! Duplicated IDs: {pk_dupes}")

               if row_dupes == 0 and len(pk_dupes) == 0:
                   logger.info("Duplicate check passed: Table is clean.")

           except Exception as e:
               logger.error(f"Duplicate check failed technical execution: {str(e)}")
               pytest.fail(f"Execution error: {e}")

       @pytest.mark.data_quality
       def test_data_quality_duplicate_check_in_inventory_levels_by_store(self, connect_to_mysql_database):
           table = 'inventory_levels_by_store'
           schema = 'edw_retail_reporting'
           pk = 'store_id'

           try:
               logger.info(f"Starting Duplicate Validation for {table}")

               # 1. Full Row Duplicate Check
               row_dupes = get_duplicate_row_count(connect_to_mysql_database, table, schema)
               check.equal(row_dupes, 0, f"Critical: Found {row_dupes} identical rows in {table}!")

               # 2. Primary Key Uniqueness Check
               pk_dupes = get_duplicate_pk_details(connect_to_mysql_database, table, schema, pk)

               # Validation: The list of duplicates should be empty
               check.equal(len(pk_dupes), 0, f"Integrity Failure: {pk} is not unique! Duplicated IDs: {pk_dupes}")

               if row_dupes == 0 and len(pk_dupes) == 0:
                   logger.info("Duplicate check passed: Table is clean.")

           except Exception as e:
               logger.error(f"Duplicate check failed technical execution: {str(e)}")
               pytest.fail(f"Execution error: {e}")

       '''
       # Duplicate checks o table and columns as well
       def test_data_quality_duplicate_check_in_fact_sales(self,connect_to_mysql_database):
            pass

       
       # Add remaining test case for rest of the tables and on different columns

       '''

       # File existence check
       @pytest.mark.data_quality
       def test_data_quality_file_existence_of_sales_data_csv_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               file_existence_status = check_for_file_existence("test_data/sales_data.csv")
               assert file_existence_status == True,f"sales_data csv file doen not exist in the location"
           except Exception as e:
               logger.error(f"error while reading the file..")
               pytest.fail(f"eerror while reading the file..")

       @pytest.mark.data_quality
       def test_data_quality_file_existence_of_product_data_csv_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               file_existence_status = check_for_file_existence("test_data/product_data.csv")
               assert file_existence_status == True, f"product_data csv file doen not exist in the location"
           except Exception as e:
               logger.error(f"error while reading the file..")
               pytest.fail(f"eerror while reading the file..")

       @pytest.mark.data_quality
       def test_data_quality_file_existence_of_supplier_data_json_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               file_existence_status = check_for_file_existence("test_data/supplier_data.json")
               assert file_existence_status == True, f"supplier_data json file does not exist in the location"
           except Exception as e:
               logger.error(f"error while reading the file..")
               pytest.fail(f"eerror while reading the file..")

       @pytest.mark.data_quality
       def test_data_quality_file_existence_of_inventory_data_xml_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               file_existence_status = check_for_file_existence("test_data/inventory_data.xml")
               assert file_existence_status == True, f"inventory_data xml file does not exist in the location"
           except Exception as e:
               logger.error(f"error while reading the file..")
               pytest.fail(f"eerror while reading the file..")


       # File size check check
       def test_data_quality_file_size_of_sales_data_csv_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               file_existence_status = check_for_file_size("test_data/sales_data.csv")
               assert file_existence_status == True,f"sales_data csv file does not have any data in the location"
           except Exception as e:
               logger.error(f"error while reading the file..")
               pytest.fail(f"error while reading the file..")

       def test_data_quality_file_size_of_product_data_csv_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               file_existence_status = check_for_file_size("test_data/product_data.csv")
               assert file_existence_status == True, f"product_data csv file does not have any data in the location"
           except Exception as e:
               logger.error(f"error while reading the file..")
               pytest.fail(f"error while reading the file..")

       def test_data_quality_file_size_of_supplier_data_json_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               file_existence_status = check_for_file_size("test_data/supplier_data.json")
               assert file_existence_status == True, f"supplier_data json file does not have any data in the location"
           except Exception as e:
               logger.error(f"error while reading the file..")
               pytest.fail(f"error while reading the file..")

       def test_data_quality_file_size_of_inventory_data_xml_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               file_existence_status = check_for_file_size("test_data/inventory_data.xml")
               assert file_existence_status == True, f"inventory_data xml file does not have any data in the location"
           except Exception as e:
               logger.error(f"error while reading the file..")
               pytest.fail(f"error while reading the file..")

       # NULL value checks on a column in the file
       def test_data_quality_null_chec_value_product_id_in_sales_data_csv_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               null_status_product_id = check_for_null_values_for_specific_column_in_file("test_data/sales_data.csv","csv","product_id")
               assert null_status_product_id == True,"There are null values  in product_id column in sales_data file"
           except Exception as e:
               logger.error(f"error while checking null values..")
               pytest.fail(f"error while checking null values..")

       def test_data_quality_null_chec_value_product_id_in_product_data_csv_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               null_status_product_id = check_for_null_values_for_specific_column_in_file("test_data/product_data.csv","csv","product_id")
               assert null_status_product_id == True,"There are null values  in product_id column in sales_data file"
           except Exception as e:
               logger.error(f"error while checking null values..")
               pytest.fail(f"error while checking null values..")

       def test_data_quality_null_chec_value_product_id_in_inventory_data_xml_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               null_status_product_id = check_for_null_values_for_specific_column_in_file("test_data/inventory_data.xml","xml","product_id")
               assert null_status_product_id == True,"There are null values  in product_id column in sales_data file"
           except Exception as e:
               logger.error(f"error while checking null values..")
               pytest.fail(f"error while checking null values..")

       def test_data_quality_null_chec_value_product_id_in_supplier_data_json_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               null_status_product_id = check_for_null_values_for_specific_column_in_file("test_data/supplier_data.json","json","supplier_id")
               assert null_status_product_id == True,"There are null values  in supplier_id column in sales_data file"
           except Exception as e:
               logger.error(f"error while checking null values..")
               pytest.fail(f"error while checking null values..")
       # Implement the checks for all other files and columns

       # NULL value checks in the file
       def test_data_quality_null_check_value_in_sales_data_csv_file(self):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               null_values__status = check_for_null_values_in_file("test_data/sales_data.csv",
                                                                                               "csv")
               assert null_values__status == True, "There are null values in product_id column in sales_data file"
           except Exception as e:
               logger.error(f"error while checking null values..")
               pytest.fail(f"error while checking null values..")

       # Implement the checks for all other files and columns

       # Implement the checks for tables as well


       # NULL value checks in the target file
       def test_data_quality_null_check_value_product_id_in_fact_sales(self, connect_to_mysql_database):
           table_name = "fact_sales"
           column_name = "product_id"

           try:
               logger.info(f"Starting NULL value validation for {table_name}.{column_name}")

               # Use SQLAlchemy text() for the query
               query = text(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL")

               # Execute directly using the connection object
               result = connect_to_mysql_database.execute(query)
               null_count = result.scalar()  # scalar() gets the first value of the first row

               assert null_count == 0, f"Integrity Failure: Found {null_count} NULL values in {table_name}.{column_name}"
               logger.info(f"NULL check passed for {table_name}.{column_name}")

           except AssertionError as ae:
               logger.error(f"Data Quality Error: {str(ae)}")
               raise
           except Exception as e:
               logger.error(f"Database Error during NULL check: {str(e)}")
               pytest.fail(f"Test failed due to database execution error: {e}")

       def test_data_quality_null_check_value_product_id_in_fact_inventory(self, connect_to_mysql_database):
           table_name = "fact_inventory"
           column_name = "product_id"

           try:
               logger.info(f"Starting NULL value validation for {table_name}.{column_name}")

               # Use SQLAlchemy text() for the query
               query = text(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL")

               # Execute directly using the connection object
               result = connect_to_mysql_database.execute(query)
               null_count = result.scalar()  # scalar() gets the first value of the first row

               assert null_count == 0, f"Integrity Failure: Found {null_count} NULL values in {table_name}.{column_name}"
               logger.info(f"NULL check passed for {table_name}.{column_name}")

           except AssertionError as ae:
               logger.error(f"Data Quality Error: {str(ae)}")
               raise
           except Exception as e:
               logger.error(f"Database Error during NULL check: {str(e)}")
               pytest.fail(f"Test failed due to database execution error: {e}")

       def test_data_quality_null_check_value_product_id_in_inventory_levels_by_store(self, connect_to_mysql_database):
           table_name = "inventory_levels_by_store"
           column_name = "store_id"

           try:
               logger.info(f"Starting NULL value validation for {table_name}.{column_name}")

               # Use SQLAlchemy text() for the query
               query = text(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL")

               # Execute directly using the connection object
               result = connect_to_mysql_database.execute(query)
               null_count = result.scalar()  # scalar() gets the first value of the first row

               assert null_count == 0, f"Integrity Failure: Found {null_count} NULL values in {table_name}.{column_name}"
               logger.info(f"NULL check passed for {table_name}.{column_name}")

           except AssertionError as ae:
               logger.error(f"Data Quality Error: {str(ae)}")
               raise
           except Exception as e:
               logger.error(f"Database Error during NULL check: {str(e)}")
               pytest.fail(f"Test failed due to database execution error: {e}")

       def test_data_quality_null_check_value_product_id_in_monthly_sales_summary(self, connect_to_mysql_database):
           table_name = "monthly_sales_summary"
           column_name = "product_id"

           try:
               logger.info(f"Starting NULL value validation for {table_name}.{column_name}")

               # Use SQLAlchemy text() for the query
               query = text(f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} IS NULL")

               # Execute directly using the connection object
               result = connect_to_mysql_database.execute(query)
               null_count = result.scalar()  # scalar() gets the first value of the first row

               assert null_count == 0, f"Integrity Failure: Found {null_count} NULL values in {table_name}.{column_name}"
               logger.info(f"NULL check passed for {table_name}.{column_name}")

           except AssertionError as ae:
               logger.error(f"Data Quality Error: {str(ae)}")
               raise
           except Exception as e:
               logger.error(f"Database Error during NULL check: {str(e)}")
               pytest.fail(f"Test failed due to database execution error: {e}")


        # referential integerity check
       def test_data_quality_refererntial_check_fact_sales_and_stag_product(self, connect_to_mysql_database):
           try:
               test_case_name = inspect.currentframe().f_code.co_name
               foreign_query = """select * from fact_sales"""
               primary_query = """select * from stag_product"""
               df_not_matched = check_referntial_integrity(
                   source_db_conn=connect_to_mysql_database,
                   target_db_conn=connect_to_mysql_database,
                   foreign_query=foreign_query,
                   primary_query=primary_query,
                   key_column="product_id",
                   csv_path="differences/foregin_key_not_mathcing.csv")

               assert df_not_matched.empty == True, "There are foreign keys values in product_id column in foregin key table"
           except Exception as e:
               logger.error(f"error while regerential integery check.")
               pytest.fail(f"error while regerential integery check.")
