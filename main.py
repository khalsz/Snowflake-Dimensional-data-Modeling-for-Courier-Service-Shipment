from database.db_connect import db_connection
from database.db_query import query_table
# from dataload.generate_data import generate_tables
from config.config import dbconfig
from database.create_table import create_db_table
from database.populate_db_table import populate_tables
import pandas as pd
from datetime import datetime

def main(data_path: str) -> None:
  """
  Main function to orchestrate data processing, database operations, and query execution.

  This function reads data from a CSV file, prepares it, establishes a database 
  connection, creates database tables, populates them with processed data, and then 
  executes a list of provided query statements.

  Args:
      data_path (str): Path to the CSV file containing source data.
  """

  try:
      # Read data from CSV file
      datadf = pd.read_csv(data_path)

      # Convert string date values to datetime objects (assuming suitable format)
      datadf['shipment_date'] = pd.to_datetime(datadf['shipment_date'])
      datadf['delivery_date'] = pd.to_datetime(datadf['delivery_date'])

      # Establish database connection (assuming `db_connection` function exists)
      connection, engine = db_connection(dbconfig['USERNAME'], dbconfig['PASSWORD'], dbconfig['HOST'], 'courier_delivery')

      # Create database tables (assuming `create_db_table` function exists)
      create_db_table(engine)

      # Populate database tables with processed data (assuming `populate_tables` function exists)
      populate_tables(engine, datadf)

      # Define query statements (assuming suitable query syntax for the database)
      query_statements = [
          # Calculating total cost revenue by customer name
          """
          SELECT c.customer_name, sum(f.total_cost) AS total_revenue
          FROM fact_table f
          INNER JOIN dim_customer c ON c.customer_id = f.customer_id
          GROUP BY c.customer_name
          """,

          # Analyzing cost performance by courier name
          """
          SELECT c.carrier_name, sum(f.total_cost) AS total_cost
          FROM fact_table f
          INNER JOIN dim_courier c ON f.courier_id = c.courier_id
          GROUP BY c.carrier_name
          """,

          # Analyzing quantity of product by shipment date
          """
          SELECT dd.shipment_year, dd.shipment_quarter, dd.shipment_month, AVG(f.quantity) AS avg_quantity
          FROM fact_table f
          INNER JOIN dim_date d ON d.date_id = f.date_id
          INNER JOIN dim_shipment_date dd ON dd.shipment_date_id = d.shipment_date_id
          GROUP BY dd.shipment_year, dd.shipment_quarter, dd.shipment_month
          """,
      ]

      # Execute each query statement and print the result
      for statement in query_statements:
          query_result = query_table(statement, connection)
          print(query_result)

  except Exception as e:
      raise Exception(f"Error: {e}")
  
if __name__ == "__main__": 
    main("raw_data.csv")
        