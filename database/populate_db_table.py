from sqlalchemy import insert
import pandas as pd
from sqlalchemy.orm import  Session
from database.create_table import Time, Product, Category, Delivery, Shipment, Courier, Destination, Origin, Customer
from database.create_table import City, Payment, Fact, Shipping, PriorTransport
from dataload.tables_data import product_data, customer_data, courier_data
from dataload.tables_data import date_data, fact_data, shipping_data

def populate_tables(engine, data_df: pd.DataFrame) -> None:
  """
  Populates database tables with data extracted and transformed from a DataFrame.

  This function takes a SQLAlchemy engine object and a DataFrame containing the 
  source data. It assumes the DataFrame structure is suitable for generating data 
  for multiple tables (details depend on the specific implementation). The function 
  extracts and transforms data from the DataFrame to populate various tables 
  (e.g., Category, Product, Customer, etc.) in the database.

  Args:
      engine (Any): SQLAlchemy engine object for the database.
      data_df (pd.DataFrame): DataFrame containing the source data.

  Raises:
      Exception: If an error occurs during the data insertion process.
  """

  try:
      # Extract and transform data for different tables from the source DataFrame
      courier_df, origin_df, destination_df = courier_data(data_df)
      customer_df, payment_df, customer_city_df = customer_data(data_df)
      product_df, category_df = product_data(data_df)
      shipping_df, proir_trans_data_df = shipping_data(data_df)
      date_df, delivery_date_df, shipment_date_df = date_data(data_df)
      fact_df = fact_data(data_df, courier_df, customer_df, product_df, 
                          shipping_df, delivery_date_df, shipment_date_df)



      with Session(engine) as session:
          # Insert data into tables using SQLAlchemy insert statements and DataFrame conversion
          session.execute(insert(Category), category_df.to_dict(orient='records'))
          session.execute(insert(Product), product_df.to_dict(orient='records'))
          session.execute(insert(Destination), destination_df.to_dict(orient='records'))
          session.execute(insert(Origin), origin_df.to_dict(orient='records'))
          session.execute(insert(Time), date_df.to_dict(orient='records'))  # Assuming Time table for dates
          session.execute(insert(Courier), courier_df.to_dict(orient='records'))
          session.execute(insert(Delivery), delivery_date_df.to_dict(orient='records'))
          session.execute(insert(Shipment), shipment_date_df.to_dict(orient='records'))
          session.execute(insert(City), customer_city_df.to_dict(orient='records'))
          session.execute(insert(Payment), payment_df.to_dict(orient='records'))
          session.execute(insert(Customer), customer_df.to_dict(orient='records'))
          session.execute(insert(PriorTransport), proir_trans_data_df.to_dict(orient='records'))
          session.execute(insert(Shipping), shipping_df.to_dict(orient='records'))
          session.execute(insert(Fact), fact_df.to_dict(orient='records'))
          session.commit()

  except Exception as e:
      raise Exception(f"Error populating tables with data: {e}")