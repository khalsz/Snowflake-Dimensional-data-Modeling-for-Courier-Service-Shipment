import pandas as pd
from dataload.unique_subset import create_unique_id_with_subset
from dataload.date_convert import extract_date_year

def generate_product_data(df_data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
  """
  Generates dataframes for product and category dimension tables.

  This function takes a DataFrame containing source data and extracts data 
  relevant for populating the product and category dimension tables. It uses the 
  `create_unique_id_with_subset` function to assign unique IDs and handles 
  foreign key relationships.

  Args:
      df_data (pd.DataFrame): DataFrame containing the source data.

  Returns:
      tuple[pd.DataFrame, pd.DataFrame]: A tuple containing two DataFrames - 
          the first for product data and the second for category data.

  Raises:
      Exception: If an error occurs during data processing.
  """

  try:
      # Define column names for product and category tables
      product_columns = ["product_name", "unit_price", "shipping_priority", "category"]
      category_columns = ["category"]

      # Extract data for category and product dimension tables with unique IDs
      category_df = create_unique_id_with_subset(df_data, category_columns, "category", "category_id", 10)
      product_df = create_unique_id_with_subset(df_data, product_columns, "product_name", "product_id", 30)

      # Merge category data (including category ID as foreign key) into product data
      product_df = pd.merge(product_df, category_df[["category", "category_id"]], on="category").drop("category", axis=1)

      return product_df, category_df

  except Exception as e:
      raise Exception(f"Error generating product and category data: {e}")
  

def generate_customer_data(df_data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
  """
  Generates dataframes for customer, payment, and customer city dimension tables.

  This function takes a DataFrame containing source data and extracts data 
  relevant for populating the customer, payment, and customer city dimension 
  tables. It uses the `create_unique_id_with_subset` function to assign unique 
  IDs and handles foreign key relationships.

  Args:
      df_data (pd.DataFrame): DataFrame containing the source data.

  Returns:
      tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple containing three 
          DataFrames - the first for customer data, the second for payment data, 
          and the third for customer city data.

  Raises:
      Exception: If an error occurs during data processing.
  """

  try:
      # Define column names for customer, payment, and customer city tables
      customer_columns = ["customer_name", "customer_segment", "payment_method", "customer_city"]
      customer_city_cols = ["customer_city", "customer_state", "customer_country"]
      payment_columns = ["payment_method"]

      # Extract data for payment, customer city, and customer dimension tables with unique IDs
      payment_df = create_unique_id_with_subset(df_data, payment_columns, "payment_method", "payment_id", 20)
      customer_city_df = create_unique_id_with_subset(df_data, customer_city_cols, "customer_city", "city_id", 50)
      customer_df = create_unique_id_with_subset(df_data, customer_columns, "customer_name", "customer_id", 80)

      # Merge payment and customer city data (including foreign key IDs) into customer data
      customer_df = pd.merge(customer_df, payment_df, on="payment_method").drop("payment_method", axis=1)
      customer_df = pd.merge(customer_df, customer_city_df, on="customer_city").drop("customer_city", axis=1)

      return customer_df, payment_df, customer_city_df

  except Exception as e:
      raise Exception(f"Error generating customer, payment, and city data: {e}")


def generate_courier_data(df_data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
  """
  Generates dataframes for courier, origin city, and destination city dimension tables.

  This function takes a DataFrame containing source data and extracts data 
  relevant for populating the courier, origin city, and destination city 
  dimension tables. It uses the `create_unique_id_with_subset` function to assign 
  unique IDs and handles foreign key relationships.

  Args:
      df_data (pd.DataFrame): DataFrame containing the source data.

  Returns:
      tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple containing three 
          DataFrames - the first for courier data, the second for origin city data, 
          and the third for destination city data.

  Raises:
      Exception: If an error occurs during data processing.
  """

  try:
      # Define column names for courier, destination city, and origin city tables
      courier_columns = ["carrier_name", "carrier_rating", "mode_of_transport", "destination_city", "origin_city"]
      courier_dest_columns = ["destination_city", "destination_state", "destination_country"]
      courier_origin_columns = ["origin_city", "origin_state", "origin_country"]

      # Extract data for destination and origin dimension tables with unique IDs
      destination_df = create_unique_id_with_subset(df_data, courier_dest_columns, "destination_city", "destination_id", 15)
      origin_df = create_unique_id_with_subset(df_data, courier_origin_columns, "origin_city", "origin_id", 40)

      # Extract data for courier dimension table with unique IDs
      courier_df = create_unique_id_with_subset(df_data, courier_columns, "carrier_name", "courier_id", 70)

      # Merge origin and destination data (including foreign key IDs) into courier data
      courier_df = pd.merge(courier_df, origin_df, on="origin_city").drop("origin_city", axis=1)
      courier_df = pd.merge(courier_df, destination_df, on="destination_city").drop("destination_city", axis=1)

      return courier_df, origin_df, destination_df

  except Exception as e:
      raise Exception(f"Error generating courier, origin, and destination data: {e}")
    
    
def generate_date_data(df_data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
  """
  Generates dataframes for date, shipment date, and delivery date dimension tables.

  This function takes a DataFrame containing source data and extracts data 
  relevant for populating the date, shipment date, and delivery date dimension 
  tables. It uses the `create_unique_id_with_subset` function to assign unique 
  IDs, handles foreign key relationships, and extracts additional date-related 
  columns (month, year, quarter, weekday) assuming the date columns are in a 
  suitable format (e.g., datetime format).

  Args:
      df_data (pd.DataFrame): DataFrame containing the source data.

  Returns:
      tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: A tuple containing three 
          DataFrames - the first for date data, the second for shipment date data, 
          and the third for delivery date data.

  Raises:
      Exception: If an error occurs during data processing.
  """

  try:
      # Define column names for date, shipment date, and delivery date tables
      date_cols = ["shipment_date", "delivery_date"]
      shipment_date_cols = ["shipment_date"]
      delivery_date_cols = ["delivery_date"]

      # Extract data for shipment and delivery date dimension tables with unique IDs
      delivery_date_df = create_unique_id_with_subset(df_data, delivery_date_cols, "delivery_date", "delivery_date_id", 25)
      shipment_date_df = create_unique_id_with_subset(df_data, shipment_date_cols, "shipment_date", "shipment_date_id", 60)

      # Extract additional date-related columns (month, year, quarter, weekday)
      delivery_date_df = extract_date_year(delivery_date_df, "delivery_date", "delivery")
      shipment_date_df = extract_date_year(shipment_date_df, "shipment_date", "shipment")

      # Extract data for date_dim table with unique IDs
      date_df = create_unique_id_with_subset(df_data, date_cols, ["shipment_date", "delivery_date"], "date_id", 45)

      # Merge shipment and delivery date data (including foreign key IDs) into date data
      date_df = pd.merge(date_df, shipment_date_df, on="shipment_date")
      date_df = pd.merge(date_df, delivery_date_df, on="delivery_date")

      return date_df, delivery_date_df, shipment_date_df

  except Exception as e:
      raise Exception(f"Error generating date, shipment, and delivery data: {e}")

def generate_fact_data(
    df_data: pd.DataFrame, date_df: pd.DataFrame, courier_df: pd.DataFrame, customer_df: pd.DataFrame, product_df: pd.DataFrame
) -> pd.DataFrame:
  """
  Generates a DataFrame for the fact table containing shipment details.

  This function takes DataFrames for source data, date dimension, courier 
  dimension, customer dimension, and product dimension and extracts relevant 
  data to populate the fact table. It assumes the source data contains columns 
  matching the `fact_columns` list and performs merges to link data using 
  foreign key relationships.

  Args:
      df_data (pd.DataFrame): DataFrame containing the source data.
      date_df (pd.DataFrame): DataFrame for the date dimension table.
      courier_df (pd.DataFrame): DataFrame for the courier dimension table.
      customer_df (pd.DataFrame): DataFrame for the customer dimension table.
      product_df (pd.DataFrame): DataFrame for the product dimension table.

  Returns:
      pd.DataFrame: DataFrame containing shipment fact data.

  Raises:
      Exception: If an error occurs during data processing.
  """

  try:
      # Define column names for the fact table
      fact_columns = [
          "shipment_id",
          "total_cost",
          "quantity",
          "product_name",
          "customer_name",
          "carrier_name",
          "shipment_date",
          "delivery_date",
      ]

      # Extract relevant data from source DataFrame for the fact table
      fact_df = df_data[fact_columns]

      # Merge with dimension tables using foreign key relationships
      fact_df = fact_df.merge(date_df, on=["shipment_date", "delivery_date"]).drop(
          ["shipment_date", "delivery_date"], axis=1
      )
      fact_df = fact_df.merge(courier_df, on="carrier_name").drop("carrier_name", axis=1)
      fact_df = fact_df.merge(customer_df, on="customer_name").drop("customer_name", axis=1)
      fact_df = fact_df.merge(product_df, on="product_name").drop("product_name", axis=1)

      return fact_df

  except Exception as e:
      raise Exception(f"Error generating fact data: {e}")
