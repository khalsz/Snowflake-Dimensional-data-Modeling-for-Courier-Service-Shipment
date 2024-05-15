import pandas as pd
from dataload.unique_subset import create_unique_id_with_subset
from dataload.date_convert import extract_date_year

# #loading csv data as dataframe
# df_data = pd.read_csv(join(os.getcwd(), "raw_data.csv"))

def generate_product_data(df_data):   
    # initializing column names for tables
    product_columns = ["product_name", "unit_price", "shipping_priority", "category"]
    category_columns = ["category"] 
    
    # extracting data for category_dim and product_dim table
    category_df = create_unique_id_with_subset(df_data, category_columns, "category", "category_id", 10)
    product_df = create_unique_id_with_subset(df_data, product_columns, "product_name", "product_id", 30)

    # extracting category_id (FK) column as FK for product_dim table 
    product_df = pd.merge(product_df, category_df[["category", "category_id"]], on="category").drop("category", axis=1)
    
    return product_df, category_df

def generate_customer_data(df_data): 
    # initializing column names for tables
    customer_columns = ["customer_name", "customer_segment", "payment_method", "customer_city"]
    customer_city_cols = ["customer_city", "customer_state", "customer_country"]
    payment_columns = ["payment_method"]
    
    # extracting data for payment_dim and customer_city_dim table
    payment_df = create_unique_id_with_subset(df_data, payment_columns, "payment_method", "payment_id", 20)
    customer_city_df = create_unique_id_with_subset(df_data, customer_city_cols, "customer_city", "city_id", 50)

    # extracting data for customer_dim table
    customer_df = create_unique_id_with_subset(df_data, customer_columns, "customer_name", "customer_id", 80)

    # extracting payment_id & city_id columns as FK for customer_dim table 
    customer_df = pd.merge(customer_df, payment_df, on="payment_method").drop("payment_method", axis=1)
    customer_df = pd.merge(customer_df, customer_city_df, on="customer_city").drop("customer_city", axis=1)
    
    return customer_df, payment_df, customer_city_df

def generate_courier_data(df_data): 
    # initializing column names for tables
    courier_columns = ["carrier_name", "carrier_rating", "mode_of_transport", "destination_city", "origin_city"]
    courier_dest_columns = ["destination_city", "destination_state", "destination_country"]
    courier_origin_columns = ["origin_city", "origin_state", "origin_country"]

    # extracting data for destination_dim and origin_dimtable
    destination_df = create_unique_id_with_subset(df_data, courier_dest_columns, "destination_city", "destination_id", 15)
    origin_df = create_unique_id_with_subset(df_data, courier_origin_columns, "origin_city", "origin_id", 40)

    # extracting data for courier_dim table
    courier_df = create_unique_id_with_subset(df_data, courier_columns, "carrier_name", "courier_id", 70)

    # extracting origin_id & destination_id columns as FK for courier_dim table 
    courier_df = pd.merge(courier_df, origin_df, on="origin_city").drop("origin_city", axis=1)
    courier_df = pd.merge(courier_df, destination_df, on="destination_city").drop("destination_city", axis=1)
    
    return courier_df, origin_df, destination_df

def generate_date_data(df_data):
    try: 
        
        # initializing column names for tables
        date_cols = ["shipment_date", "delivery_date"]
        shipment_date_cols = ["shipment_date"]
        delivery_date_cols = ["delivery_date"]

        # extracting data for delivery_date_dim and shipment_date_dim table
        delivery_date_df = create_unique_id_with_subset(df_data, delivery_date_cols, "delivery_date", "delivery_date_id", 25)
        shipment_date_df = create_unique_id_with_subset(df_data, shipment_date_cols, "shipment_date", "shipment_date_id", 60)
        
        # creating month, year, quarter & weekday columns
        delivery_date_df = extract_date_year(delivery_date_df, "delivery_date", "delivery")
        shipment_date_df = extract_date_year(shipment_date_df, "shipment_date", "shipment")
        
        # extracting data for date_dim table
        date_df = create_unique_id_with_subset(df_data, date_cols, ["shipment_date", "delivery_date"], "date_id", 45)

        # extracting delivery_date_id & shipment_date_id columns as FK for date_dim table 
        date_df = pd.merge(date_df, shipment_date_df, on="shipment_date")
        date_df = pd.merge(date_df, delivery_date_df, on="delivery_date")
        
        return date_df, delivery_date_df, shipment_date_df
    except Exception as e: 
        raise(f"Error {e}")

def generate_fact_data(df_data, date_df, courier_df, customer_df, product_df): 
    
    # initializing column names for fact_table
    fact_columns = ["shipment_id", "total_cost", "quantity", "product_name", "customer_name", 
                    "carrier_name", "shipment_date", "delivery_date"]
    # extracting data for fact_shipment table
    fact_df = df_data[fact_columns]

    # extracting date_id, courier_id, customer_id, and product_id as FK for Fact shipment table. 
    fact_df = pd.merge(fact_df, date_df, on=["shipment_date", "delivery_date"]).drop(["shipment_date", "delivery_date"], axis=1)
    fact_df = pd.merge(fact_df, courier_df, on="carrier_name").drop("carrier_name", axis=1)
    fact_df = pd.merge(fact_df, customer_df, on="customer_name").drop("customer_name", axis=1)
    fact_df = pd.merge(fact_df, product_df, on="product_name").drop("product_name", axis=1)
    return fact_df

# date_df.drop(["shipment_date", "delivery_date"], axis=1, inplace=True)