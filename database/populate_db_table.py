from sqlalchemy import insert
from sqlalchemy.orm import  Session
from database.create_table import Time, Product, Category, Delivery, Shipment, Courier, Destination, Origin, Customer
from database.create_table import City, Payment, Fact
from dataload.tables_data import generate_product_data, generate_customer_data, generate_courier_data
from dataload.tables_data import generate_date_data, generate_fact_data

def populate_tables(engine, data_df): 
    """
    Populates database tables with data.

    Args:
        engine (Engine): SQLAlchemy engine object for the database.
        sales_dicts (list of dict): List of dictionaries containing data for the Sales table.
        product_dicts (list of dict): List of dictionaries containing data for the Product table.
        store_dicts (list of dict): List of dictionaries containing data for the Store table.
        date_dicts (list of dict): List of dictionaries containing data for the Date table.
        promotion_dicts (list of dict): List of dictionaries containing data for the Promotion table.

    Raises:
        Exception: If an error occurs during the data insertion process.
    """
    try: 
        courier_df, origin_df, destination_df = generate_courier_data(data_df)
        customer_df, payment_df, customer_city_df = generate_customer_data(data_df)
        product_df, category_df = generate_product_data(data_df)
        date_df, delivery_date_df, shipment_date_df = generate_date_data(data_df)
        fact_df = generate_fact_data(data_df, date_df, courier_df, customer_df, product_df)
        
        date_df.drop(["shipment_date", "delivery_date"], axis=1, inplace=True)
        
        
        with Session(engine) as session:  
            # Insert data into Category table
            session.execute(insert(Category), category_df.to_dict(orient='records'))
            # Insert data into Product table
            session.execute(insert(Product), product_df.to_dict(orient='records')) 
            # Insert data into Destination table
            session.execute(insert(Destination), destination_df.to_dict(orient='records'))
            # Insert data into Origin table
            session.execute(insert(Origin), origin_df.to_dict(orient='records'))
            # Insert data into Courier table
            session.execute(insert(Courier), courier_df.to_dict(orient='records')) 
            # Insert data into Delivery table
            session.execute(insert(Delivery), delivery_date_df.to_dict(orient='records')) 
            # Insert data into Shipment table
            session.execute(insert(Shipment), shipment_date_df.to_dict(orient='records'))
            # Insert data into Date table
            session.execute(insert(Time), date_df.to_dict(orient='records'))  
            # Insert data into City table            
            session.execute(insert(City), customer_city_df.to_dict(orient='records'))
            # Insert data into Payment table
            session.execute(insert(Payment), payment_df.to_dict(orient='records'))
            # Insert data into Customer table
            session.execute(insert(Customer), customer_df.to_dict(orient='records')) 
            # Insert data into Fact table
            session.execute(insert(Fact), fact_df.to_dict(orient='records'))
            session.commit()
    except Exception as e: 
        raise Exception("Error populating table with data: {e}")