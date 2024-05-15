from database.db_connect import db_connection
from database.db_query import query_table
# from dataload.generate_data import generate_tables
from config.config import dbconfig
from database.create_table import create_db_table
from database.populate_db_table import populate_tables
import pandas as pd
from datetime import datetime

def main(data): 
    """
    Main function to generate data tables, create database tables,  and execute queries.

    Args:
        num_records (int): Number of records to generate for each table.

    """
    try:
        datadf = pd.read_csv(data)
        
        # converting string date values to datatime object
        datadf['shipment_date'] = pd.to_datetime(datadf['shipment_date'])
        datadf['delivery_date'] = pd.to_datetime(datadf['delivery_date'])
            
        # Establish database connection
        connection, engine = db_connection(dbconfig['USERNAME'], dbconfig['PASSWORD'], 
                    dbconfig['HOST'], 'courier_delivery')
        
        # Create database tables
        create_db_table(engine)
        
        populate_tables(engine,  datadf)
        
        # # Execute query statement list
        query_statements = [ # Calculating total cost revenue by customer name
                            "SELECT  c.customer_name, sum(f.total_cost)  \
                                from fact_table f \
                                INNER JOIN dim_customer c ON c.customer_id=f.customer_id \
                                GROUP BY c.customer_name" ,
                             
                            # Analyzing cost performance by courier name
                           "SELECT  c.carrier_name, sum(f.total_cost) as total_cost \
                            from fact_table f \
                                INNER JOIN dim_courier c ON f.courier_id=c.courier_id \
                                GROUP BY c.carrier_name", 
                            
        #                      # Analyzing quanity of product by shipment date
                                "SELECT dd.shipment_year, dd.shipment_quarter, dd.shipment_month, AVG(f.quantity) as Avg_quantity \
                                    FROM fact_table f \
                                    INNER JOIN dim_date d ON d.date_id=f.date_id \
                                    INNER JOIN dim_shipment_date dd ON dd.shipment_date_id=d.shipment_date_id \
                                    GROUP BY dd.shipment_year, dd.shipment_quarter, dd.shipment_month "]

        for statement in query_statements: 
            query_result = query_table(statement, connection)
            print(query_result)
            
    except Exception as e: 
        raise Exception(f"Error: {e}")

if __name__ == "__main__": 
    main("raw_data.csv")
        