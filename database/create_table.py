from sqlalchemy import  Column, Integer, ForeignKey, String, DATE
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref


Base = declarative_base()


class Product(Base): 
    __tablename__ = "dim_product"
    product_id = Column( Integer, primary_key=True)
    name = Column(String(40))
    categor_id = Column(Integer)
    shipping_priority = Column(String(40))
    price = Column(Integer)
    shipping_priority = Column(String(40))
    

class Time(Base): 
    __tablename__ = "dim_date"
    date_id = Column(Integer, primary_key=True)
    shipment_date_id = Column(Integer)
    delivery_date_id = Column(Integer)
    
class Courier(Base): 
    __tablename__ = "dim_courier"
    courier_id = Column(Integer, primary_key=True)
    name = Column(String(40))
    rating = Column(Integer)
    destination_id = Column(Integer)
    origin_id = Column(Integer)
    mode_of_transport = Column(String(40))

class Customer(Base): 
    __tablename__ = 'dim_customer'
    customer_id = Column(Integer, primary_key=True)
    name = Column(String(40))  
    city_id = Column(Integer)  
    segment = Column(String(40))
    
class Fact(Base): 
    __tablename__ = "fact_table"
    shipment_id = Column(Integer, primary_key=True)
    quantity = Column(Integer)
    cost = Column(Integer)
    courier_id = Column(Integer, ForeignKey("dim_courier.courier_id"), nullable=False)
    product = relationship("Courier")
    date_id = Column(Integer, ForeignKey("date_dimension.date_id"), nullable=False)
    time = relationship('Time', backref=backref('Fact'))
    product_id = Column( Integer, ForeignKey("dim_product.product_id"), nullable=False)
    customer = relationship('Product')
    customer_id = Column(Integer, ForeignKey("dim_customer.customer_id"), nullable=False)
    order = relationship('Customer')

def create_db_table(engine): 
    """
    Creates database tables based on SQLAlchemy metadata.

    This function drops existing tables (if any) and creates new tables based on the SQLAlchemy metadata defined in Base.

    Args:
        engine (Engine): SQLAlchemy engine object for the database.

    Raises:
        Exception: If an error occurs during the table creation process.
    """
    try: 
        # Drop existing tables
        Base.metadata.drop_all(engine, 
                               tables=[Base.metadata.tables["promotion_dimension"], 
                                       Base.metadata.tables["store_dimension"], 
                                       Base.metadata.tables["product_dimension"], 
                                       Base.metadata.tables["date_dimension"], 
                                       Base.metadata.tables["sales_table"]])
        # Create new tables
        Base.metadata.create_all(engine)
        print("all table successfully created")
        
    except Exception as e: 
        raise Exception(f"Error creating tables: {e}")