from sqlalchemy import  Column, Integer, ForeignKey, String, DATE, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

# initializing base class to map db_table bject 
Base = declarative_base()

class Category(Base): 
    __tablename__ = "dim_category"
    category_id = Column(Integer, primary_key=True)
    category = Column(String(40))
    
class Product(Base): 
    __tablename__ = "dim_product"
    product_id = Column( Integer, primary_key=True)
    product_name = Column(String(40))
    unit_price = Column(Integer)
    category_id = Column(Integer, ForeignKey("dim_category.category_id", ondelete='CASCADE', 
                                             onupdate='CASCADE'), nullable=False)
    category = relationship("Category")


    
class Delivery(Base): 
    __tablename__ = "dim_delivery_date"
    delivery_date_id = Column(Integer, primary_key=True)
    delivery_date = Column(DATE)
    time = relationship("Time")
    date_id = Column(Integer, ForeignKey("dim_date.date_id", ondelete='CASCADE', 
                                         onupdate='CASCADE'), nullable=False)


class Shipment(Base): 
    __tablename__ = "dim_shipment_date"
    shipment_date_id = Column(Integer, primary_key=True)
    time = relationship("Time")
    date_id = Column(Integer, ForeignKey("dim_date.date_id", ondelete='CASCADE', 
                                         onupdate='CASCADE'), nullable=False)
    
class Time(Base): 
    __tablename__ = "dim_date"
    date_id = Column(Integer, primary_key=True)
    date = Column(DATE)
    year = Column(Integer)
    month = Column(Integer)
    quarter = Column(Integer)
    day = Column(Integer)
    week_day = Column(String(15))
    
class Country(Base): 
    __tablename__ = "dim_country"
    country_id = Column(Integer, primary_key=True)
    country = Column(String(10))

class Destination(Base): 
    __tablename__ = "dim_courier_destination"
    destination_id = Column(Integer, primary_key=True)
    destination_city = Column(String(50))
    destination_state = Column(String(50))
    country = relationship("Country")
    country_id = Column(Integer, ForeignKey("dim_country.country_id", ondelete='CASCADE', 
                                           onupdate='CASCADE'), nullable=False)
    
class Origin(Base): 
    __tablename__ = "dim_courier_origin"
    origin_id = Column(Integer, primary_key=True)
    origin_city = Column(String(50))
    origin_state = Column(String(50))
    origin_country = Column(String(50))
    country = relationship("Country")
    country_id = Column(Integer, ForeignKey("dim_country.country_id", ondelete='CASCADE', 
                                           onupdate='CASCADE'), nullable=False)    
        
class Courier(Base): 
    __tablename__ = "dim_courier"
    courier_id = Column(Integer, primary_key=True)
    carrier_name = Column(String(40))
    carrier_rating = Column(Integer)
    destination_id = Column(Integer, ForeignKey("dim_courier_destination.destination_id", ondelete='CASCADE', 
                                                onupdate='CASCADE'), nullable=False)
    destination = relationship("Destination")
    origin_id = Column(Integer, ForeignKey("dim_courier_origin.origin_id", ondelete='CASCADE', 
                                           onupdate='CASCADE'), nullable=False)
    origin = relationship("Origin")

class City(Base): 
    __tablename__ = 'dim_customer_city'
    city_id = Column(Integer, primary_key=True)
    customer_city = Column(String(40))    
    customer_state = Column(String(40))
    customer_country = Column(String(40))

class Payment(Base): 
    __tablename__ = 'dim_customer_payment'
    payment_id = Column(Integer, primary_key=True)
    payment_method = Column(String(40))   

class Customer(Base): 
    __tablename__ = 'dim_customer'
    customer_id = Column(Integer, primary_key=True)
    customer_name = Column(String(40))    
    customer_segment = Column(String(40))
    city_id = Column(Integer, ForeignKey("dim_customer_city.city_id", ondelete='CASCADE', 
                                         onupdate='CASCADE'), nullable=False)
    city = relationship("City")
    payment_id = Column(Integer, ForeignKey("dim_customer_payment.payment_id", ondelete='CASCADE', 
                                            onupdate='CASCADE'), nullable=False)
    payment = relationship("Payment")
    
class Shipping(Base): 
    __tablename__ = 'dim_shipping'
    shipment_id = Column(Integer, primary_key=True)
    prior_trans_id = Column(Integer, ForeignKey("dim_priority_transport.prior_trans_id", 
                                                ondelete='CASCADE', onupdate='CASCADE'), nullable=False)
    prioritytransport = relationship("PriorTransport")
    
class PriorTransport(Base): 
    __tablename__ = 'dim_priority_transport'
    prior_trans_id = Column(Integer, primary_key=True)
    mode_of_transport = Column(String(20))
    shipping_priority = Column(String(20))       


class Fact(Base): 
    __tablename__ = "fact_sales_table"
    sales_id = Column(Integer, primary_key=True)
    quantity = Column(Integer)
    total_cost = Column(Float)
    courier = relationship("Courier")
    courier_id = Column(Integer, ForeignKey("dim_courier.courier_id", ondelete='CASCADE', 
                                            onupdate='CASCADE'), nullable=False)
    
    shipment = relationship('Shipment')
    shipment_date_id = Column(Integer, ForeignKey("dim_shipment_date.shipment_date_id", ondelete='CASCADE', 
                                         onupdate='CASCADE'), nullable=False)
    
    delivery = relationship('Delivery')
    delivery_date_id = Column(Integer, ForeignKey("dim_delivery_date.delivery_date_id", ondelete='CASCADE', 
                                         onupdate='CASCADE'), nullable=False)
    
    product = relationship('Product')
    product_id = Column( Integer, ForeignKey("dim_product.product_id", ondelete='CASCADE', 
                                             onupdate='CASCADE'), nullable=False)
    
    customer = relationship('Customer')
    customer_id = Column(Integer, ForeignKey("dim_customer.customer_id", ondelete='CASCADE', 
                                             onupdate='CASCADE'), nullable=False)
     
    shipping = relationship("Shipping")
    shipment_id = Column(Integer, ForeignKey("dim_shipping.shipment_id", ondelete='CASCADE', 
                                             onupdate='CASCADE'), nullable=False)
    
    

def create_db_table(engine): 
    """
    Creates database tables based on SQLAlchemy ORM metadata.

    This function drops existing tables (if any) and creates new tables based on the SQLAlchemy ORM metadata defined in Base.

    Args:
        engine (Engine): SQLAlchemy engine object for the database.

    Raises:
        Exception: If an error occurs during the table creation process.
    """
    try: 
        # Drop existing tables
        Base.metadata.drop_all(engine, 
                               tables=[Base.metadata.tables["dim_category"], 
                                       Base.metadata.tables["dim_product"],
                                       Base.metadata.tables["dim_date"],
                                       Base.metadata.tables["dim_delivery_date"], 
                                       Base.metadata.tables["dim_shipment_date"],
                                       Base.metadata.tables["dim_courier_destination"], 
                                       Base.metadata.tables["dim_courier_origin"], 
                                       Base.metadata.tables["dim_courier"],
                                       Base.metadata.tables["dim_customer_city"], 
                                       Base.metadata.tables["dim_customer_payment"], 
                                       Base.metadata.tables["dim_customer"],
                                       Base.metadata.tables["fact_sales_table"],
                                       Base.metadata.tables["dim_shipping"], 
                                       Base.metadata.tables["dim_priority_transport"], 
                                       Base.metadata.tables["dim_country"]]) 
        # Create new tables
        Base.metadata.create_all(engine)
        
    except Exception as e: 
        raise Exception(f"Error creating tables: {e}")