from sqlalchemy import  Column, Integer, ForeignKey, String, DATE
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref


Base = declarative_base()


class Product(Base): 
    __tablename__ = "dim_product"
    product_id = Column( Integer, primary_key=True)
    name = Column(String(40))
    shipping_priority = Column(String(40))
    price = Column(Integer)
    shipping_priority = Column(String(40))
    categor_id = Column(Integer, ForeignKey("dim_category.category_id"), nullable=False)
    Category = relationship("Category", backref=backref('Product'))
    
class Category(Base): 
    __tablename__ = "dim_category"
    category_id = Column(Integer, primary_key=True)
    name = Column(String(40))


class Time(Base): 
    __tablename__ = "dim_date"
    date_id = Column(Integer, primary_key=True)
    delivery_date_id = Column(Integer, ForeignKey("dim_delivery_date.delivery_date_id"), nullable=False)
    Delivery = relationship("Delivery", backref=backref('Time'))
    shipment_date_id = Column(Integer, ForeignKey("dim_shipment_date.shipment_date_id"), nullable=False)
    Shipment = relationship("Shipment", backref=backref('Time'))

class Delivery(Base): 
    __tablename__ = "dim_delivery_date"
    delivery_date_id = Column(Integer, primary_key=True)
    date = Column(DATE)
    month = Column(DATE)
    quater = Column(Integer)
    year = Column(DATE)
    day = Column(Integer)
    week_day = Column(String(15))

class Shipment(Base): 
    __tablename__ = "dim_shipment_date"
    shipment_date_id = Column(Integer, primary_key=True)
    date = Column(DATE)
    month = Column(DATE)
    quater = Column(Integer)
    year = Column(DATE)
    day = Column(Integer)
    week_day = Column(String(15))
    
class Courier(Base): 
    __tablename__ = "dim_courier"
    courier_id = Column(Integer, primary_key=True)
    name = Column(String(40))
    rating = Column(Integer)
    mode_of_transport = Column(String(40))
    destination_id = Column(Integer, ForeignKey("dim_courier_destination, destination_id"), nullable=False)
    Destination = relationship("Destination", backref=backref("Courier"))
    origin_id = Column(Integer, ForeignKey("dim_courier_origin, origin_id"), nullable=False)
    Destination = relationship("Origin", backref=backref("Courier"))

    

class Destination(Base): 
    __tablename__ = "dim_courier_destination"
    destination_id = Column(Integer, primary_key=True)
    city = Column(String(50))
    state = Column(String(50))
    country = Column(String(50))

class Origin(Base): 
    __tablename__ = "dim_courier_origin"
    origin_id = Column(Integer, primary_key=True)
    city = Column(String(50))
    state = Column(String(50))
    country = Column(String(50))



class Customer(Base): 
    __tablename__ = 'dim_customer'
    customer_id = Column(Integer, primary_key=True)
    name = Column(String(40))    
    segment = Column(String(40))
    city_id = Column(Integer, ForeignKey("dim_customer_city.city_id"), nullable=False)
    City = relationship("City", backref=backref("Customer"))
    payment_id = Column(Integer, ForeignKey("dim_customer_payment.payment_id"), nullable=False)
    Payment = relationship("Payment", backref=backref("Customer"))
    
class City(Base): 
    __tablename__ = 'dim_customer_city'
    city_id = Column(Integer, primary_key=True)
    city = Column(String(40))    
    state = Column(String(40))
    country = Column(Integer)

class Payment(Base): 
    __tablename__ = 'dim_customer_payment'
    payment_id = Column(Integer, primary_key=True)
    method = Column(String(40))    


    
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
                               tables=[Base.metadata.tables["dim_category"], 
                                       Base.metadata.tables["dim_product"],
                                       Base.metadata.tables["dim_delivery_date"], 
                                       Base.metadata.tables["dim_shipment_date"],
                                       Base.metadata.tables["dim_date"],
                                       Base.metadata.tables["dim_courier_destination"], 
                                       Base.metadata.tables["dim_courier_origin"], 
                                       Base.metadata.tables["dim_courier"],
                                       Base.metadata.tables["dim_customer_city"], 
                                       Base.metadata.tables["dim_customer_payment"], 
                                       Base.metadata.tables["dim_customer"],
                                       Base.metadata.tables["fact_table"]]) 
        # Create new tables
        Base.metadata.create_all(engine)
        print("all table successfully created")
        
    except Exception as e: 
        raise Exception(f"Error creating tables: {e}")