from sqlalchemy import create_engine

def db_connection(username, password, host, db_name): 
    """
    Establishes a connection to a PostgreSQL database.

    Args:
        username (str): Username for database authentication.
        password (str): Password for database authentication.
        host (str): Hostname or IP address of the PostgreSQL server.
        db_name (str): Name of the database to connect to.

    Returns:
        tuple: A tuple containing the database connection and the SQLAlchemy engine.

    Raises:
        Exception: If an error occurs during the connection process.
    """
    try: 
        # Construct the connection string
        conn_str = f'postgresql+psycopg2://{username}:{password}@{host}:5432/{db_name}' 
        
        # Create an SQLAlchemy engine
        engine = create_engine(conn_str, echo=True, isolation_level = "AUTOCOMMIT")
        
        # Connect to the database
        connection = engine.connect()
        
        return connection, engine
    
    except Exception as e: 
        # Raise an exception if an error occurs
        raise Exception (f"Error: {e}")
