import configparser
import hashlib

hashed_password = "a36fc0ffe969954bf0dc4de4a7ba8e0886d77219a9d3c294324ccb709bf442b5"

def get_hash(content):
    m = hashlib.sha256(content.encode())
    my_hash = m.hexdigest()
    return my_hash

def get_oracle_cursor():
    config = configparser.ConfigParser()
    config.read("../config.ini")
    
    hostname = config["Database"]["hostname"]
    portnumber = config["Database"]["portnumber"]
    db_id = config["Database"]["db_id"]
    username = config["Database"]["username"]
    password = config["Database"]["password"]
    
    if get_hash(password) == hashed_password:
        dsn = cx_Oracle.makedsn(host=hostname, port=portnumber, sid=db_id)
        connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()
        
        return connection, cursor
    else:
        raise BaseException("Incorrect password!")
        
# prints all data from table
def get_all_data(cursor):
    print("print data")
    i = 0
    for row in cursor.execute("select * from external_data.MGB_BIOBANK"):
        if i < 5:
            print(row)
        i += 1
        
def get_database_columns(cursor):
    col_names = []
    
    cursor.execute("select * from MGB_BIOBANK")
    for column in cursor.description:
        col_names.append(column[0])
        
    return col_names
