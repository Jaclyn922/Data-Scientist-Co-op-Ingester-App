import cx_Oracle
import pandas as pd
import configparser
import hashlib
import sys
import uuid

hashed_password = "a36fc0ffe969954bf0dc4de4a7ba8e0886d77219a9d3c294324ccb709bf442b5"

def load_data(path):
    # ensure .xlsx file is referenced
    if not path.endswith(".xlsx"):
        raise BaseException("File must be .xlsx!")
        
    try:
        df = pd.read_excel(path, sheet_name=None)["Sheet1"]
        return df
    except FileNotFoundError:
        raise

# This will change depending on state of input file
def clean_data(df):
    df["Collection Date"] = df["Collection Date"].astype(str)
    df["FILENAME"] = "FINAL_LIMS_MGBBIOBANK1411.xlsx"
    df["ID"] = str(uuid.uuid4())[:8]
    df[" Cust. Subj."] = df[" Cust. Subj."].astype(str)
    df["Gender"] = df["Gender"].fillna("U")
    
    df.rename(columns=
          {"MGBbiobank Request ID": "MGBBIOBANK_REQUEST_ID", 
           "Lab ID": "LAB_ID", 
           " Inv. Code": "INV_CODE",
           " Cust. Subj.": "CUST_SUBJ",
           " Sample Type": "SAMPLE_TYPE",
           " Volume": "VOL",
           " Box": "BOX",
           "Box Name": "BOX_NAME",
           " Slot": "SLOT",
           "Collection Date": "COLLECTION_DATE",
           "Gender": "GENDER",
           "plasma_count": "PLASMA_COUNT",
           "Covid_Positive": "COVID_POSITIVE"
          }, inplace=True)
    
def get_oracle_cursor(config_path, input_password):
    config = configparser.ConfigParser()
    config.read(config_path)
    
    hostname = config["Database"]["hostname"]
    portnumber = config["Database"]["portnumber"]
    db_id = config["Database"]["db_id"]
    username = config["Database"]["username"]
    
    if get_hash(input_password) == hashed_password:
        dsn = cx_Oracle.makedsn(host=hostname, port=portnumber, sid=db_id)
        connection = cx_Oracle.connect(user=username, password=input_password, dsn=dsn)
        cursor = connection.cursor()
        
        return connection, cursor
    else:
        raise BaseException("Incorrect password!")
    
def get_hash(content):
    m = hashlib.sha256(content.encode())
    my_hash = m.hexdigest()
    return my_hash

# deletes all data from table
def delete_all_data(connection, cursor):
    cursor.execute("DELETE FROM MGB_BIOBANK WHERE MGBBIOBANK_REQUEST_ID=1411")
    
# prints all data from table
def get_all_data(cursor):
    print("print data")
    i = 0
    for row in cursor.execute("select * from external_data.MGB_BIOBANK"):
        if i < 5:
            print(row)
        i += 1
        
# load data (passed in as pandas dataframe) into database table
def input_data(connection, cursor, data):
    to_input = data.to_dict("records")
    cursor.executemany("insert into MGB_BIOBANK (MGBBIOBANK_REQUEST_ID, LAB_ID, INV_CODE, CUST_SUBJ, SAMPLE_TYPE, VOL, BOX, BOX_NAME, SLOT, COLLECTION_DATE, GENDER, PLASMA_COUNT, COVID_POSITIVE, FILENAME, ID) values (:MGBBIOBANK_REQUEST_ID, :LAB_ID, :INV_CODE, :CUST_SUBJ, :SAMPLE_TYPE, :VOL, :BOX, :BOX_NAME, :SLOT, :COLLECTION_DATE, :GENDER, :PLASMA_COUNT, :COVID_POSITIVE, :FILENAME, :ID)",
              to_input)

# commits a transaction
def commit(connection):
    connection.commit()
    
# rollback to last commit
def rollback(connection):
    connection.rollback()
        
        
if __name__ == "__main__":
    if len(sys.argv) < 3:
        raise BaseException("Database password or file path not specified!")
        
    input_password = sys.argv[1]
    file_path = sys.argv[2]
    connection, cursor = get_oracle_cursor("config.ini", input_password)
    
    df = load_data(file_path)
    clean_data(df)
    
    get_all_data(cursor)

    #print(uuid.uuid4())
    

    