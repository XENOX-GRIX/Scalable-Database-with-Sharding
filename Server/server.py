# Implementation of the server

from flask import Flask, request
import os
# use mysql-connector-python
from mysql import connector as ce
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
import logging

#creating the Flask class object
app = Flask(__name__)

# Connect to the database
#mysql_engine = ce('mysql://root:password@localhost:3306/shardsDB')
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://root:password@localhost:3306/shardsDB'
app.config['SECRET_KEY'] = 'my key'

# Creating the SQLALchemy object
db = SQLAlchemy(app)

# Set SQLAlchemy's logger to DEBUG level
logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)

# Create a logger
logger = logging.getLogger(__name__)

# Configure the logger to print messages to the console
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


# Server endpoint for requests at http://localhost:5000/home, methond=GET
@app.route('/home', methods = ['GET'])
def home():
    # Server ID taking from the environment variable named SERVER_ID
    serverID = os.environ.get('SERVER_ID')

    # Dictionary to return as a JSON object
    serverHomeMessage =  {"message": "Hello from Server: [" + str(serverID) + "]",
                          "status": "successfull"}

    # Returning the JSON object along with the status code 200
    return serverHomeMessage, 200

# Server endpoint for requests at http://localhost:5000//heartbeat, method=GET
@app.route('/heartbeat', methods = ['GET'])
def heartbeat():
    # Returning empty response along with status code 200
    return "", 200

# Server endpoints for all other requests. Kind of error handler
@app.route('/', defaults={'path': ''}, methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD'])
@app.route('/<path:path>', methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD'])
def invalidUrlHandler(path):
    # Returning an error message stating the valid endpoints
    errorMessage = {"message": "Invalid Endpoint",
                    "Valid Endpoints": ["/home method='GET'", "/heartbeat method='GET'"],
                    "status": "Unsuccessfull"}
    
    # Returning the JSON object along with the status code 404
    return errorMessage, 404

def executeQuery(query):
    try:
        # start a transaction
        db.session.begin()
        
        # Execute the query using SQLAlchemy's session
        db.session.execute(text(query))
        # Commit the transaction
        db.session.commit()
        return True
    except SQLAlchemyError as e:
        # Rollback the transaction in case of an error
        db.session.rollback()
        print("Error executing query:", str(e))
        return False

def createTable(shard, columns, dtypes):
    # mapping for the data types
    dtypeMap = {"Number":"INT", "String":"VARCHAR(255)"}

    # Creating the table in the database
    table = shard
    columns = columns
    dtypes = dtypes
    query = "CREATE TABLE " + table + " ("
    for i in range(len(columns)):
        query += columns[i] + " " + dtypeMap[dtypes[i]] + ","
    # Make 'Stud_id' as primary key
    query += " PRIMARY KEY (Stud_id)"

    # close the query
    query += ")"

    print(query)

    # Creating the table in the database
    if executeQuery(query):
        print("Table created successfully")
    else:
        print("Error creating table")

# Server endpoint for requests at http://localhost:5000/config, methond=POST
@app.route('/config', methods = ['POST'])
def config():
    # 1) Endpoint (/config, method=POST): This endpoint initializes the shard tables in the server database after the container
    # is loaded. The shards are configured according to the request payload. An example request-response pair is shown below.

    # Payload Json= {
    # "schema":{"columns":["Stud_id","Stud_name","Stud_marks"],
    # "dtypes":["Number","String","String"]}
    # "shards":["sh1","sh2"]
    # }
    # Response Json ={
    # "message" : "Server0:sh1, Server0:sh2 configured",
    # "status" : "success"
    # },
    # Response Code = 200

    payload = request.get_json()
    schema = payload.get('schema')
    shards = payload.get('shards')

    # checking if the schema and shards are present in the payload
    errorMessage = {}
    if schema is None or shards is None:
        # Returning an error message stating the valid endpoints
        errorMessage = {"message": "Invalid payload",
                        "status": "Unsuccessfull"}
        
    else:
        # Getting 'columns' and 'dtypes' from the schema
        columns = schema.get('columns')
        dtypes = schema.get('dtypes')

        # Checking if the columns and dtypes are present in the schema
        if columns is None or dtypes is None:
            # Returning an error message stating the valid endpoints
            errorMessage = {"message": "Invalid payload",
                            "status": "Unsuccessfull"}
        else:
            # Creating the shards in the database
            for shard in shards:
                # Creating the table in the database
                createTable(shard, columns, dtypes)

            # Returning the success message
            successMessage = {"message": "Server0:sh1, Server0:sh2 configured",
                              "status": "success"}
            return successMessage, 200
        
    # Returning the error message along with the status code 400
    return errorMessage, 400

# endpoint to show tables
@app.route('/config', methods = ['GET'])
def showTables():

    # Query to get the list of tables in the database
    query = "SHOW TABLES"

    # Execute the query using SQLAlchemy's session
    result = db.session.execute(text(query))

    # List to store the tables
    tables = []

    # Iterating through the result and storing the tables in the list
    for row in result:
        tables.append(row[0])

    # Returning the list of tables along with the status code 200
    return {"tables": tables, "status": "success"}, 200
    
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)