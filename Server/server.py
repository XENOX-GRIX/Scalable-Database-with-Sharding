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
        # end the transaction
        db.session.close()

        return True
    except SQLAlchemyError as e:
        # Rollback the transaction in case of an error
        db.session.rollback()
        print("Error executing query:", str(e))
        return False
    
def executeAndReturn(query):
    try:
        # start a transaction
        db.session.begin()
        
        # Execute the query using SQLAlchemy's session
        result = db.session.execute(text(query))
        # Commit the transaction
        db.session.commit()
        # end the transaction
        db.session.close()

        return result.fetchall()
    except SQLAlchemyError as e:
        # Rollback the transaction in case of an error
        db.session.rollback()
        print("Error executing query:", str(e))
        return None

def createTable(table, columns, dtypes):
    # mapping for the data types
    dtypeMap = {"Number":"INT", "String":"VARCHAR2(100)"}

    # Check if the table already exists
    query = "SHOW TABLES LIKE '" + table + "'"
    result = executeAndReturn(query)

    # If the table already exists, return
    if len(result) > 0:
        print("Table already exists")
        return
    

    # Creating query to create the table in the database
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

    payload = request.get_json()
    schema = payload.get('schema')
    shards = payload.get('shards')

    # checking if the schema and shards are present in the payload
    errorMessage = {}
    isError = False

    if schema is None or shards is None:
        isError = True
        
    else:
        # Getting 'columns' and 'dtypes' from the schema
        columns = schema.get('columns')
        dtypes = schema.get('dtypes')

        # Checking if the columns and dtypes are present in the schema
        if columns is None or dtypes is None:
            isError = True
        else:
            # Creating the shards in the database
            for shard in shards:
                # Creating the table in the database
                createTable(shard, columns, dtypes)

            # Server ID taking from the environment variable named SERVER_ID
            serverID = os.environ.get('SERVER_ID')

            serverName = "Server" + str(serverID)

            # Returning the success message
            successMessage = {}
            successMessage["message"] = ""
            for shard in shards:
                successMessage["message"] += serverName + ":" + shard + ", "
            successMessage["message"] += "configured"
            successMessage["status"] = "success"
            return successMessage, 200
        
    # If the schema or shards are not present in the payload
    if isError:
        errorMessage["message"] = "Invalid Payload"
        errorMessage["status"] = "Unsuccessfull"

        # Returning the error message along with the status code 400
        return errorMessage, 400

# endpoint to show tables. Not in the assignment. Used for testing
@app.route('/showTables', methods = ['GET'])
def showTables():

    # Query to get the list of tables in the database
    query = "SHOW TABLES"

    # Execute the query using SQLAlchemy's session
    result = executeAndReturn(query)

    # List to store the tables
    tables = []

    # Iterating through the result and storing the tables in the list
    for row in result:
        tables.append(row[0])

    # Returning the list of tables along with the status code 200
    return {"tables": tables, "status": "success"}, 200

# Server endpoint for requests at http://localhost:5000/copy, methond=GET
# Endpoint (/copy, method=GET): This endpoint returns all data entries corresponding to one shard table in the server
# container. Copy endpoint is further used to populate shard tables from replicas in case a particular server container fails,
# as shown in Fig. 1. An example request-response pair is shown below.
# Payload Json= {
# "shards":["sh1","sh2"]
# }
# Response Json ={
# "sh1" : [{"Stud_id":1232,"Stud_name":ABC,"Stud_marks":25},
# {"Stud_id":1234,"Stud_name":DEF,"Stud_marks":28},
# ....],
# "sh2" : [{"Stud_id":2255,"Stud_name":GHI,"Stud_marks":27},
# {"Stud_id":2535,"Stud_name":JKL,"Stud_marks":23},
# ....],
# "status" : "success"
# },
# Response Code = 200
@app.route('/copy', methods = ['GET'])
def copy():
    # Getting the list of shard tables from the payload
    payload = request.get_json()
    shards = payload.get('shards')

    # Dictionary to store the data entries
    data = {}

    # Iterating through the shard tables
    for shard in shards:
        # Query to get the data entries from the shard table
        query = "SELECT * FROM " + shard

        # Execute the query using SQLAlchemy's session
        result = executeAndReturn(query)

        # List to store the data entries
        dataEntries = []

        # Iterating through the result and storing the data entries in the list
        for row in result:
            dataEntry = {}
            for i in range(len(row)):
                if i == 0:
                    dataEntry["Stud_id"] = row[i]
                elif i == 1:
                    dataEntry["Stud_name"] = row[i]
                elif i == 2:
                    dataEntry["Stud_marks"] = row[i]
            dataEntries.append(dataEntry)
        
        # Storing the data entries in the dictionary
        data[shard] = dataEntries

    # Returning the dictionary along with the status code 200
    return data, 200


# Server endpoint for requests at http://localhost:5000/write, methond=POST
# 5) Endpoint (/write, method=POST): This endpoint writes data entries in a shard in a particular server container. The
# endpoint expects multiple entries to be written in the server container along with Shard id and the current index for the
# shard. An example request-response pair is shown below.
# Payload Json= {
# "shard":"sh2",
# "curr_idx": 507
# "data": [{"Stud_id":2255,"Stud_name":GHI,"Stud_marks":27}, ...] /* 5 entries */
# }
# Response Json ={
# "message": "Data entries added",
# "current_idx": 512, /* 5 entries added */
# "status" : "success"
# },
# Response Code = 200
@app.route('/write', methods = ['POST'])
def write():
    # Getting the shard, current index and data entries from the payload
    payload = request.get_json()
    shard = payload.get('shard')
    curr_idx = int(payload.get('curr_idx'))
    data = payload.get('data')

    # Dictionary to store the data entries
    dataEntries = []

    # Iterating through the data entries
    for entry in data:
        # Query to insert the data entry in the shard table
        query = "INSERT INTO " + shard + " ("
        for key in entry:
            query += key + ", "
        query = query[:-2] + ") VALUES ("
        for key in entry:
            query += "'" + str(entry[key]) + "', "
        query = query[:-2] + ")"

        # Execute the query using SQLAlchemy's session
        if executeQuery(query):
            dataEntries.append(entry)
        else:
            # Returning the error message along with the status code 400
            return {"message": "Error adding data entries", "status": "Unsuccessfull"}, 400

    # Returning the dictionary along with the status code 200
    return {"message": "Data entries added", "current_idx": str(curr_idx + len(dataEntries)), "status": "success"}, 200

# Server endpoint for requests at http://localhost:5000/read, methond=POST
@app.route('/read', methods = ['POST'])
def read():
    message = {"mssage" : "To be implemented"}
    return message, 200

# Server endpoint for requests at http://localhost:5000/update, methond=PUT
@app.route('/update', methods = ['PUT'])
def update():
    message = {"mssage" : "To be implemented"}
    return message, 200

# Server endpoint for requests at http://localhost:5000/del, methond=DELETE
@app.route('/del', methods = ['DELETE'])
def delete():
    message = {"mssage" : "To be implemented"}
    return message, 200
    
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)