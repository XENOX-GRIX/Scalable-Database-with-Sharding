# Implementation of the server

from flask import Flask, request
import os
# use mysql-connector-python
from mysql import connector as ce
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
from sqlalchemy import Table
import logging

#creating the Flask class object
app = Flask(__name__)

# Connect to the database
# Retrieve server name from environment variable
serverName = os.environ.get('SERVER_NAME')
app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql+mysqlconnector://server:password@{serverName}:3306/shardsDB'
app.config['SECRET_KEY'] = 'my key'

# Creating the SQLALchemy object
db = SQLAlchemy(app)

# ORM Model for the Employees table. Table name will be dynamically provided
def ClassFactory(name):
    # Check if the table already exists in metadata
    existing_table = db.Model.metadata.tables.get(name)
    if existing_table is not None:
        print("Table already exists")
        return type(name, (db.Model,), {'__tablename__': name, '__table__': existing_table})

    # If the table does not exist, create the table
    tabledict={'Stud_id': db.Column(db.Integer, primary_key=True),
               'Stud_name': db.Column(db.String(100)),
               'Stud_marks': db.Column(db.String(100))}

    newclass = type(name, (db.Model,), tabledict)
    print("Table created")
    return newclass


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

# Server endpoint for requests at http://localhost:5000/config, methond=POST
@app.route('/config', methods = ['POST'])
def config():

    payload = request.get_json()
    schema = payload.get('schema')
    shards = payload.get('shards')

    # checking if the schema and shards are present in the payload
    errorMessage = {}
    successMessage = {}
    successMessage["message"] = ""
    isError = False
    # Server ID taking from the environment variable named SERVER_ID
    serverID = os.environ.get('SERVER_ID')
    serverName = "Server" + str(serverID)

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
                # Check if the table already exists in metadata
                existing_table = db.Model.metadata.tables.get(shard)
                if existing_table is not None:
                    successMessage["message"] += serverName + ":" + shard + "(existing), "
                    continue
                # Creating the table in the database
                table = ClassFactory(shard)
                db.create_all()
                db.session.commit()
                successMessage["message"] += serverName + ":" + shard + ", "

            # Returning the success message along with the status code 200
            # Remove the last comma from the message
            successMessage["message"] = successMessage["message"][:-2]
            successMessage["message"] += "configured"
            successMessage["status"] = "success"
            return successMessage, 200
        
    # If the schema or shards are not present in the payload
    if isError:
        errorMessage["message"] = "Invalid Payload"
        errorMessage["status"] = "Unsuccessfull"

        # Returning the error message along with the status code 400
        return errorMessage, 400

def executeAndReturn(query):
    try:
        # start a transaction
        db.session.begin()
        
        # Execute the query using SQLAlchemy's session
        executionResult = db.session.execute(text(query))
        # Commit the transaction
        db.session.commit()
        # end the transaction
        db.session.close()

        result = executionResult.fetchall()

        if result is None or len(result) == 0:
            result = []
        return result
    except SQLAlchemyError as e:
        # Rollback the transaction in case of an error
        db.session.rollback()
        print("Error executing query:", str(e))
        return None

# endpoint to show tables. Not in the assignment. Used for testing
@app.route('/showTables', methods = ['GET'])
def showTables():

    # Query to get the list of tables in the database
    query = "SHOW TABLES"

    # Execute the query using SQLAlchemy's session
    result = executeAndReturn(query)
    print("Checking\n")
    print(result)
    print("Checking\n")

    # List to store the tables
    tables = []

    # Iterating through the result and storing the tables in the list only if result is not None or is not empty
    for row in result:
        tables.append(row[0])

    # Returning the list of tables along with the status code 200
    return {"tables": tables, "status": "success"}, 200

# # Server endpoint for requests at http://localhost:5000/copy, methond=GET
@app.route('/copy', methods = ['GET'])
def copy():
    # Getting the list of shard tables from the payload
    payload = request.get_json()
    shards = payload.get('shards')

    # Dictionary to store the data entries
    data = {}

    # Use ORM to get the data entries from the shard tables. If table is empty, return empty list
    for shard in shards:
        data[shard] = []
        table = ClassFactory(shard)
        query = db.session.query(table).all()
        for row in query:
            data[shard].append({"Stud_id":row.Stud_id, "Stud_name":row.Stud_name, "Stud_marks":row.Stud_marks})
        
    data["status"] = "success"

    # Returning the dictionary along with the status code 200
    return data, 200


# # Server endpoint for requests at http://localhost:5000/write, methond=POST
# # 5) Endpoint (/write, method=POST): This endpoint writes data entries in a shard in a particular server container. The
# # endpoint expects multiple entries to be written in the server container along with Shard id and the current index for the
# # shard. An example request-response pair is shown below.
# # Payload Json= {
# # "shard":"sh2",
# # "curr_idx": 507
# # "data": [{"Stud_id":2255,"Stud_name":GHI,"Stud_marks":27}, ...] /* 5 entries */
# # }
# # Response Json ={
# # "message": "Data entries added",
# # "current_idx": 512, /* 5 entries added */
# # "status" : "success"
# # },
# # Response Code = 200
@app.route('/write', methods = ['POST'])
def write():
    # Getting the shard, current index and data entries from the payload
    payload = request.get_json()
    shard = payload.get('shard')
    curr_idx = int(payload.get('curr_idx'))
    data = payload.get('data')
    duplicate = 0
    message = {}

    # Dictionary to store the data entries
    dataEntries = []

    # Iterating through the data entries. Use ORM to insert the data. Also check for duplicate entries and entries that does not violate the integrity constraints
    for entry in data:
        # Check if the entry already exists in the shard
        table = ClassFactory(shard)
        query = db.session.query(table).filter_by(Stud_id=entry['Stud_id']).all()
        if len(query) > 0:
            duplicate += 1
            # If the entry already exists, skip the entry
            continue
        # If the entry does not exist, add the entry to the list
        dataEntries.append(table(Stud_id=entry['Stud_id'], Stud_name=entry['Stud_name'], Stud_marks=entry['Stud_marks']))
        print("Entry added")

    # Add the data entries to the shard table
    db.session.add_all(dataEntries)
    db.session.commit()
    

    # Returning the dictionary along with the status code 200
    message["message"] = "Data entries added"
    message["current_idx"] = str(curr_idx + len(dataEntries))
    if duplicate > 0:
        if duplicate == len(data):
            message["message"] = "No data entries added. All entries are duplicate"
        else:
            message["message"] += " (" + str(duplicate) + " duplicate entries skipped)"
    message["status"] = "success"
    return message, 200

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