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

# # Connect to the database
# # Configuration
# DATABASE_USER = os.environ.get('MYSQL_USER', 'server')
# DATABASE_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'password')
# DATABASE_DB = os.environ.get('MYSQL_DATABASE', 'shardsDB')
# DATABASE_HOST = os.environ.get('MYSQL_HOST', 'localhost')
# DATABASE_PORT = os.environ.get('MYSQL_PORT', '3306')

# app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql+mysqlconnector://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_DB}'
# app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# For local testing
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://root:password@localhost:3306/shardsDB'

# Creating the SQLALchemy object
db = SQLAlchemy(app)

# ORM Model for the Employees table. Table name will be dynamically provided
def ClassFactory(name):
    # Check if the table already exists in metadata
    existing_table = db.Model.metadata.tables.get(name)
    if existing_table is not None:
        return type(name, (db.Model,), {'__tablename__': name, '__table__': existing_table})

    # If the table does not exist, create the table
    tabledict={'Stud_id': db.Column(db.Integer, primary_key=True),
               'Stud_name': db.Column(db.String(100)),
               'Stud_marks': db.Column(db.String(100))}

    newclass = type(name, (db.Model,), tabledict)
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

    try:
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
    except Exception as e:
        errorMessage["message"] = "Error: " + str(e)

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
        return "Error: " + str(e)

# endpoint to show tables. Not in the assignment. Used for testing
@app.route('/showTables', methods = ['GET'])
def showTables():

    # Query to get the list of tables in the database
    query = "SHOW TABLES"

    # Execute the query using SQLAlchemy's session
    result = executeAndReturn(query)

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
    try:
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
    except Exception as e:
        data = {"message": "Error: " + str(e), "status": "Unsuccessfull"}

    # Returning the dictionary along with the status code 200
    return data, 200

@app.route('/write', methods = ['POST'])
def write():
    try:
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
    except Exception as e:
        message = {"message": "Error: " + str(e), "status": "Unsuccessfull"}

    return message, 200

@app.route('/read', methods = ['POST'])
def read():
    message = {}

    try:
        # Getting the shard, low and high from the payload
        payload = request.get_json()
        shard = payload.get('shard')
        low = int(payload.get('Stud_id').get('low'))
        high = int(payload.get('Stud_id').get('high'))

        # List to store the data entries
        data = []

        # Use ORM to get the data entries from the shard table. If table is empty, return empty list
        table = ClassFactory(shard)
        query = db.session.query(table).filter(table.Stud_id >= low, table.Stud_id <= high).all()
        for row in query:
            data.append({"Stud_id":row.Stud_id, "Stud_name":row.Stud_name, "Stud_marks":row.Stud_marks})
        message["data"] = data
        message["status"] = "success"
    except Exception as e:
        message = {"message": "Error: " + str(e), 
                   "status": "Unsuccessfull"}

    return message, 200

# Server endpoint for requests at http://localhost:5000/update, methond=PUT
@app.route('/update', methods = ['PUT'])
def update():
    message = {"mssage" : "To be implemented"}
    return message, 200

# Server endpoint for requests at http://localhost:5000/del, methond=DELETE
@app.route('/del', methods = ['DELETE'])
def delete():
    message = {}

    try:
        # Getting the shard and Stud_id from the payload
        payload = request.get_json()
        shard = payload.get('shard')
        Stud_id = int(payload.get('Stud_id'))

        # Use ORM to delete the data entry from the shard table
        table = ClassFactory(shard)
        query = db.session.query(table).filter_by(Stud_id=Stud_id).delete()
        db.session.commit()
        message["message"] = "Data entry with Stud_id:" + str(Stud_id) + " removed"
        message["status"] = "success"
    except Exception as e:
        message = {"message": "Error: " + str(e), 
                   "status": "Unsuccessfull"}

    return message, 200
    
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)