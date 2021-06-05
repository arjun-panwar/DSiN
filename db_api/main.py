from flask import Flask, render_template, request, jsonify
import log_py
import sql
import mongodb
import casandra
app = Flask(__name__)




@app.route('/sql_postman', methods=['POST'])  # for calling the API from Postman/SOAPUI
def sql_via_postman():
    if (request.method == 'POST'):
        operation = request.json['operation']
        host = request.json['host']
        user = request.json['user']
        password = request.json['password']
        db = request.json['db']
        ob=sql.sql(host,user,password,db)
        table_name=request.json['table']
        print(host,user, password, db, table_name)



        if (operation == 'create'):
            '''
            for creating table
            JSON format
            {
                "operation":"create",
                "host":"host url",
                "user":"username",
                "password":"password",
                "db":"database name",
                "table":"table name",
                "columns":{"column name":"datatype(size)",...."}
            }
            '''
            col = request.json['columns']
            columns = ""
            for i in col:
                columns += i + " " + col[i] + ","
            columns=columns[:-1]
            ob.create_table(table_name, columns)
            msg = "Table created"
        elif (operation == 'insert'):
            '''
            for creating table
            JSON format
            {
                "operation":"insert",
                "host":"host url",
                "user":"username",
                "password":"password",
                "db":"database name",
                "table":"table name",
                "data":"data sepreated by comma"
            }
            '''
            data  = request.json['data']
            print(f"INSERT INTO {table_name} VALUES ({data})")
            ob.insert(table_name,data)
            msg = "data inserted"
        elif (operation == 'update'):
            '''
            for creating table
            JSON format
            {
                "operation":"update",
                "host":"host url",
                "user":"username",
                "password":"password",
                "db":"database name",
                "table":"table name",
                "set": "key=value pair of columns & values to be updated"
                "where": "condition"
            }
            '''
            set = request.json['set']
            where= request.json['where']


            ob.update(table_name, set,where)
            msg = "data updated"

        if (operation == 'bluk'):
            '''
            for dumping csv
            JSON format
            {
                "operation":"bluk",
                "host":"host url",
                "user":"username",
                "password":"password",
                "db":"database name",
                "table":"table name",
                "f_path":"file path"
                "columns":{"column name":"datatype(size)",...."}
            }
            '''
            f_path = request.json['filepath']
            col = request.json['columns']
            columns = ""
            for i in col:
                columns += i + " " + col[i] + ","
            columns = columns[:-1]
            ob.dump_file(f_path, table_name, columns)
            msg = "data dumped"
        if (operation == 'delete'):
            '''
           for deleting table
           JSON format
           {
               "operation":"delete",
               "host":"host url",
               "user":"username",
               "password":"password",
               "db":"database name",
               "table":"table name",
               "where": "condition"
           }
           '''

            where = request.json['where']

            ob.delete(table_name, where)
            msg = "data deleted"

        if (operation == 'download'):
            '''
                       for downloading table
                       JSON format
                       {
                           "operation":"delete",
                           "host":"host url",
                           "user":"username",
                           "password":"password",
                           "db":"database name",
                           "table":"table name",
                           
                       }
                       '''
            link=ob.download(table_name)
            msg = "you can download data using this link :   http://127.0.0.1:5000/" +link

        return jsonify(msg)


@app.route('/mongodb_postman', methods=['POST'])  # for calling the API from Postman/SOAPUI
def mongodb_via_postman():
    if (request.method == 'POST'):
        operation = request.json['operation']
        url = request.json['url']
        db = request.json['db']
        ob = mongodb.mongodb(url,db)
        collection_name = request.json['collection_name']


        if (operation == 'create'):
            '''
            for creating collection
            JSON format
            {
                "operation":"create",
                "url":connection url
                "db" : db name
                "collection_name": collection name
            }
            '''

            ob.create_collection(collection_name)
            msg = "Table created"
        elif (operation == 'insert'):
            '''
            for inserting in collection
            JSON format
            {
                "operation":"create",
                "url":connection url
                "db" : db name
                "collection_name": collection name
                "record": for single record a dict,for many record list of dict
            }
            '''
            record = request.json['record']
            ob.insert(collection_name,record)
            msg = "data inserted"
        elif (operation == 'update'):
            '''
            for updating collection
            JSON format
            {
                "operation":"create",
                "url":connection url
                "db" : db name
                "collection_name": collection name
           
                "set": "key=value pair of columns & values to be updated"
                "where": "condition"
            }
            '''
            set = request.json['set']
            where = request.json['where']

            ob.update(collection_name,set, where)
            msg = "data updated"


        if (operation == 'delete'):
            '''
           for deleting record
           JSON format
           {
               "operation":"create",
                "url":connection url
                "db" : db name
                "collection_name": collection name
               "where": "condition"
           }
           '''

            where = request.json['where']

            ob.delete(collection_name, where)
            msg = "data deleted"

        if (operation == 'download'):
            '''
                       for downloading table
                       JSON format
                       {
                            "operation":"create",
                            "url":connection url
                            "db" : db name
                            "collection_name": collection name
                       

                       }
                       '''
            link = ob.download(collection_name)
            msg = "you can download data using this link :   http://127.0.0.1:5000/" + link

        return jsonify(msg)

@app.route('/casandra_postman', methods=['POST'])  # for calling the API from Postman/SOAPUI

def casandra_via_postman():
    if (request.method == 'POST'):
        '''
        JSON format
        {

            "operation": "create",
            "zip_path": connection zip path
            "CLIENT_ID":CLIENT_ID
            "CLIENT_SECRET":CLIENT_SECRET
            "keyspace" :keyspace
            "table": table_name
        }
            
            
        '''
        operation = request.json['operation']
        zip_path = request.json['zip_path']
        CLIENT_ID = request.json['CLIENT_ID']
        CLIENT_SECRET = request.json['CLIENT_SECRET']
        keyspace = request.json['keyspace']
        ob = casandra.casandra(zip_path, CLIENT_ID,CLIENT_SECRET,keyspace)
        table_name = request.json['table']

        if (operation == 'create'):
            '''
            for creating table
            JSON format
            {
                "columns":columns names with data type and other discription 
            }
            '''
            columns = request.json['columns']


            ob.create_table(table_name, columns)
            msg = "Table created"
        elif (operation == 'insert'):
            '''
            for inserting table
            JSON format
            {
                "columns_name":columns name
                "data":"data sepreated by comma"
            }
            '''
            columns = request.json['columns_name']
            record = request.json['data']
            ob.insert(table_name,columns,record)
            msg = "data inserted"
        elif (operation == 'update'):
            '''
            for update table
            JSON format
            {
                "set": "key=value pair of columns & values to be updated"
                "where": "condition"
            }
            '''
            set = request.json['set']
            where = request.json['where']

            ob.update(table_name, set, where)
            msg = "data updated"


        if (operation == 'delete'):
            '''
           for deleting table
           JSON format
           {
               "where": "condition"
           }
           '''

            where = request.json['where']

            ob.delete(table_name, where)
            msg = "data deleted"



        return jsonify(msg)


if __name__ == '__main__':
    app.run()
