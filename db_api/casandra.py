from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

import log_py
import pandas as pd

class casandra:
    '''
    casandra class through with we can perform most of the casandra tasks using python

    Parameters
    ----------
    host: host URL of MySQL server
    user: user name
    passwd: password
    db: database name- default empty string ("")
    '''

    def __init__(self,zip_path, CLIENT_ID,CLIENT_SECRET,keyspace):
        '''
        init function of sql class
        '''
        cloud_config = {'secure_connect_bundle': f"{zip_path}"}
        auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        self.session = cluster.connect()
        self.keyspace=keyspace
        self.logger = log_py.App_Logger("casandra_logs.txt")  # creating App_Logger object
        row = self.session.execute("select release_version from system.local").one()
        if row:
            self.logger.log("info", " cansader object created")
        else:
            self.logger.log("error", "cansader object not created")  # logging



        # Create Keyspace
        try:
            try:
                self.session.execute(f"USE {keyspace}")
                self.logger.log("info", " Keyspace selected")
            except:
                query = f"CREATE KEYSPACE {keyspace} WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 3};"
                self.session.execute(query)
                self.logger.log("info", " Keyspace created")
        except Exception as e:
            self.logger.log("info", " Keyspace not created-",e)


    def create_table(self, table_name, columns):
        '''
        Function create_ table is used to create a new table

        Parameters
        ----------
        table_name: table name
        columns: columns names with data type and other discription in SQL format
        '''
        try:
            self.session.execute(f"CREATE TABLE {table_name}({columns})")

            self.logger.log("info", f"{table_name} table created with columns: {columns}")  # logging
        except Exception as e:

            self.logger.log("error", f"table not created error : {str(e)}")  # logging

    def insert(self, table_name, column_name,record):
        '''
        Function insert is used to insert value in table

        Parameters
        ----------
        table_name: table name
        data: values to be inserted
        '''
        try:
            v=((column_name.count(",")+1)*"%s,")[:-1]
            query = f"INSERT INTO {table_name} ({column_name}) VALUES({v})"
            self.session.execute(query, record)

            self.logger.log("info", f"inserted successfully")  # logging
        except Exception as e:

            self.logger.log("error", f"insert error : {str(e)}")  # logging

    def update(self, table_name, set,where):
        '''
                Function update is used to update value in table

                Parameters
                ----------
                table_name: table name
                set: key=value pair of columns & values to be updated
                where: condition
                '''
        try:
            self.session.execute(f"UPDATE {table_name} SET {set} WHERE {where}")  # executing Query
            self.logger.log("info", f"update successfully")  # logging
        except Exception as e:
            self.logger.log("error", f"update error : {str(e)}")  # logging


    def delete(self, table_name, where):
        '''
        Function delete is used to delete row from table

        Parameters
        ----------
        table_name: table name
        where: condition
        '''
        try:
            self.session.execute(f"DELETE FROM {table_name}  WHERE {where}")  # executing Query
            self.logger.log("info", f"deleted successfully")  # logging
        except Exception as e:
            self.logger.log("error", f"delete error : {str(e)}")  # logging



