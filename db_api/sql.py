import mysql.connector as connection  # importing MYSQL connector
import log_py
import pandas as pd

class sql:
    '''
    SQL class through with we can perform most of the SQL tasks using python

    Parameters
    ----------
    host: host URL of MySQL server
    user: user name
    passwd: password
    db: database name- default empty string ("")
    '''

    def __init__(self, host, user, passwd, db):
        '''
        init function of sql class
        '''
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.logger = log_py.App_Logger("sql_logs.txt")  # creating App_Logger object
        self.logger.log("info", "SQL object created")  # logging

    def conn(self):
        '''
        Function conn is used to make connection to SQL server

        Parameters
        ----------

        '''
        try:
            if self.db == "":
                # connection without db
                return connection.connect(host=self.host, user=self.user, passwd=self.passwd)
            else:
                # connection with db
                return connection.connect(host=self.host, user=self.user, database=self.db, passwd=self.passwd)
        except Exception as e:
            self.logger.log("error", f"connection error : {str(e)}")  # logging
            print(str(e))

    def create_table(self, table_name, columns):
        '''
        Function create_ table is used to create a new table

        Parameters
        ----------
        table_name: table name
        columns: columns names with data type and other discription in SQL format
        '''
        try:
            conn = self.conn()  # making connection
            cursor = conn.cursor()  # create a cursor to execute queries
            cursor.execute(f"CREATE TABLE {table_name} ({columns})")  # executing Query
            conn.close()  # connection closed
            self.logger.log("info", f"{table_name} table created with columns: {columns}")  # logging
        except Exception as e:
            conn.close()  # connection closed
            print(str(e))
            self.logger.log("error", f"table not created error : {str(e)}")  # logging

    def insert(self, table_name, data):
        '''
        Function insert is used to insert value in table

        Parameters
        ----------
        table_name: table name
        data: values to be inserted
        '''
        try:
            conn = self.conn()  # making connection
            cursor = conn.cursor()  # create a cursor to execute queries

            cursor.execute(f"INSERT INTO {table_name} VALUES ({data})")  # executing Query
            conn.commit()  # commiting the query
            conn.close()  # connection closed

            self.logger.log("info", f"inserted successfully")  # logging
        except Exception as e:
            conn.close()  # connection closed
            self.logger.log("error", f"insert error : {str(e)}")  # logging

    def dump_file(self, f_name, t_name, columns, csv=True):
        '''
        Function dump_file is used to dump a csv into a table

        Parameters
        ----------
        f_name: file name
        t_name: table name
        columns:  columns names with data type and other discription in SQL format
        csv: True if csv file is comma separated otherwise False if csv file is semicolon separated
        '''
        try:
            f = open(f_name, "r")  # opening file in read mode
            f.readline()  # reading first line to skip columns line in file
            self.create_table(t_name, columns)  # creating table

            for line in f.readlines():  # reading file line by line
                if csv:
                    data = "\'" + line[:-1].replace(",", "\',\'") + "\'"  # data format if comma separated
                    print(data)
                else:
                    data = "\'" + line[:-1].replace(";", "\',\'") + "\'"  # data format if semicolon separated
                self.insert(t_name, data)  # inserting data

            self.logger.log("info", f"{f_name} file data dumped to {t_name} table")  # logging

        except Exception as e:
            print(str(e))
            self.logger.log("error", f"file dump error : {str(e)}")  # logging

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
            conn = self.conn()  # making connection
            cursor = conn.cursor()  # create a cursor to execute queries

            cursor.execute(f"UPDATE {table_name} SET {set} WHERE {where}")  # executing Query
            conn.commit()  # commiting the query
            conn.close()  # connection closed
            self.logger.log("info", f"update successfully")  # logging
        except Exception as e:
            conn.close()  # connection closed
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
            conn = self.conn()  # making connection
            cursor = conn.cursor()  # create a cursor to execute queries

            cursor.execute(f"DELETE FROM {table_name}  WHERE {where}")  # executing Query
            conn.commit()  # commiting the query
            conn.close()  # connection closed
            self.logger.log("info", f"deleted successfully")  # logging
        except Exception as e:
            conn.close()  # connection closed
            self.logger.log("error", f"delete error : {str(e)}")  # logging


    def download(self, table_name):

        df = pd.read_sql_query(f"SELECT * FROM {table_name}", self.conn())
        df.to_csv(f"static/{table_name}.csv",index=False)
        return f"static/{table_name}.csv"

