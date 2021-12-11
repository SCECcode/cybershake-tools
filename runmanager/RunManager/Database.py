#!/usr/bin/env python3

import sys
import pymysql


class Database:
    host = ""
    port = 0
    user = ""
    password = ""
    db = ""
    connection = None
    cursor = None
    html = True

    def __init__(self, host, port, user, password, db):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.connection = None
        self.cursor = None
        self.html = True


    def _printError(self, str):
        if (self.html):
            print("%s<p>" % (str))
        else:
            print("%s" % (str))
        return


    def useHTML(self, flag):
        self.html = flag
        return
    
        
    def open(self):
        try:
            self.connection = pymysql.connect(host=self.host,\
                                                port=self.port,user=self.user,\
                                                passwd=self.password,db=self.db)
            self.cursor = self.connection.cursor()
        except pymysql.OperationalError as message:
            self._printError("Error %d:\n%s" % (message[0], message[1]))
            return 1
        return 0


    def close(self):
        try:
            self.connection.close()
        except pymysql.OperationalError as message:
            self._printError("Error %d:\n%s" % (message[0], message[1]))
            return 1
        return 0


    def beginTransaction(self):
        try:
            self.cursor.execute("BEGIN")
            #self.connection.begin()
        except pymysql.OperationalError as message:
            self._printError("Error %d:\n%s" % (message[0], message[1]))
            return 1
        return 0


    def commit(self):
        try:
            self.connection.commit()
        except pymysql.OperationalError as message:
            self._printError("Error %d:\n%s" % (message[0], message[1]))
            return 1
        return 0        


    def rollback(self):
        try:
            self.connection.rollback()
        except pymysql.OperationalError as message:
            self._printError("Error %d:\n%s" % (message[0], message[1]))
            return 1
        return 0        


    def execsql(self, sql):
        try:
            self.cursor.execute(sql)
        except pymysql.DatabaseError as message:
            self._printError("Error %d:\n%s" % (message[0], message[1]))
            return 1
        return 0


    def execsqlbulk(self, sql, data):
        try:
            self.cursor.executemany(sql, data)
        except pymysql.DatabaseError as message:
            self._printError("Error %d:\n%s" % (message[0], message[1]))
            return 1
        return 0


    def getResultsAll(self):
        try:
            results = self.cursor.fetchall()
            return results
        except pymysql.DatabaseError as message:
            self._printError("Error %d:\n%s" % (message[0], message[1]))

        return None


    def getResultsNext(self):
        try:
            results = self.cursor.fetchone()
            return results
        except pymysql.DatabaseError as message:
            self._printError("Error %d:\n%s" % (message[0], message[1]))

        return None


