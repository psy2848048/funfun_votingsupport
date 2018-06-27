import pymysql
import config

class DBActions(object):
    def getConnection(self):
        return pymysql.connect(**(config.DBINFO)
                , charset='utf8', cursorclass=pymysql.cursors.DictCursor)

    def closeConnection(self, conn):
        conn.close()
