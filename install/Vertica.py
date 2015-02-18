#!/opt/vertica/oss/python/bin/python


import pyodbc
import os
import logging
import sys
import config

config = config

class Vertica(object):
    """docstring for conn"""

    def __init__(self):
        super(Vertica, self).__init__()
        os.environ['VERTICAINI'] = '/opt/vertica/config/vertica.ini'
        try:
            self.cn = pyodbc.connect('DSN=%s;' %config.DSN, ansi=True)
            self.cn.autocommit = True
            self.cursor = self.cn.cursor()
        except Exception, e:
            logging.fatal(e)
            logging.info("-------------------------------------")
            #mailANDsql('Unable to connect to Vertica DB DSN=%s<br><br>%s' %(config.DSN,e),False)
            logging.shutdown()
            #os.remove(pidfile)
            sys.exit(-1)

    def execute(self, query):
        try:
            #logging.debug(query)
            rows = self.cursor.execute(query).rowcount
            try:
                output = self.cursor.fetchall()
            except Exception, e:
                if not (e.message.startswith('No results') ):
                    logging.debug(e)
                output = [(None, )]
            return 0, output, rows
        except Exception, e:
            logging.error('SQL EXCEPTION %s', e)
            logging.error(query)
            #print ('SQL EXCEPTION %s', e)
            #mailANDsql('Exception during execute sqls<br><br>%s<br><br >SQL: %s' %(e,query),False)
            return -1, str(e).replace("'", '')

    def insert_many(self, query, rows):
        try:
            logging.debug(query)
            rows = self.cursor.executemany(query, rows)
            self.commit()
        except Exception, e:
            logging.error(e)
            return -1, str(e).replace("'", '')

    def commit(self):
        self.cn.commit()

    def close(self):
        self.cn.close()

    def rollback(self):
        self.cn.rollback()


    def is_cluster(self):
        rs= self.execute('select count(*)>1 as cluster from nodes')
        return bool(rs[1][0][0])

    def get_default_schema_name(self):
        return self.execute("select current_schema()")

    def set_default_schema(self, schema_name):
        if not self.has_schema(schema_name):
            self.create_schema(schema_name)
        self.cursor.execute("SET search_path TO %s" % schema_name)


    def create_schema(self, schema_name):
        self.cursor.execute("create schema IF NOT EXISTS %s" % schema_name)


    def has_schema(self, schema):
        query = ("SELECT EXISTS (SELECT schema_name FROM v_catalog.schemata "
                 "WHERE schema_name='%s')") % (schema)
        rs = self.execute(query)
        return bool(rs[1][0][0])


    def has_table(self, table_name, schema=None):
        if schema is None:
            schema = self.get_default_schema_name()[1][0][0]
        if table_name == schema:
            schema = self.get_default_schema_name()[1][0][0]
        if table_name == 'Version':
            schema = 'install'

        query = ("SELECT EXISTS ("
                 "SELECT table_name FROM v_catalog.all_tables "
                 "WHERE schema_name='%s' AND "
                 "table_name='%s'"
                 ")") % (schema, table_name)
        rs = self.execute(query)
        return bool(rs[1][0][0])


    def has_projection(self, projection_name, schema=None):
        if schema is None:
            schema = self.get_default_schema_name()[1][0][0]
        if projection_name == schema:
            schema = self.get_default_schema_name()[1][0][0]
        query = ("SELECT EXISTS ("
                 "SELECT projection_name FROM v_catalog.projections "
                 "WHERE projection_schema ='%s' AND "
                 "projection_basename ='%s'"
                 ")") % (schema, projection_name)
        rs = self.execute(query)
        return bool(rs[1][0][0])


    def has_user(self, user_name):
        query = ("SELECT EXISTS ("
                 "select 1 from v_catalog.users where user_name = '%s' "
                 ")") % (user_name)
        rs = self.execute(query)
        return bool(rs[1][0][0])


    def has_pool(self, pool_name):
        query = ("SELECT EXISTS ("
                 "select 1 from v_catalog.resource_pools where name = '%s' "
                 ")") % (pool_name)
        rs = self.execute(query)
        return bool(rs[1][0][0])



    def drop_table(self, table_name):
        """Drops a table"""
        sql = """
            DROP TABLE {table_name} CASCADE
            """.format(table_name=table_name)
        #logging("Dropping table {table_name}\n".format(table_name=table_name))
        return self.execute(sql)