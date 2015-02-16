#!/opt/vertica/oss/python/bin/python

import config
import logging
import argparse
import re
from os.path import join as path_join
import fnmatch
from stat import *
import sys
import os


from Vertica import *


def comp_versions(current, target):
    if not current:
        return True
    a = current.split(".")
    b = target.split(".")
    current_v = []
    target_v = []
    for n in a:
        current_v.append(int(n))
    for n in b:
        target_v.append(int(n))
    return target_v >= current_v

def get_current_version():
    sql = 'select Version , InstallTime from install.Version order by InstallTime desc limit 1'
    return vert.execute(sql)

def get_dirs_to_process(schema_dir, current_version, target_version):
#ToDo check if exists target_version
    all_files = os.listdir(schema_dir)
    filtered = fnmatch.filter(all_files, 'v*')
    if not filtered:
        logging.info('DONE: no version to install')
        vert.close()
        sys.exit(0)
    dir_to_process = {}
    for item in sorted(filtered):
        item_ver = item.replace('v', '').strip(' ')
        if config.is_rerun_the_installed_version and current_version == target_version == item_ver:
            dir_to_process.append(path_join(schema_dir, item))
            break
        if comp_versions(item_ver, current_version):
            #print('%s ; %s ' % (item_ver, current_version.strip(' ')))
            continue

        if comp_versions(item_ver, target_version):
            dir_to_process[item_ver] = path_join(schema_dir, item)
    return dir_to_process


def get_files_to_process(dir):

    for f in os.listdir(dir):
        pathname = os.path.join(dir, f)
        mode = os.stat(pathname)[ST_MODE]
        if S_ISDIR(mode):
            # It's a directory, recurse into it
            get_files_to_process(pathname)
        elif S_ISREG(mode):
            # It's a file, call the callback function
            files_to_process.append(pathname)
        else:
            # Unknown file type, print a message
            pass
    return files_to_process


def parse_file(full_path):

    sql_file = open(full_path, 'r')
    sql_query = sql_file.read()
    sql_file.close()

    return sql_query


def execute_file(dir,file):

    sql = parse_file(path_join(dir,file))
    sql = ksafe(sql, ksafety)

    return vert.execute(sql)


def execute_misc_file(file):

    #sql = parse_file(path_join(dir,file))
    sql = parse_file(file)
    sql = ksafe(sql, ksafety)
    sqls = sql.split(";")
    pattern_user = re.compile(' *create * user', re.IGNORECASE)
    pattern_pool = re.compile(' *create * RESOURCE * POOL', re.IGNORECASE)
    pattern_table = re.compile(' *create * table * ', re.IGNORECASE)
    pattern_if_exists = re.compile(' *if * not * exists', re.IGNORECASE)
    pattern_projection = re.compile(' *create * projection * ', re.IGNORECASE)
    pattern_schema = re.compile(' *create * schema * ', re.IGNORECASE)
    for sql in sqls:
        if len(sql.replace(' ', '')) > 5:
            sql = sql.strip(' \t\n\r')
            #print 'SQL IS: %s' %sql
            if re.match(pattern_user, sql) and vert.has_user(sql.split()[2]):
                logging.info(' User %s exists' % sql.split()[2])
                continue
            if re.match(pattern_pool, sql) and vert.has_pool(sql.split()[3]):
                logging.info(' Resource Pool %s exists' % sql.split()[3])
                continue
            if re.match(pattern_table, sql) and not re.match(pattern_if_exists, sql)\
                    and vert.has_table(sql.split()[2].split('.')[-1]):
                logging.info(' Table  %s exists' % sql.split()[2].split('.')[-1])
                continue
            if re.match(pattern_projection, sql) and not re.match(pattern_if_exists, sql)\
                    and vert.has_projection(sql.split()[2].split('.')[-1]):
                logging.info(' Projection  %s exists' % sql.split()[2].split('.')[-1])
                continue

            vert.execute(sql)
    return 0


def create_tables(tables_dir):
    logging.info('  ')
    logging.info('  .... Create Tables ....')
    logging.info('    ')
    for table_file in os.listdir(tables_dir):

        table = table_file.replace('.sql', '')
        if vert.has_table(table) and not withdrop:
            logging.info(' Table %s exists' % table)
            continue

        if vert.has_table(table) and withdrop:
            logging.info(' Table %s exists' % table)
            if table == 'Version':
                continue
            vert.drop_table(table)
            logging.info(' Table %s dropped' % table)
        else:
            logging.info(" Table %s doesn't exist" % table)
# Create the table
        execute_file(tables_dir,table_file)
        logging.info(' Table %s created' % table)
        logging.info('    ')


def create_misc(file_dir):

    for file_name in os.listdir(file_dir):

# Run the file
        logging.info(' ')
        logging.info('  .... File %s ....' % file_name)
        logging.info('    ')
        execute_misc_file(file_dir,file_name)


def get_ksafety():
    if vert.is_cluster():
        return 1
    else:
        return 0


def ksafe(sql, ksafety):
##    Parses a given sql string and replaces all ksafe with given value.

    old_str = "ALL NODES KSAFE %s" % (str(1 - ksafety))
    new_str = "ALL NODES KSAFE %s" % (str(ksafety))
    pattern = re.compile(old_str, re.IGNORECASE)

    sql = pattern.sub(new_str, sql)
# exchange 1 - 0 or 0 - 1
    old_str = "(%s)" % (str(1 - ksafety))
    new_str = "%s" % (str(ksafety))
    pattern = re.compile(old_str, re.IGNORECASE)
    sql = pattern.sub(new_str, sql)

    return sql

def write_version():
    vert.execute("insert into install.Version(Version,VersionAsInt,Description) values('%s','%s','%s')"
                 %(config.version, config.version_as_int, config.description))
def arg_validation():
# example: ./install.py drop=yes

    parser = argparse.ArgumentParser('install.py')
    parser.add_argument('withdrop', default='drop=no', nargs='?', help="Drop Objects if exist: drop=yes or drop=no. Default drop=no ")
    withdropTypes = ['drop=yes', 'drop=no']
    args = parser.parse_args()
    if not withdropTypes.__contains__(args.withdrop):
        print("Not a valid withdrop parameter - it should be one of the following: %s" %withdropTypes)
        logging.error("Not a valid withdrop parameter - it should be one of the following: %s" %withdropTypes )
        sys.exit(-1)

    return args

if __name__ == '__main__':

    # task parameters
    args=arg_validation()
    if(args.withdrop == 'drop=yes'):
        withdrop = True
    else:
        withdrop = False

    log_format = '%(asctime)s %(levelname)-8s %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_format, filename=config.log_file, filemode='a')
    logging.info('Arguments: %s' %args)
    success = True
    vert = Vertica()
    sid = vert.execute('select current_session()')
    logging.info('  .... created new connection [session_id: %s]', sid[1][0][0])
    version,install_time = get_current_version()[1][0]
    vertica_version = vert.execute('select version()')[1][0][0]
    logging.info('  .... Vertica version is %s  ' % vertica_version)
    logging.info('  .... Optimal current version is %s ; installed %s ' % (version,install_time))
    logging.info('    ')
    if comp_versions(version.strip(' '), config.version.strip(' ')):
        schema_dir = config.schema_dir
        dirs = get_dirs_to_process(schema_dir, version.strip(' '), config.version.strip(' '))
        vert.set_default_schema(config.default_schema)
        ksafety = get_ksafety()
        for ver, dir in sorted(dirs.iteritems()):
            logging.info('  .... Processing version %s from %s ' % (ver, dir))
            logging.info('    ')
            files_to_process = []
            files = get_files_to_process(dir)

            for file in files:
                # execute files
                logging.info('  .... Processing file %s  ' % file)
                execute_misc_file(file)
        # if success:
        #     write_version()
    else:
        logging.info('  .... installed version %s is greater than %s ' % ( version,config.version) )
        logging.info('    ')



