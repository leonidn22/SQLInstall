#!/opt/vertica/oss/python/bin/python

import argparse
import re
from os.path import join as path_join
import fnmatch
from stat import *

from Vertica import *


success = True

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
    if not vert.has_table('Version', 'install'):
        vert.create_schema('install')
        vert.execute('''
                    CREATE TABLE install.Version
                    (
                        Version varchar(255) NOT NULL,
                        InstallTime timestamp default current_timestamp NOT NULL,
                        Description varchar(1024) NOT NULL)
                     ''')
        write_version('0',  '')
        vert.commit()
    sql = 'select Version , InstallTime from install.Version order by InstallTime desc limit 1'
    res = vert.execute(sql)
    if len(res[1]) == 0:
        write_version('0',  '')
        res =vert.execute(sql)
    return res


def get_dirs_to_process(schema_dir, current_version, target_version):

    if target_version != '999.999.9999.999' and not os.path.exists(path_join(schema_dir, 'v' + target_version)):
        logging.info('  .... No target version directory exists %s ' % path_join(schema_dir, target_version))
        logging.info('    ')
        global success
        success = False
        return {}
    all_files = os.listdir(schema_dir)
    filtered = fnmatch.filter(all_files, 'v*')
    if not filtered:
        logging.info('DONE: no version to install')
        vert.close()
        sys.exit(0)
    dir_to_process = {}
    for item in sorted(filtered):
        item_ver = item.replace('v', '').strip(' ')
#
        if config.is_rerun_the_installed_version and current_version == target_version == item_ver:
            dir_to_process[item_ver] = path_join(schema_dir, item)
            break
        if comp_versions(item_ver, current_version):
            #print('%s ; %s ' % (item_ver, current_version.strip(' ')))
            continue

        if comp_versions(item_ver, target_version):
            dir_to_process[item_ver] = path_join(schema_dir, item)
    return dir_to_process


def get_files_to_process(dir,ext):

    for f in os.listdir(dir):
        pathname = os.path.join(dir, f)
        mode = os.stat(pathname)[ST_MODE]
        if S_ISDIR(mode):
            # It's a directory, recurse into it
            get_files_to_process(pathname,ext)
        elif S_ISREG(mode):
            # It's a file, call the callback function
            if os.path.splitext(pathname)[1].replace('.','') == ext:
                files_to_process.append(pathname)
        else:
            # Unknown file type
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

def is_alter_table_add_column(sql):


    r_dot = re.compile('alter table (?P<sname>[a-zA-Z0-9_]+)\.(?P<tname>[a-zA-Z0-9_]+) '
                       'ADD COLUMN (?P<cname>[a-zA-Z0-9_]+) .+', re.IGNORECASE)

    r_no_dot = re.compile('alter table (?P<tname>[a-zA-Z0-9_]+) '
                          'ADD COLUMN (?P<cname>[a-zA-Z0-9_]+) .+', re.IGNORECASE)
    match_dot = r_dot.search(sql)
    match_no_dot = r_no_dot.search(sql)
    if match_dot:
        sname = match_dot.group('sname')
        tname = match_dot.group('tname')
        cname = match_dot.group('cname')
    elif match_no_dot:
        sname = config.default_schema
        tname = match_no_dot.group('tname')
        cname = match_no_dot.group('cname')
    else:
        return False, '', '', ''

    return True, sname, tname, cname


def is_alter_table_drop_column(sql):


    r_dot = re.compile('alter table (?P<sname>[a-zA-Z0-9_]+)\.(?P<tname>[a-zA-Z0-9_]+) '
                       'DROP COLUMN (?P<cname>[a-zA-Z0-9_]+).*', re.IGNORECASE)

    r_no_dot = re.compile('alter table (?P<tname>[a-zA-Z0-9_]+) '
                          'DROP COLUMN (?P<cname>[a-zA-Z0-9_]+).*', re.IGNORECASE)
    match_dot = r_dot.search(sql)
    match_no_dot = r_no_dot.search(sql)
    if match_dot:
        sname = match_dot.group('sname')
        tname = match_dot.group('tname')
        cname = match_dot.group('cname')
    elif match_no_dot:
        sname = config.default_schema
        tname = match_no_dot.group('tname')
        cname = match_no_dot.group('cname')
    else:
        return False, '', '', ''

    return True, sname, tname, cname


def is_alter_table_rename_column(sql):

    #replace multiply blank to one
    sql = ' '.join(sql.split())

# RENAME [ COLUMN ] column TO new-column
    r_dot = re.compile('alter table (?P<sname>[a-zA-Z0-9_]+)\.(?P<tname>[a-zA-Z0-9_]+) '
                       'RENAME (COLUMN|[ ]*)(?P<old_cname>[a-zA-Z0-9_]+) TO (?P<new_cname>[a-zA-Z0-9_]+)', re.IGNORECASE)

    r_no_dot = re.compile('alter table (?P<tname>[a-zA-Z0-9_]+) '
                       'RENAME (COLUMN|[ ]*)(?P<old_cname>[a-zA-Z0-9_]+) TO (?P<new_cname>[a-zA-Z0-9_]+).*', re.IGNORECASE)
    match_dot = r_dot.search(sql)
    match_no_dot = r_no_dot.search(sql)
    if match_dot:
        sname = match_dot.group('sname')
        tname = match_dot.group('tname')
        old_cname = match_dot.group('old_cname')
        new_cname = match_dot.group('old_cname')
    elif match_no_dot:
        sname = config.default_schema
        tname = match_no_dot.group('tname')
        old_cname = match_dot.group('old_cname')
        new_cname = match_dot.group('old_cname')
    else:
        return False, '', '', '', ''

    return True, sname, tname, old_cname, new_cname


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
            # replace multiply blank to one
            #sql = ' '.join(sql.split(' '))
            sql = sql.strip(' \t\n\r')
            sql = sql.replace('\r', ' ')


            sql_command = ' '
            for sql_c in sql.split('\n'):
                if sql_c.startswith('--') or sql.strip(' ') == '':
                    continue
                sql_command = sql_command + sql_c + ' '
            if sql_command.strip(' ') == '':
                continue
            sql = sql_command
            #print 'SQL IS: %s' %sql
            if re.match(pattern_user, sql) and vert.has_user(sql.split()[2]):
                logging.info(' User %s exists' % sql.split()[2])
                continue
            if re.match(pattern_pool, sql) and vert.has_pool(sql.split()[3]):
                logging.info(' Resource Pool %s exists' % sql.split()[3])
                continue
            if re.match(pattern_table, sql) and not re.match(pattern_if_exists, sql)\
                    and vert.has_table(sql.split()[2].split('.')[-1], sql.split()[2].split('.')[0]):
                logging.info(' Table  %s exists' % sql.split()[2].split('.')[-1])
                continue
            if re.match(pattern_projection, sql) and not re.match(pattern_if_exists, sql)\
                    and vert.has_projection(sql.split()[2].split('.')[-1], sql.split()[2].split('.')[0]):
                logging.info(' Projection  %s exists' % sql.split()[2].split('.')[-1])
                continue

            is_alter, sname, tname, cname = is_alter_table_add_column(sql)
            if is_alter:
                logging.info(' Column %s in table %s exists' % (cname,tname))
                continue

            is_alter, sname, tname, cname = is_alter_table_drop_column(sql)
            if is_alter:
                logging.info(' Column %s in table %s has already dropped' % (cname,tname))
                continue
            is_alter, sname, tname, old_name, new_cname = is_alter_table_rename_column(sql)
            if is_alter:
                logging.info(' Column %s in table %s has already renamed to %s' % (old_name, tname, new_cname))
                continue

            res = vert.execute(sql)
            if res[0] == -1:
                global success
                success = False
                if config.stop_after_first_exception:
                    logging.info(' ')
                    logging.error(' ----> Exit after first exception <-------')
                    sys.exit(1)


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
        execute_misc_file(file_dir, file_name)


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

def write_version(version, description):
    vert.execute("insert into install.Version(Version,Description) values('%s','%s')"
                 % (version, description))


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



#ToDo  The folders will be as in TFS


    # task parameters
    # args=arg_validation()
    # if(args.withdrop == 'drop=yes'):
    #     withdrop = True
    # else:
    withdrop = False
    root = os.path.dirname(os.path.realpath(sys.argv[0]))
    default_log_dir = os.path.join(root, 'logs/')
    log_dir = os.getenv('OT_LOGDIR', default_log_dir)
    log_name = 'VerticaSchemaInstall.log'
    log_file = os.path.join(log_dir, log_name)
    if not os.path.exists(os.path.dirname(log_file)):
        os.makedirs(os.path.dirname(log_file))
    log_format = '%(asctime)s %(levelname)-8s %(message)s'
    #log_file = ''
    logging.basicConfig(level=logging.DEBUG, format=log_format, filename=log_file, filemode='a')


    log_to_stder = logging.getLogger()
    h_err = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - ERROR - %(levelname)s - %(message)s')

    h_err.setFormatter(formatter)
    log_to_stder.addHandler(h_err)
    h_err.setLevel(logging.ERROR)

    # log_screen = logging.getLogger('Screen')
    # h_screen = logging.StreamHandler(sys.stdout)
    # log_screen.addHandler(h_screen)
    # log_screen.setLevel(logging.DEBUG)
    log_screen = logging.getLogger()
    log_screen.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - DEBUG - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    log_screen.addHandler(ch)



    # logging.info('Arguments: %s' %args)
    vert = Vertica()
    sid = vert.execute('select current_session()')
    logging.info('  .... ')
    logging.info('  .... Starting Vertica schema installation')
    logging.info('  .... created new connection [session_id: %s]', sid[1][0][0])
    version,install_time = get_current_version()[1][0]
    vertica_version = vert.execute('select version()')[1][0][0]
    logging.info('  .... Vertica version is %s  ' % vertica_version)
    logging.info('  .... Optimal current version is %s ; installed %s ' % (version,install_time))
    logging.info('    ')
    target_version = config.version.strip(' ')

    if comp_versions(version.strip(' '), target_version):
        schema_dir = config.schema_dir
        dirs = get_dirs_to_process(schema_dir, version.strip(' '), target_version)
        if dirs == {}:
            logging.info('The last version %s has already installed ' % version)
            logging.info('  .... exiting  ')
            sys.exit(0)

        vert.set_default_schema(config.default_schema)
        ksafety = get_ksafety()
    ### Processing all versions
        for ver, ver_dir in sorted(dirs.iteritems()):
            logging.info('  .... Processing version %s from %s ' % (ver, ver_dir))
            logging.info('    ')
            files_to_process = []
            files = get_files_to_process(ver_dir, 'sql')
    ### Processing all files in version ver
            for file in sorted(files):
                # execute files
                logging.info('  ........ Processing file %s  ' % file)
                logging.info(' ')
                execute_misc_file(file)
            if success:
                write_version(ver,  '')
    if not success:
        logging.error('  .... exiting  ')
        sys.exit(1)
    else:
        logging.info('  .... installed version %s is greater than %s ' % (version, target_version))
        logging.info('  .... exiting  ')



