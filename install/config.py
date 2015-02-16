from os.path import join as path_join

version = '1.4'
description = 'The  version'
is_rerun_the_installed_version = True
version_as_int = 11

default_schema = 'public'
root = '/home/dbadmin/optinstall'

DSN = "vertica"
log_file = path_join(root, 'logs/install.log')
log_file = ''

schema_dir = '/home/dbadmin/optinstall/schema/Vertica'
tables_dir = path_join(root, 'schema/Vertica/Tables')
misc_dir = path_join(root, 'schema/Vertica/Misc')



version_sql = """
            insert into install.Version
            """