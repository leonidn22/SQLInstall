from os.path import join as path_join

DSN = "vertica"
version = '1.4'
description = 'The  version'
# only rerun the current version
is_rerun_the_installed_version = True

# default_schema will be created if not exists
default_schema = 'optimal2'
root = '/home/dbadmin/optinstall'
schema_dir = '/home/dbadmin/optinstall/schema/Vertica'

log_file = path_join(root, 'logs/install.log')
log_file = ''

stop_after_first_exception = True



#version_as_int = 11
