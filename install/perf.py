from Vertica import *


if __name__ == '__main__':

    vert = Vertica()
    res = vert.execute('select * from leo.c1')
    print('Row count: %s' % res[2])

