# -*- coding: UTF-8 -*-

import MySQLdb

import settings


def singleton(cls, *args, **kw):
    instances = {}

    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return _singleton


@singleton
class DBUtil():
    db = MySQLdb.connect(settings.DB_HOST, settings.DB_USER, settings.DB_PASSWD, settings.DB_NAME)

    def get_connection(self):
        if self.db.open:
            return self.db
        else:
            self.db = MySQLdb.connect(settings.DB_HOST, settings.DB_USER, settings.DB_PASSWD, settings.DB_NAME)
            return self.db


dbutil = DBUtil()


def get_spark_apps():
    app_names = set()
    cursor = dbutil.get_connection().cursor()

    try:
        cursor.execute('select instance_name from running_alert_sparkmonitorinstance where valid = 1')
        results = cursor.fetchall()
        for row in results:
            app_names.add(row[0])
    except Exception, e:
        print "Error: unable to fecth data: %s " % e
        raise e
    finally:
        cursor.close()
    return app_names
