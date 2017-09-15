import os

YARN = 'http://10.2.19.83:8088/ws/v1/cluster/apps'
APP_PATTERN = '(.*)_monitor_online'

DB_HOST = os.environ.get('DB_HOST')
DB_PASSWD = os.environ.get('DB_PASSWD')
DB_USER = os.environ.get('DB_USER')
DB_NAME = os.environ.get('DB_NAME')