#!/usr/bin/env python3

import sqlite3
import psycopg2
import configparser

config = configparser.ConfigParser()
config.read("config.ini")
psql_config = config['psql_database']
sqlite = config['sqlite_database']

sqlite_db = sqlite3.connect(sqlite['location'])
sqlite_cur = sqlite_db.cursor()

psql_db = psycopg2.connect("dbname={} user={} host={} password={}".format(psql_config['dbname'],psql_config['user'],psql_config['host'],psql_config['password']))
psql_cur = psql_db.cursor()

id = 0

while True:
    data = sqlite_cur.execute("SELECT id,name FROM author WHERE id > ? LIMIT 10000",(id,)).fetchall()
    if not data: break
    id = data[-1][0]
    print(id)
    sql = "INSERT INTO author VALUES {} ON CONFLICT DO NOTHING"
    args_str = ','.join(['%s'] * len(data))
    sql = sql.format(args_str)
    psql_cur.execute(sql,data)
    psql_db.commit()
