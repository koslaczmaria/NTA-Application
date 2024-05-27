import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import ipaddress
import os
import pandas as pd
import asyncpg

def get_password():
    password = ''
    if 'POSTGRES_PASSWORD_FILE' in os.environ:
        with open(os.environ['POSTGRES_PASSWORD_FILE'], 'r') as f:
            password = f.read().strip()
    else:
        password = os.environ['POSTGRES_PASSWORD']
    return password

def check_if_db_exists():
    password = get_password()
    try:
        conn = psycopg2.connect(database="ipdb", host="dblocations", user="postgres", password=password)
    except:
        return False
    return True

def check_if_table_exists():
    password = get_password()
    conn = psycopg2.connect(database="ipdb", host="dblocations", user="postgres", password=password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS ipv4addresses;')
    cursor.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname='public' AND tablename = 'ipv4addresses');")
    result = cursor.fetchone()[0]
    return result

def create_table():
    password=get_password()
    conn = psycopg2.connect(database="ipdb", host="dblocations", user="postgres", password=password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    sqlCreateTable = '''CREATE TABLE ipv4addresses (
        id SERIAL PRIMARY KEY,
        ip_from INET NOT NULL,
        ip_from_num BIGINT NOT NULL,
        ip_to INET NOT NULL,
        ip_to_num BIGINT NOT NULL,
        country_code VARCHAR(3) NOT NULL,
        country_name VARCHAR(60) NOT NULL,
        region_name VARCHAR(60) NOT NULL,
        city_name VARCHAR(60) NOT NULL,
        latitude FLOAT NOT NULL,
        longitude FLOAT NOT NULL)'''
    cursor.execute(sqlCreateTable)
    cursor.close()
    conn.close()

async def import_locations(filename):
    password=get_password()
    conn = await asyncpg.connect(database="ipdb", host="dblocations", user="postgres", password=password)
    chunksize = 100
    column_names = ['ip_from_num','ip_to_num','country_code','country_name','region_name','city_name','latitude','longitude']
    with pd.read_csv(filename, chunksize=chunksize, names=column_names, dtype={'ip_from_num':'int64','ip_to_num':'int64'}) as reader:
        for chunk in reader:
            chunk.insert(loc=0, column='ip_from', value='')
            chunk['ip_from'] = chunk['ip_from_num'].apply(long2DotIP)
            chunk.insert(loc=2, column='ip_to', value='')
            chunk['ip_to'] = chunk['ip_to_num'].apply(long2DotIP)
            chunk['country_name'] = chunk['country_name'].apply(replace_char)
            chunk['region_name'] = chunk['region_name'].apply(replace_char)
            chunk['city_name'] = chunk['city_name'].apply(replace_char)
            sqlImportDatabase = ''
            for idx,row in chunk.iterrows():
                sqlImportDatabase += "INSERT INTO ipv4addresses (ip_from,ip_from_num,ip_to,ip_to_num,country_code,country_name,region_name,city_name,latitude,longitude) VALUES ("
                sqlImportDatabase += "'" + str(row['ip_from']) + "',"
                sqlImportDatabase += "'" + str(row['ip_from_num']) + "',"
                sqlImportDatabase += "'" + str(row['ip_to']) + "',"
                sqlImportDatabase += "'" + str(row['ip_to_num']) + "',"
                sqlImportDatabase += "'" + str(row['country_code']) + "',"
                sqlImportDatabase += "'" + str(row['country_name']) + "',"
                sqlImportDatabase += "'" + str(row['region_name']) + "',"
                sqlImportDatabase += "'" + str(row['city_name']) + "',"
                sqlImportDatabase += "'" + str(row['latitude']) + "',"
                sqlImportDatabase += "'" + str(row['longitude']) + "');"
            await conn.execute(sqlImportDatabase)

    sqlCreateIndex1 = "CREATE INDEX idx_ipfromnum ON ipv4addresses(ip_from_num);"
    sqlCreateIndex2 = "CREATE INDEX idx_iptonum ON ipv4addresses(ip_to_num);"
    conn.execute(sqlCreateIndex1)
    conn.execute(sqlCreateIndex2)
    conn.close()

def searchIP(ipAddr):
    password=get_password()
    conn = psycopg2.connect(database="ipdb", host="dblocations", user="postgres", password=password)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    ipNum=int(ipaddress.IPv4Address(ipAddr))
    sqlSearchIP = f"SELECT * FROM ipv4addresses WHERE ip_from_num<={ipNum} AND ip_to_num>={ipNum};"
    cursor.execute(sqlSearchIP)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result

def long2DotIP(ipnum):
    dotIP = str(int(ipnum / 16777216) % 256) + "."
    dotIP += str(int(ipnum / 65536) % 256) + "."
    dotIP += str(int(ipnum / 256) % 256) + "."
    dotIP += str(ipnum % 256)
    return dotIP

def replace_char(name):
    return name.replace("'","''")