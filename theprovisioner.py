#!/usr/bin/python3.6

import mysql.connector
from random import randint
from datetime import datetime, timedelta
import sys, getopt, operator, threading, string, time, re, random, argparse, signal

ARGS=None
STOP=False
STRGENTIME=0
NOINSERT=["auto_increment", "VIRTUAL GENERATED", "STORED GENERATED"]


def main(argv):
    signal.signal(signal.SIGINT, signal_handler)
    parser = argparse.ArgumentParser(description='theprovisioner, fill tables with random data')
    parser.add_argument('--host', default="127.0.0.1", help='Host (default: 127.0.0.1)')
    parser.add_argument('-P', '--port', type=int, default=3306, help='Port (default: 3306)')
    parser.add_argument('-u', '--user', required=True, help='User')
    parser.add_argument('-p', '--password', required=True, help='Password')
    parser.add_argument('-d', '--database', required=True, help='Default schema')
    parser.add_argument('-t', '--table', required=True, help='Table to be inserted')
    parser.add_argument('-r', '--rows', type=int, required=True, help='Rows to insert')
    parser.add_argument('-c', '--concurrency', type=int, required=True, help='Concurrency')
    args = parser.parse_args()
    global ARGS
    ARGS=args

    truncateTable()
    getStatement()


def getConn():
    try:
        conn = mysql.connector.connect( raw=False, host=ARGS.host, port=ARGS.port, database=ARGS.database, user=ARGS.user, password=ARGS.password, auth_plugin='caching_sha2_password')
        conn.set_charset_collation('utf8mb4', 'utf8mb4_0900_ai_ci')
    except mysql.connector.Error as error :
        print(error)
        sys.exit()
    return conn


def truncateTable():
    conn = getConn()
    cursor = conn.cursor()
    try:
        cursor.execute("TRUNCATE "+ARGS.table+";")
        conn.commit()
    except mysql.connector.Error as error :
        print(error)
        sys.exit()


def getStatement():
    try:
        conn = getConn()
        cursor = conn.cursor()
        cursor.execute("SHOW FULL COLUMNS FROM "+ARGS.table+";")
        records=cursor.fetchall()
        cols = len(records)
        stmt = "INSERT INTO "+ARGS.table+"("
        finalSet = []
        skipCols=0
        for row in records:
            #res = list(map(type, row)) 
            #print("The data types of tuple in order are : " + str(res))
            if row[6] in NOINSERT:
                skipCols=skipCols+1
                continue
            else:
                stmt = stmt + row[0] + ","
                finalSet.append(row)
        nfields=cols - skipCols
        stmt = stmt[:-1]+") VALUES("+ ("%s," * (nfields))
        stmt = stmt[:-1] + ");"

    except mysql.connector.Error as error :
        print("Unable to run SHOW COLUMNS:")
        print(error)
        sys.exit()

    print(stmt)
    cursor.close()
    conn.close()

    threads = []
    for x in range(0, ARGS.concurrency):
        print("Started thread #" + str(x))
        processThread = threading.Thread(target=spanConn, args=[stmt, finalSet, str(x)])
        threads.append(processThread)
        processThread.start()

    statsThread = threading.Thread(target=stats) 
    statsThread.daemon = True
    statsThread.start()

    start = time.time()
    for x in threads:
        x.join()

    end = time.time()
    print("Duration: " + str(format(round(end-start, 2))))
    sys.exit()


def spanConn(theStmt, theRecords, theThreadId):
    conn = getConn()
    conn.autocommit = True
    cursor = conn.cursor(prepared=True)
    for x in range(0, round(ARGS.rows/ARGS.concurrency)):
        if STOP:
            sys.exit(0)
        insert_list = []
        for row in theRecords:
            #datatype=row[1].decode("utf-8")
            datatype=row[1] 
            if "char" in datatype:
                insert_list.append(randomString(int(re.sub("[^0-9]", "", datatype))))
            elif datatype.startswith("tinyint"):
                insert_list.append(randint(0, 127))
            elif datatype.startswith("smallint"):
                insert_list.append(randint(0, 32767))
            elif datatype.startswith("mediumint"):
                insert_list.append(randint(0, 8388607))
            elif datatype.startswith("int"):
                insert_list.append(randint(0, 2147483647))
            elif datatype.startswith("bit"):
                insert_list.append(randint(0, 1))				
            elif datatype.startswith("bigint"):
                insert_list.append(randint(0, 9223372036854775807))
            elif datatype.startswith("double"):
                insert_list.append(randint(0, 2147483647))
            elif datatype.startswith("float"):
                insert_list.append(randint(0, 999))
            elif  "datetime" in datatype:
                insert_list.append(gen_datetime())
            elif  "date" in datatype:
                insert_list.append(gen_datetime(1970, 2037).strftime('%Y-%m-%d'))
            elif  "timestamp" in datatype:
                insert_list.append(gen_datetime(1970, 2037).strftime('%Y-%m-%d %H:%M:%S'))
            elif  "blob" in datatype:
                insert_list.append(randomString(65535))
            elif  "text" in datatype:
                insert_list.append(randomString(65535))
            elif  "json" in datatype:
                insert_list.append('{"key1": "value1", "key2": "value2"}')	
            elif datatype.startswith("enum"):
                insert_list.append('0')				
        
        try:
            cursor.execute(theStmt, tuple(insert_list))
        except mysql.connector.Error as error :
            print(error)
        #conn.commit()


def randomString(stringLength=8):
    start = time.time()
    letters = string.ascii_lowercase
    rnd= ''.join(random.choice(letters) for i in range(stringLength))
    #rnd = "A" * stringLength
    end = time.time()
    global STRGENTIME
    STRGENTIME = STRGENTIME + (end - start)
    return rnd  

def gen_datetime(min_year=1900, max_year=datetime.now().year):
    # generate a datetime in format yyyy-mm-dd hh:mm:ss.000000
    start = datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + timedelta(days=365 * years)
    return start + (end - start) * random.random()


def stats():
    status = getStatus()
    com_insert = status['Com_insert']
    Innodb_data_fsyncs = status['Innodb_data_fsyncs']
    Innodb_os_log_fsyncs =status['Innodb_os_log_fsyncs']
    header = ["insert/s", "data_fsyncs/s", "data_pending_fsyncs", "log_fsyncs/s", "log_pending_fsyncs"]
    print("{: >20} {: >20} {: >20} {: >20} {: >20}".format(*header))

    while True:
        time.sleep(1)
        status = getStatus()
        row=[   int(status["Com_insert"])-int(com_insert), 
                int(status["Innodb_data_fsyncs"]) - int(Innodb_data_fsyncs), 
                int(status["Innodb_data_pending_fsyncs"]), 
                int(status["Innodb_os_log_fsyncs"]) - int(Innodb_os_log_fsyncs), 
                int(status["Innodb_os_log_pending_fsyncs"])]

        print("{: >20} {: >20} {: >20} {: >20} {: >20}".format(*row))
        com_insert = status["Com_insert"]
        Innodb_data_fsyncs = status["Innodb_data_fsyncs"]
        Innodb_os_log_fsyncs = status["Innodb_os_log_fsyncs"]
#Innodb_pages_read Innodb_pages_written Innodb_pages_created Innodb_row_lock_time Innodb_os_log_written Innodb_data_written Innodb_buffer_pool_write_requests 

def getStatus():
    conn = getConn()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("show global status;")
    records = cursor.fetchall()
    return dict(map(operator.itemgetter('Variable_name','Value'),records))


def signal_handler(sig, frame):
    print("theprovisioner stopped")
    global STOP
    STOP=True    


if __name__ == "__main__":
   main(sys.argv[1:])

