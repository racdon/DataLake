import MySQLdb
import sys
import logging

logfile = "DataLoader_log_file.txt"
logging.basicConfig(filename=logfile, filemode='a', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

def info(host,user,password,maxrows,dbname,tblnm):
    logging.info("Establishing Database Connection For Fetching rows information")
    conn=MySQLdb.connect(host=host,user=user,passwd=password)
    cursor=conn.cursor()
    sql_info="select * from "+dbname+"."+tblnm+" WHERE run_sts='SUBMITTED' limit "+maxrows
    cursor.execute(sql_info)
    for data in cursor:
        return(data)
    cursor.close()
    conn.close()

def update_run_status(host,user,password,dbname,tblnm,rowid,status):
    conn=MySQLdb.connect(host=host,user=user,passwd=password);
    cursor=conn.cursor()
    update_run_sts="UPDATE "+dbname+"."+tblnm+" SET run_sts = '"+status+"' WHERE task_no="+rowid
    #print(update_run_sts)
    cursor.execute(update_run_sts)
    for line in cursor:
        print(line)
    cursor.close()
    conn.close()

def describe(host,user,password,dbname,tblnm):
    conn=MySQLdb.connect(host=host,user=user,passwd=password);
    cursor=conn.cursor()
    tbl_struct="select COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH from INFORMATION_SCHEMA.COLUMNS where table_schema='"+dbname+"' AND table_name='"+tblnm+"'"
    cursor.execute(tbl_struct)
    col_list=[]
    for data in cursor:
        col_list.append(data)
    return(col_list)
    cursor.close()
    conn.close()

def update_status(host,user,password,dbname,tblnm,rowid):
    conn=MySQLdb.connect(host=host,user=user,passwd=password);
    cursor=conn.cursor()
    update_sts="UPDATE "+dbname+"."+tblnm+" SET hive_ddl = 'N' WHERE task_no="+rowid
    #print(update_sts)
    cursor.execute(update_sts)
    for line in cursor:
        print(line)
    cursor.close()
    conn.close()
