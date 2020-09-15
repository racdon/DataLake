import sys
import os
import subprocess
import logging

logfile = "DataLoader_log_file.txt"
logging.basicConfig(filename=logfile, filemode='a', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

def execute_unix_command(command):
    logging.info("Starting to run unix command "+command)
    return os.system(command)

def col_attr_syntax(dbName,tblname,tbl_info):
    flag=0
    logging.info("Checking for the existence Of Database.")
    db_list='hive --hiveconf dbName='+dbName+' -f db_exists.hql'
    if execute_unix_command(db_list) == 0:
        logging.info("Database Exists.")
        flag=1
    else:
        logging.info("Database Does Not Exists.")
        logging.info("Database Creation Started.")
        database_creation='hive --hiveconf dbName='+dbName+' -f db_creation.hql'
        if execute_unix_command(database_creation) == 0:
            logging.info("DB Created Successfully.....................")
            flag=1
        else:
            logging.info("DB Creation Failed.....................")
    if flag==1:
        tbl_exists='hive --hiveconf dbName='+dbName+' --hiveconf tblname='+tblname+' -f tbl_exists.hql'
        if execute_unix_command(tbl_exists) == 0:
            logging.info("Table Already Present.")
            drop_tbl='hive --hiveconf dbName='+dbName+' --hiveconf tblname='+tblname+' -f drop_tbl.hql'
            logging.info("Table Droped Successfully.")
            #return 0
        else:
            logging.info("Table Does Not Exists.")
            logging.info("Starting Table Creation Process")
        
        #------------------- Reading mysql configuration file and making it to key Value Pair ------------
        dict={}
        datatype_conversion_det=open("mysqlHive.Conf","r")
        for dt in datatype_conversion_det:
            mysql_dt,hive_dt=dt.split(' ')
            hive_dt=hive_dt[:-1:]
            dict[mysql_dt]=hive_dt
        #------------------- End Of Reading mysql configuration file and making it to key Value Pair ------------


        create_table_column_attr=''
        col_name=''
        old_col_dt=''
        new_col_dt=''
        for x in range(len(tbl_info)):
            col_name=tbl_info[x][0]
            old_col_dt=tbl_info[x][1]
            for m,n in dict.items():
                if(old_col_dt==m):
                    new_col_dt=n
                    break
            
            create_table_column_attr = create_table_column_attr+col_name+' '+new_col_dt+','
        #print(create_table_column_attr[:-1:])
        creation_type='EXTERNAL'
        hive_hql_query = 'hive --hiveconf database='+dbName+' --hiveconf table='+tblname+' --hiveconf col_attr_syntax="'+create_table_column_attr[:-1:]+'" --hiveconf creation_type='+creation_type+' -f create_table.hql'
        #print(hive_hql_query)
        if execute_unix_command(hive_hql_query) == 0:
            logging.info("Table Created Successfully.")
        else:
            logging.info("Table Creation Failed.")
    else:
        logging.info("Oops Some Unexceptional Eroro Occured!!!!!!!!!!")
        return 0



def load_data(dbName,tblname,hdfs_loc):
    hive_load_cmd='hive --hiveconf database='+dbName+' --hiveconf table='+tblname+' --hiveconf hdfs_loc='+hdfs_loc+'/'+tblname+'/part-m-00000 -f load_data_hdfs.hql'
    #print(hive_load_cmd)
    if execute_unix_command(hive_load_cmd) == 0:
        logging.info("Data Loaded To Hive Table Successfully.")
    else:
        logging.info("Data Loading To Hive Table Failed.")
