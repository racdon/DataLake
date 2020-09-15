import sys
import os
import subprocess
import mysqlconnection
import hiveTable
import logging

logfile = "DataLoader_log_file.txt"
logging.basicConfig(filename=logfile, filemode='a', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

def execute_unix_command(command):
    logging.info("Starting to run unix command "+command)
    return os.system(command)

if __name__ == '__main__':
    logging.info("-------------------- Execution Process Started -----------------------------")
    host='localhost'
    username='training'
    password='training'
    maxInstance='1'
    dbName='training'
    tablename='metdata_dt_tbl'

    #---------------------- Calling Mysql to fetch row for which execution will take place ---------------------
    logging.info("Calling mysqlconnection.py")
    row_info=mysqlconnection.info(host,username,password,maxInstance,dbName,tablename)
    logging.info("Information retrieve Successfully....")
    #---------------------- End Of Calling Mysql to fetch row for which execution will take place ---------------------
    
    if(row_info != None):
        #---------------------- Calling Mysql for updating run_sts from 'SUBMITTED' to 'RUNNING' ---------------------
        logging.info("Calling mysqlconnection.py for updating run_sts from 'SUBMITTED' to 'RUNNING'")
        mysqlconnection.update_run_status(host,username,password,dbName,tablename,row_info[0],'RUNNING')
        logging.info("run_sts Updated To RUNNING successfully.")
        #---------------------- End Of Calling Mysql for updating run_sts from 'SUBMITTED' to 'RUNNING' ---------------------

        #---------------------- Calling Mysql to fetch Schema information of the table which is going to be sqooped ----------- 
        logging.info("Calling mysqlconnection.py Again For Retriving structure Of The table")
        tbl_info=mysqlconnection.describe(host,username,password,row_info[7],row_info[8])
        logging.info("Information required for table creation retrieved Successfully....")
        #-------------- End Of Calling Mysql to fetch Schema information of the table which is going to be sqooped -----------

        #---------------------- Hive Table Cretion OR its Existence Checking -----------
        hive_ddl_exists=0
        hive_dll=row_info[10]
        if(hive_dll=='Y'):
            logging.info("Hive DDl Needs to be created since the flag is Y.")
            #print(hive_dll)
            hive_tbl_crt=hiveTable.col_attr_syntax(row_info[7],row_info[8],tbl_info)
            logging.info("Hive DDl Creation took Place Successfully.Change status of hive_dll to N in metdata_dt_table")
            hive_ddl_exists=1
        else:
            hive_ddl_exists=1
            logging.info("Hive DDl Already Exists. Change status of hive_dll to N in metdata_dt_table")
        #---------------------- End Of Hive Table Cretion OR its Existence Checking -----------

        update_hive_ddl_status=mysqlconnection.update_status(host,username,password,dbName,tablename,row_info[0])
        logging.info("Status of hive_dll is updated to N in metdata_dt_table")

        #---------------------- Data Sqooping Process Starts ----------------------------------
        data_sqooped=0
        if(hive_ddl_exists==1):
            logging.info("Starting To Prepare Sqoop Command: ")
            logging.info("Started checking Destination Path Existance: ")
            command_check_destination_file_exists = 'hadoop fs -ls '+row_info[11]+'/'+row_info[8]
            #print(command_check_destination_file_exists)
            command_remove_destination_file = 'hadoop fs -rmr '+row_info[11]+'/'+row_info[8]
            #command_remove_java_file = 'rm /home/training/python_script/'+x[:-1:]+'.java'
            if execute_unix_command(command_check_destination_file_exists) == 0:
                #print("Thanks For Reaching Here")
                logging.info("Destination File Exists as "+command_check_destination_file_exists)
                logging.info("Deleting Destination Files")
                execute_unix_command(command_remove_destination_file)
                #--output-dir = "directory path"
                #logging.info("Deleting Java Files")
                #execute_unix_command(command_remove_java_file)
                logging.info("Sqoop Command Preparation Started")
                sqoop_command = "sqoop import --connect "+row_info[2]+row_info[3]+"/"+row_info[7]+" --username="+row_info[5]+" --password="+row_info[6]+" --table="+row_info[8]+" --as-textfile -m 1 --target-dir="+row_info[11]+'/'+row_info[8]
                logging.info("Sqoop Command Preparation completed")
                #print(sqoop_command)
                if execute_unix_command(sqoop_command) == 0:
                    logging.info("Sqoop Command Excecution completed SuccessFully For table: "+row_info[8])
                    data_sqooped=1
                else:
                    logging.warning("Sqoop Command Excecution Failed For table: "+row_info[8])
                    data_sqooped=0
            else:
                #print("Thanks For Reaching THere")
                create_path = 'hadoop fs -mkdir '+row_info[11]
                #print(create_path);
                execute_unix_command(create_path)
                logging.info("Sqoop Command Preparation Started")
                sqoop_command = "sqoop import --connect "+row_info[2]+row_info[3]+"/"+row_info[7]+" --username="+row_info[5]+" --password="+row_info[6]+" --table="+row_info[8]+" --as-textfile -m 1 --target-dir="+row_info[11]+'/'+row_info[8]
                logging.info("Sqoop Command Preparation completed")
                #print(sqoop_command)
                if execute_unix_command(sqoop_command) == 0:
                    logging.info("Sqoop Command Excecution completed SuccessFully For table: "+row_info[8])
                    data_sqooped=1
                else:
                    logging.warning("Sqoop Command Excecution Failed For table: "+row_info[8])
                    data_sqooped=0
        else:
            logging.info("Oops Something Went wrong. Please check hive table whether it's present or not.")
            data_sqooped=0
        #---------------------- End Of Data Sqooping Process Starts ----------------------------------


        #---------------------- Transfer Of Data from hdfs location to hive table ----------------------------------
        if(data_sqooped==1):
            logging.info("Starting To prepare Hive Load Command.")
            hive_dt_load=hiveTable.load_data(row_info[7],row_info[8],row_info[11])
            logging.info("Data Loaded Successfully To Hive Table.")
        else:
            logging.info("Oops Something Went wrong. Please check whether data present at hdfs location or not.")
        #---------------------- Transfer Of Data from hdfs location to hive table -----------------------------------

        #---------------------- Calling Mysql to fetch row for which execution will take place ---------------------
        logging.info("Calling mysqlconnection.py for updating run_sts from 'RUNNING' to 'COMPLETED'")
        row_info=mysqlconnection.update_run_status(host,username,password,dbName,tablename,row_info[0],'COMPLETED')
        logging.info("run_sts Updated To COMPLETED successfully.")
        #---------------------- End Of Calling Mysql to fetch row for which execution will take place ---------------------
    else:
        logging.info("We donnot have any row with run_sts as SUBMITTED.")
        print('We donot have any row with run_sts as SUBMITTED')

    logging.info("---------------------------------- Execution Process Ended -----------------------------")
