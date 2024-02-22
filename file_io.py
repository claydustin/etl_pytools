import os 
import sys
import logging
from pyspark.sql import SparkSession
from .utils import *
import time


class HQLFile():

    def __init__(self, file_name):

        self.src_path = "/".join(sys.path[0].split("/")[0:-1])
        self.file_name, self.ext = os.path.splitext(file_name)
        self.ext = ".hql" if len(self.ext)==0 else self.ext

        if 'SCRIPT_VERSION' in os.environ:
            self.script_version = os.environ['SCRIPT_VERSION']
            self.emr_file_path = os.path.join(self.src_path, 'hql', self.file_name) + "-" + self.script_version + self.ext

        else:
            self.emr_file_path = os.path.join(self.src_path, 'hql', self.file_name) + self.ext
            self.script_version = ''

    def read(self):
        return open(self.emr_file_path).read()


class HQLQuery():
    """
    Class to manage the File IO and Spark running of HQL files housed on HDFS. The class constructor hides all the 
        messy string manipulation that gets us the formatted EMR file path. 

    Attributes
    ----------
    file_name: str
        Name of file in the /hql directory. If in a subdirectory specify the subdirectory as well. 
        Ex. file_name = "mvno_churn_bcd" OR "mvno_churn_bcd.hql"
            file_name = "monthly/mvno_churn_bcd" OR "monthly/mvno_churn_bcd.hql"

    is_temp: boolean
        Indicates the location of the saved table, either externally in S3 (False) or 
            on HDFS in cluster (True)
    
    Methods 
    -------
    read():
        Reads the contents of the HQL file. No formatting just reads

    run(run_settings):
        Formats the HQL query using run_settings and executes the HQL Query in Spark
    
    """
    def __init__(self, query, is_temp=False, table_name=None):
        if table_name:
            self.query_string = query
            self.table_name = table_name
        
        else:
            ## If you want to read from a file your table name can only be saved as the name of the file. 
            hql_file = HQLFile(query)
            self.query_string = hql_file.read()
            self.table_name = hql_file.file_name.split("/")[-1]

        self.is_temp = is_temp       
        self.spark = SparkSession.builder.getOrCreate()

    def run(self, run_settings):
        start = time.time()
        formatted_qs = self.query_string.format(**run_settings)
        logging.log(level=10, msg = formatted_qs)

        if self.is_temp:
            logging.info(f"HQLQuery: Executing TEMP query {self.table_name}")
            self.spark.sql(formatted_qs).write.format("orc").mode("overwrite").saveAsTable(f"{run_settings['tmp_env']}.{self.table_name}")

        else: 
            logging.info(f"HQLQuery: Executing {self.table_name} ...")
            self.spark.sql(formatted_qs)
            
        end = time.time()
        _ , run_time_in_minutes = timer(start, end)
        print(f"***FINISHED*** in {run_time_in_minutes} minutes")
        print("---------------------------------------------------------------------------------")
        print("\n")

