from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import pyspark.sql.functions as func
from pyspark.sql.types import DateType


class toParquet(object):

    def __init__(self):
        GateSchema = StructType([StructField('gate_id', StringType(), True),
                     StructField('gate_kodifikasi', StringType(), True),
                     StructField('gate_nama', StringType(), True),
                     StructField('gate_kode_halte', StringType(), True),                  
                     StructField('gate_status', StringType(), True),       
                     StructField('gate_tipe', StringType(), True)])
                     
        TransaksiSchema = StructType([StructField('transaksi_id', StringType(), True),
                     StructField('transaksi_serial_number', StringType(), True),
                     StructField('transaksi_tanggal_pengakuan', StringType(), True),
                     StructField('transaksi_shift_pengakuan', StringType(), True),                  
                     StructField('transaksi_tanggal_transaksi_kartu', StringType(), True),       
                     StructField('transaksi_transaksi_istransit', StringType(), True),       
                     StructField('transaksi_jns_pengguna', StringType(), True),       
                     StructField('transaksi_jns_kartu', StringType(), True),       
                     StructField('transaksi_nominal', StringType(), True),       
                     StructField('transaksi_kode_gate_gab', StringType(), True),       
                     StructField('transaksi_moda', StringType(), True),       
                     StructField('transaksi_lokasi', StringType(), True),                  
                     StructField('transaksi_status', StringType(), True),       
                     StructField('transaksi_issuer', StringType(), True),       
                     StructField('transaksi_counter', StringType(), True),       
                     StructField('transaksi_pengiriman_timestamp', StringType(), True),       
                     StructField('transaksi_header', StringType(), True),       
                     StructField('transaksi_copy_timestamp', IntegerType(), True),       
                     StructField('transaksi_sent_status', StringType(), True),                 
                     StructField('transaksi_approved_status', StringType(), True),       
                     StructField('transaksi_unapproved_status', StringType(), True)])
        self.gate      = GateSchema
        self.transaksi = TransaksiSchema
                     
                     
    def save(self, path1, path2):
        TransaksiSchema = self.transaksi
        GateSchema = self.gate
        TransaksiDf = sqlContext.read.format('com.databricks.spark.csv').options(header='false').load("hdfs:///user/zeppelin/newtransjakdb/smts_transjakarta/transaksi/part-m-00000")
        GateDf = sqlContext.read.format('com.databricks.spark.csv').options(header='false').load("hdfs:///user/zeppelin/newtransjakdb/smts_transjakarta/gate_ref/part-m-00000")
        TransaksiDf.write.format('parquet').save(path1)
        GateDf.write.format('parquet').save(path2)