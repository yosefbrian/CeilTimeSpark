from __future__ import division
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
import pyspark.sql.functions as func
from pyspark.sql.types import DateType
import math



class timeseries(object):
    def __init__(self, path1, path2):
        a = sqlContext.read.parquet(path1)
        halte = sqlContext.read.parquet(path2)
        self.halte = halte
        self.a = a
        a.show()
    
    def aggregate(self, granularity):
        a = self.a
        halte = self.halte
        a.registerTempTable("datatj")
        select = sqlContext.sql("SELECT C9, C4, C7 FROM datatj ORDER BY `C9` DESC")
        joinedDF = select.join(halte, select.C9 == halte.C0).drop(halte.C4)
        summary = joinedDF.withColumn("count", lit(1))
        ts = summary.C4.cast("timestamp").cast("long")
        l  = long(granularity)
        f  = float(granularity)
        interval = (func.ceil(ts / l) * f).cast("timestamp").alias("interval")
        aggregated = summary.groupBy("C9", "C3", "C7", interval).sum("count")
        time = aggregated.withColumn('interval', aggregated.interval.cast('timestamp'))
        data = time.select(col("C9").alias("gate_id"), col("C3").alias("halte_id"), col("C7").alias("kartu"), col("interval").alias("waktu"), col("sum(count)").alias("jumlah"))
        self.data = data
        data.show()

    def display(self):
        data = self.data
        data.registerTempTable("data2")
        data.show()
	    
    def export(self, path):
        data = self.data
        data.rdd.map(lambda x: ",".join(map(str, x))).coalesce(1).saveAsTextFile(path)
	    