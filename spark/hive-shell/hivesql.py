from pyspark.sql import HiveContext 
from pyspark import SparkContext 

import time 

sc = SparkContext()

hc = HiveContext(sc)

f_name = int(round(time.time() * 1000))

qry = """
		select 
			date(date_time) as event_date,
			type,
			state,
			hashtag,
			count(*) as hashtag_count 
		from 
			user_server_logs 
		where 
			pid is null 

		group by 
			1,2,3,4
		"""

hashtag_count = hc.sql(qry)

hashtag_count.coalesce(1).write.format('parquet').save("gs://spark-cluster/stream-output/"+str(f_name))
