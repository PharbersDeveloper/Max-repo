Sys.setenv(SPARK_HOME="/Users/alfredyang/Desktop/spark/spark-2.3.0-bin-hadoop2.7")
Sys.setenv(YARN_CONF_DIR="/Users/alfredyang/Desktop/hadoop-3.0.3/etc/hadoop/")

library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))

ss <- sparkR.session(master = "yarn",
                     appName = "HiveTest",
                     sparkConfig = list(spark.driver.memory = "1g",
                                        spark.executor.memory = "2g",
                                        spark.executor.cores = "2",
                                        spark.executor.instances = "1"),
                     enableHiveSupport=TRUE
)
print(ss)

alf <- sql("SELECT * FROM cpa_Janssen WHERE company='Janssen'")
print(head(alf))
