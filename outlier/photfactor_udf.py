# coding=utf-8
import time
from pyspark.sql.types import *
import numpy as np
from phOutlierParameters import df_rlt_brf_path,df_factor_result_path,df_result_path

from copy import deepcopy
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,DoubleType
from pyspark.sql.functions import pandas_udf, PandasUDFType

spark = SparkSession.builder \
    .master("yarn") \
    .appName("udf_cvxpy_demo") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.instance", "4") \
    .config("spark.sql.execution.arrow.enabled", "true") \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", 10000) \
    .getOrCreate()
 

def cvxpy_func(rltsc):
    # rltsc is a pandas.DataFrame
  
    import numpy as np
    from cvxpy import *
    #from phOutlierParameters import prd_input
    prd_input = [u"普米克令舒", u"Others-Pulmicort", u"益索"]
    
    fst_prd=3
    bias=2
    
    rltsc=rltsc.fillna(0)
    print (len(rltsc))
    f = Variable()
    poi_ratio = {}
    mkt_ratio = {}

    for iprd in range(len(rltsc.index)):
        if rltsc["ims_poi_vol"][iprd] == 0:
            poi_ratio[iprd] = 0
        else:
            poi_ratio[iprd] = np.divide(
                (rltsc["poi_vol"][iprd] - rltsc["sales_pnl"][iprd]) * f + rltsc["sales_pnl"][iprd],
                rltsc["ims_poi_vol"][iprd]) - 1
        if rltsc["ims_mkt_vol"][iprd] == 0:
            mkt_ratio[iprd] = 0
        else:
            mkt_ratio[iprd] = np.divide(
                (rltsc["mkt_vol"][iprd] - rltsc["sales_pnl_mkt"][iprd]) * f + rltsc["sales_pnl_mkt"][iprd],
                rltsc["ims_mkt_vol"][iprd]) - 1

    par = []
    for s in range(len(rltsc.index)):
        if rltsc["poi"][s] in prd_input[:fst_prd]:
            par += ["np.divide(abs(poi_ratio[%s])," % s + str(bias) + ")"]
            par += ["abs(mkt_ratio[%s])" % s]
    exec ("obj=Minimize(maximum(" + ",".join(par) + "))")
    ##      obj=Minimize(max_elemwise(abs(poi_ratio[0]),abs(poi_ratio[1]),abs(poi_ratio[2]),abs(poi_ratio[3]),
    #                                abs(mkt_ratio[0]),abs(mkt_ratio[1]),abs(mkt_ratio[2]),abs(mkt_ratio[3])))
    #print(obj)
    #minimize maximum(abs((3716916.3308907785 * var0 + 13260299.0) / 18102154.0 + -1.0) / 2.0, abs((26475892.12411076 * var0 + 104242148.0) / 149269415.0 + -1.0), abs((nan * var0 + nan) / 0.0 + -1.0) / 2.0, abs((26475892.12411076 * var0 + 104242148.0) / 149269415.0 + -1.0), abs((12442094.414764605 * var0 + 49410439.0) / 74349372.0 + -1.0) / 2.0, abs((26475892.12411076 * var0 + 104242148.0) / 149269415.0 + -1.0))
    
    prob = Problem(obj, [0 <= f])
    prob.solve(solver=cvxpy.ECOS)
    #rltsc["factor"] = f.value
    #for i in range(len(rltsc)):
    #    scennew = ",".join(rltsc["scen"][i])
      
    return rltsc.assign(factor=f.value)
    


def max_outlier_factor(spark, df_result):    
   
    schema = deepcopy(df_result.schema) # 深拷贝
    schema.add("factor", DoubleType())
    
    pudf_cvxpy_func = pandas_udf(cvxpy_func, schema, PandasUDFType.GROUPED_MAP)
    #print(pudf_cvxpy_func)
    df_factor_result=df_result.groupby(["city", "scen_id"]).apply(pudf_cvxpy_func)
    
    
    df_factor_result = df_factor_result.withColumn("poi_tmp",
                                                   ((df_factor_result.poi_vol - df_factor_result.sales_pnl) *
                                                    df_factor_result.factor + df_factor_result.sales_pnl)) \
        .withColumn("mkt_tmp",
                    ((df_factor_result.mkt_vol - df_factor_result.sales_pnl_mkt) *
                     df_factor_result.factor + df_factor_result.sales_pnl_mkt))

    df_factor_result = df_factor_result \
        .withColumn("poi_ratio", df_factor_result.poi_tmp / df_factor_result.ims_poi_vol - 1) \
        .withColumn("mkt_ratio", df_factor_result.mkt_tmp / df_factor_result.ims_mkt_vol - 1) \
        .withColumn("share_factorized", df_factor_result.poi_tmp / df_factor_result.mkt_tmp) \
        .withColumn("share_gap", (df_factor_result.poi_tmp / df_factor_result.mkt_tmp) - df_factor_result.ims_share)

    # df_factor_result.show()

    df_factor_result = df_factor_result.withColumn("rel_gap", df_factor_result.share_gap / df_factor_result.ims_share)
    # brf 的那个行转列我就不写了 @luke
    df_rlt_brf = df_factor_result.select("city", "ims_mkt_vol", "scen", "scen_id", "num_ot", "mkt_ratio", "rel_gap",
                                         "poi")
    return [df_factor_result, df_rlt_brf]

start_time = time.time() 
   
#df_result_path="/user/spark/max/AZ_Sanofi/outlier/df_result"   
df_result=spark.read.parquet(df_result_path)

df_result=df_result.withColumn("scen", df_result["scen"].cast(StringType()))
[df_factor_result, df_rlt_brf]=max_outlier_factor(spark, df_result)
df_factor_result.show(5)

df_factor_result = df_factor_result.repartition(2)
df_rlt_brf = df_rlt_brf.repartition(2)
df_factor_result.write.format("parquet") \
    .mode("overwrite").save(df_factor_result_path)
df_rlt_brf.write.format("parquet") \
    .mode("overwrite").save(df_rlt_brf_path)

end_time = time.time()  # 记录程序结束运行时间
print('Took %f second' % (end_time - start_time))    
#test    
#df_pandas=df_factor_result.toPandas()
#df_pandas.to_csv("test/df_factor_result_udf.csv",encoding = 'utf_8_sig')    
