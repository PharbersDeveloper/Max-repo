# coding=utf-8

import numpy as np
import cvxpy as cp

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyarrow import *

spark = SparkSession.builder \
    .master("yarn") \
    .appName("udf_cvxpy_demo") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.instance", "4") \
    .getOrCreate()


def string_to_int(str):
    m = 30
    n = 20
    np.random.seed(1)
    A = np.random.randn(m, n)
    b = np.random.randn(m)

    x = cp.Variable(n)
    objective = cp.Minimize(cp.sum_squares(A * x - b))
    constraints = [0 <= x, x <= 1]
    prob = cp.Problem(objective, constraints)
    prob.solve()

    return np.array2string(x.value)      # TODO 返回值调整


udf_string_to_int = udf(string_to_int, StringType()) # TODO 返回值调整

if __name__ == "__main__":
    pnl_path = u"/common/projects/max/AZ_Sanofi/price"
    df_EIA = spark.read.parquet(pnl_path)
    print df_EIA.count()

    df_EIA = df_EIA.withColumn("result", udf_string_to_int(df_EIA.Panel_ID)) # TODO 入参调整
    df_EIA.show(truncate=False)
