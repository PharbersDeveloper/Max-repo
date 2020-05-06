# coding=utf-8

import numpy as np
import cvxpy as cp

from copy import deepcopy
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
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


def cvxpy_func(pdf):
    # pdf is a pandas.DataFrame
    # v = pdf.ISCOUNTY
    # v = v - v.mean() + 1.0

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
    result = ",".join([str(i) for i in x.value.tolist()])

    # TODO 返回值类型只凭借成了 String，如需 Array 类型可自行尝试
    return pdf.assign(result=result)


if __name__ == "__main__":
    pnl_path = u"/common/projects/max/AZ_Sanofi/universe_az_sanofi_onc"
    df_EIA = spark.read.parquet(pnl_path)

    schema = deepcopy(df_EIA.schema) # 深拷贝
    schema.add("result", StringType())
    pudf_cvxpy_func = pandas_udf(cvxpy_func, schema, PandasUDFType.GROUPED_MAP)

    df_EIA.groupby(["Hosp_Level", "Province"]).apply(pudf_cvxpy_func).show()
