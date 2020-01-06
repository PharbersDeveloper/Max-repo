# coding=utf-8
from pyspark.sql.functions import udf
from pyspark.sql.types import *


def udf_get_poi(pdn):
    prd_input = [u"加罗宁", u"凯纷", u"诺扬"]
    if prd_input[0] in pdn:
        return prd_input[0]
    else:
        return u"其它"


def max_outlier_poi_job(spark, df_EIA):
    max_outlier_poi_udf = udf(udf_get_poi, StringType())

    df_EIA = df_EIA.withColumn("POI", max_outlier_poi_udf(df_EIA.Prod_Name))

    # 基于 ID，Date，Hosp_name， HOSP_ID 求和
    df_EIA.groupBy("ID", "Date", "Hosp_name", "HOSP_ID", "POI", "Year") \
        .agg({
            "Prod_Name": "first",
            "Prod_CNAME": "first",
            "Strength": "first",
            "DOI": "first",
            "DOIE": "first",
            "Sales": "sum",
            "Units": "sum"
        })
    return df_EIA
