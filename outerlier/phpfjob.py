# coding=utf-8
from pyspark.sql.functions import udf, from_json
from pyspark.sql.types import *
import json


def udf_get_poi(pdn):
    prd_input = [u"加罗宁", u"凯纷", u"诺扬"]
    result = ""
    for item in prd_input:
        if item in pdn:
            result = item

    if result == "":
        return u"其它"
    else:
        return result


def udf_poi_stack(p, s):
    return json.dumps({u"加罗宁": 0, u"凯纷": 0, u"诺扬": 0, u"其它": 0, p: s})


def max_outlier_poi_job(spark, df_EIA):
    max_outlier_poi_udf = udf(udf_get_poi, StringType())
    max_outlier_poi_stack_udf = udf(udf_poi_stack, StringType())

    df_EIA = df_EIA.withColumn("POI", max_outlier_poi_udf(df_EIA.Prod_Name))

    # TODO: 验证代码以后删除
    # data_EIA = df_EIA.toPandas()
    #
    # cols = data_EIA.drop(['Sales', 'Units'
    #                          , "Prod_Name", "Prod_CNAME"
    #                          , "Strength", "DOI", "DOIE"], axis=1).columns
    #
    # data_EIA[cols] = data_EIA[cols].fillna(" ")
    #
    # data_EIA2 = data_EIA. \
    #     groupby(cols.values.tolist() \
    #             ).agg({u'Sales': 'sum', u'Units': 'sum'})
    #
    # data_EIA3 = data_EIA2["Sales"].unstack("POI").reset_index()
    # print data_EIA3

    # 基于 ID，Date，Hosp_name， HOSP_ID 求和
    df_EIA_res = df_EIA.groupBy("ID", "Date", "Hosp_name", "HOSP_ID", "POI", "Year") \
        .agg({
            "Prod_Name": "first",
            "Prod_CNAME": "first",
            "Strength": "first",
            "DOI": "first",
            "DOIE": "first",
            "Sales": "sum",
            "Units": "sum"
        }) \
        .withColumnRenamed("first(Prod_Name)", "Prod_Name") \
        .withColumnRenamed("first(Prod_CNAME)", "Prod_CNAME") \
        .withColumnRenamed("first(Strength)", "Strength") \
        .withColumnRenamed("first(DOI)", "DOI") \
        .withColumnRenamed("first(DOIE)", "DOIE") \
        .withColumnRenamed("sum(Sales)", "Sales") \
        .withColumnRenamed("sum(Units)", "Units")

    df_EIA_res = df_EIA_res.withColumn("value", max_outlier_poi_stack_udf(df_EIA_res.POI, df_EIA_res.Sales))
    schema = StructType(
        [
            StructField(u"凯纷", DoubleType()),
            StructField(u"诺扬", DoubleType()),
            StructField(u"其它", DoubleType()),
            StructField(u"加罗宁", DoubleType()),
        ]
    )

    df_EIA_res = df_EIA_res.select(
        "ID", "Date", "Hosp_name", "HOSP_ID", "POI", "Year",
        "Prod_Name", "Prod_CNAME", "Strength", "DOI",
        "DOIE", "Sales", "Units",
        from_json(df_EIA_res.value, schema).alias("json")
    ).select(
        "ID", "Date", "Hosp_name", "HOSP_ID", "POI", "Year",
        "Prod_Name", "Prod_CNAME", "Strength", "DOI",
        "DOIE", "Sales", "Units", "json.*")

    df_EIA_res = df_EIA_res.groupBy("ID", "Date", "Hosp_name", "HOSP_ID", "Year").agg(
        {
            "凯纷": "sum",
            "诺扬": "sum",
            "其它": "sum",
            "加罗宁": "sum",
        })\
        .withColumnRenamed("sum(凯纷)", "凯纷")\
        .withColumnRenamed("sum(诺扬)", "诺扬")\
        .withColumnRenamed("sum(其它)", "其它")\
        .withColumnRenamed("sum(加罗宁)", "加罗宁")

    # print df_EIA.count()
    # df_EIA.show()

    return [df_EIA_res, df_EIA]