from pyspark.sql import functions as func


def cal_mkt(list_prod, df_eia):
    df_eia = df_eia.withColumn("mkt_size", func.lit(0))
    for p in list_prod:
        df_eia = df_eia.withColumn("mkt_size",
                                   df_eia[p] + df_eia["mkt_size"])
    df_eia = df_eia.withColumn("mkt_size",
                               df_eia["other"] + df_eia["mkt_size"])
    return df_eia
