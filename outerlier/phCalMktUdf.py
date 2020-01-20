
def cal_mkt(list_prod, df_eia):
    df_eia["mkt_size"] = 0
    for p in list_prod:
        df_eia = df_eia.withColumn("mkt_size",
                                   df_eia[list_prod] + df_eia["mkt_size"])
    df_eia = df_eia.withColumn("mkt_size",
                               df_eia["other"] + df_eia["mkt_size"])
    return df_eia
