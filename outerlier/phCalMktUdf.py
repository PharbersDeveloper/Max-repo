
def cal_mkt(list_prod, df_eia):
    df_eia["mkt_size"] = 0
    for p in list_prod:
        df_eia = df_eia.withColumn("mkt_size",
                           df_EIA_res_iter[list_prod] + df_EIA_res_iter["mkt_size"])
    df_eia = df_eia.withColumn("mkt_size",
                               df_EIA_res_iter["other"] + df_EIA_res_iter["mkt_size"])
    return df_eia
