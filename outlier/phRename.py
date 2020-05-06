# coding=utf-8

def udf_rename(df_eia, list_prod, suffix=""):
    for p in list_prod:
        df_eia = df_eia.withColumnRenamed("sum("+p+")",
                                          p+suffix)

    # df_eia = df_eia.withColumnRenamed("sum(other)",
    #                                   "other").withColumnRenamed("sum(Est_DrugIncome_RMB)",
    #                                                              "Est_DrugIncome_RMB")
    return df_eia