from pyspark.sql import functions as func


def sum_columns(df_eia, existing_names, result_name, new_name = True):
    if(new_name):
        df_eia = df_eia.withColumn(result_name, func.lit(0))
    for p in existing_names:
        df_eia = df_eia.withColumn(result_name,
                                   df_eia[p.encode("utf-8")] + df_eia[result_name])

    return df_eia