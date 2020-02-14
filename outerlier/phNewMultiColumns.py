from pyspark.sql import functions as func

def udf_new_columns(df_oth_seg, list_prod, suffix = "", value = 0):
    for p in list_prod:
        df_oth_seg = df_oth_seg.withColumn(p+suffix, func.lit(value))
    return df_oth_seg