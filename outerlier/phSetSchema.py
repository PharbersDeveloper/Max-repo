# coding=utf-8
from pyspark.sql.types import *


def udf_add_struct(prd_prod):
    # schema = StructType(
    #     [
    #         StructField("other", DoubleType())
    #     ]
    # )

    # for i in range(len(prd_prod)):
    #     if i == 0:
    #         schema = StructType(
    #             [
    #                 StructField(prd_prod[0], DoubleType())
    #             ]
    #         )
    #     else:
    #         schema.add(
    #             StructField(prd_prod[i], DoubleType())
    #         )
    schema = StructType(
        []
    )
    for i in range(len(prd_prod)):
        schema.add(
                StructField(prd_prod[i], DoubleType())
            )
    return schema
