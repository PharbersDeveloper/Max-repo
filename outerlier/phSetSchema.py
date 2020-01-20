

def udf_add_struct(prd_prod):
    schema = StructType(
        [
            StructField("other", DoubleType())
        ]
    )
    for p in prd_prod:
        schema.add(
            StructType(
                [
                    StructField(p, DoubleType())
                ]
            )
        )
     return schema




