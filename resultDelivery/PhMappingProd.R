mapping_prod <- function(df, prod_mapping){
    mp <- distinct(select(prod_mapping, "mkt", "min2", "通用名", '商品名',
                          "剂型", "规格", "包装数量", "生产企业", "ATC3"))
    
    new_product <- ifelse(df$Product == 'Boen Nuokang       B&amp;b|Boen Nuokang Vial Dry 3.75mg 1|BJ.BIOTE PHARM',
                          'Boen Nuokang       B&b|Boen Nuokang Vial Dry 3.75mg 1|BJ.BIOTE PHARM',
                          df$Product)
    df <- df %>%
        mutate(Product = 
                   new_product
               )
    
    
    df <- join(df, mp, df$Product == mp$min2, "left") %>%
        drop_dup_cols()
    
    print(sum(is.null(df$mkt)))
    return(df)
}