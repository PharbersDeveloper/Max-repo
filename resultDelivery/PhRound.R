
unit_based_round <- function(df){
    
    unit_r <- bround(df$Predict_Unit)
    f <- unit_r/df$Predict_Unit
    f <- ifelse(isNull(f), 0, f)
    
    df <- df %>% mutate(f = f)
    df$Predict_Unit <- unit_r
    df$Predict_Sales <- bround(df$Predict_Sales * df$f, 2)
    
    df <- remove_nega(df, c('Predict_Sales', 'Predict_Unit'))
    
    if(F){
        df$Predict_Sales <- ifelse(df$Predict_Unit <= 0, 0,
                                   df$Predict_Sales)
        
        df$Predict_Sales <- ifelse(df$Predict_Sales <= 0, 0,
                                   df$Predict_Sales)
        
        
        df$Predict_Unit <- ifelse(df$Predict_Sales <= 0, 0,
                                  df$Predict_Unit)
        
        df$Predict_Unit <- ifelse(df$Predict_Unit <= 0, 0,
                                  df$Predict_Unit)
    }
    
    return(
        df
    )
}






