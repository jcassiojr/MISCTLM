function f_coefrisco(df_temp_carteira) {
    # aqui ler csv de riscocoef pre gerado
    filein_rcoef <- paste0(rawDir,"/temp_riscocoef.csv")
    # procedimennto (Amauri): criar script para copiar arquivo mais atual a partir do servidor
    df_riscocoef <- read.csv(filein_rcoef, sep = ",")

    # eliminando o símbolo de percentual "%"
    df_riscocoef <-
        df_riscocoef %>%
        mutate (COEFICIENTE = gsub("%", "", COEFICIENTE, perl=TRUE))

    # all.y -> se TRUE, traz todas as linhas de y e somente as de x que tem correspondente em y
    df_temp_carteira <- merge(df_riscocoef, df_temp_carteira,by=c("DT_REF", "CD_CRTR", "FAIXA_REPORT"), all.y = TRUE)

    df_temp_carteira <-
        #x <-
        df_temp_carteira %>%
        mutate(COEFICIENTE = ifelse(is.na(COEFICIENTE),0,as.numeric(COEFICIENTE)),
               PDD = (COEFICIENTE * SLD_CARTEIRA)/100,
               PDD = ifelse(is.na(PDD),0,PDD)) %>%
        arrange(DT_REF,CD_CRTR,FAIXA_REPORT)
    # com diferença pequena! possível problema de precisão. Não usado no momento

    return(df_temp_carteira)
}
