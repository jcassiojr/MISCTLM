f_funding <-function(df_temp_carteira) {
    # ------------- PASSO 14 e 15 (OK) Não precisa no momento destes passos
    # quando precisar, Amauri gera scritp para checar versão mais atual no
    # servidor e carregar no diretorio rawdata
    filein_fund <- paste0(rawDir,"/temp_fundinginputs.csv")
    # aqui ler csv de fundinginputs pre gerado e carregado no diretorio
    # procedimennto (Amauri): criar script para copiar arquivo mais atual a partir do servidor
    f_fundinginputs <- read.csv(filein_fund, sep = ",")

    # eliminando o símbolo de percentual "%"
    f_fundinginputs <-
        df_fundinginputs %>%
        mutate (TX_FUNDING = gsub("%", "", TX_FUNDING, perl=TRUE))

    # --------- MERGES com arquivos preexistentes de fundinginputs e riscocoef
    # all.y -> se TRUE, traz todas as linhas de y e somente as de x que tem correspondente em y
    df_temp_carteira <- merge(df_fundinginputs, df_temp_carteira,by=c("DT_BASE", "QT_PRCL"), all.y = TRUE)

    df_temp_carteira <-
        df_temp_carteira %>%
        mutate(TX_FUNDING = ifelse(is.na(TX_FUNDING),0,as.numeric(TX_FUNDING)),
               FUNDING = (TX_FUNDING * SLD_CARTEIRA)/100,
                FUNDING = ifelse(is.na(FUNDING),0,FUNDING)) %>%
        arrange(DT_REF,CD_CRTR, FAIXA_REPORT)
     #com diferença pequena! possível problema de precisão. Não usado no momento

    return(df_temp_carteira)
}
