#' OBJETIVO:
#' ler os dados das origens (arquivos gerados pelo processamento SAS a partir do dump do banco
#' de dados DB2 do SICLID
#' gravar os arquivos csv de raw data para serem manipulados na geração do relatório RelCTLM01
#' as seguintes planilha serão criadas em formato csv:
#' 1. tidyCarprod.csv
#' 2. tidyCarteProd.csv
#' 3. tidyCarteSeguros.csv
#' 4. tidyCarteCartoes.csv
#' 5. tidyCarteAmort.csv
#' Passos:
#' 1.forçando todos os nomes das colunas em maiusculo e tocando chainefin por chaineori
#' 2.inserindo coluna de tipo da tabela para cada xx_ano e DadosAmort
#' 3.gravar cada tabela gerada em arquivo csv diferente com tidy data
#' obs: para ler os arquivos de entrada padronizar nomes gerados para AAAAMM para todos
#' obs2. manter os arquivos dos meses anteriores no ano nesta pasta"
#' entrada: mm_aaaa - mês/ano corrente a processar. Ex. maio -> 05.2015
#'          filein - path e parte do nome do arquivo de entrada sem o mes
#'          fileout - path e nome do arquivo gravado como csv
########################
f_SICLID_raw <- function(dataproc)
{
  out <- tryCatch(
  {  
    # constates
    mm_aaaa <- paste0(substr(dataproc,5,6),".",substr(dataproc,1,4))
    aaaamm <- paste0(substr(dataproc,1,4),substr(dataproc,5,6))
    carprod_in <- paste0("./rawdata/", mm_aaaa, "/CARPROD_", dataproc,".txt")
    carteprod_in <- paste0("./rawdata/", mm_aaaa, "/carte_prod_TDB_PRODUCAO_",dataproc,".csv")
    carteseguros_in <- paste0("./rawdata/", mm_aaaa, "/carte_seguros_", aaaamm,".csv")
    cartecartoes_in <- paste0("./rawdata/", mm_aaaa, "/carte_cartoes_", aaaamm,".csv")
    carteamort_in <- paste0("./rawdata/", mm_aaaa, "/carte_amort_",aaaamm,".csv")
    
    #fileout <- paste("./tidydata", mm_aaaa, "baseCarProd.xlsx", sep = "/")
    fileout1 <- paste("./tidydata", mm_aaaa, "tidyCarProd.csv", sep = "/")
    fileout2 <- paste("./tidydata", mm_aaaa, "tidyCarteProd.csv", sep = "/")
    fileout3 <- paste("./tidydata", mm_aaaa, "tidyCarteSeguros.csv", sep = "/")
    fileout4 <- paste("./tidydata", mm_aaaa, "tidyCarteCartoes.csv", sep = "/")
    fileout5 <- paste("./tidydata", mm_aaaa, "tidyCarteAmort.csv", sep = "/")

    # mes de processamento
    temHeader <- TRUE
    # meses <- c("Jan","Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez")
    
    ################################
    # aba <mês>", DADOS DE PRODUÇÃO CREDIÁRIO
    ################################
    # ler arquivos de janeiro/2015 a maio/2015
    #filein <- paste0(carprod_in,sprintf("%02d",mes),".csv")
    df_creprod <- f_leCsvCARPROD_raw(carprod_in, !temHeader)
    # wrinting the csv file. for Windows Excel 2007/10
    # mudando nomes
    new_names <- c("SOCIEDADE","DATAPROC", "CHAINEORI","PRODALP","MODPAY","TIPOTNC","NBFI","MTFI",
                   "DURFIN","TAC",  "TEC",	"TXCLM",	"TXVDRFINAL",	"SEGURO",	"RETENCAO")
    names(df_creprod) <- new_names
    # rearranjando colunas
    df_creprod <- 
      df_creprod %>%
      select(SOCIEDADE, CHAINEORI,PRODALP,MODPAY,TIPOTNC,NBFI,MTFI,
             DURFIN,TAC,  TEC,	TXCLM,	TXVDRFINAL,	SEGURO,	RETENCAO, DATAPROC)
    # proxima tentativa: trocar virgula por ponto ou truncar
    # mudando tipo de MTFI e TXVDR para numerico
    # (para nao dar overload de inteiro na multiplicacao a frente)
    #df_creprod <- transform(df_creprod, TXCLM = as.character(TXCLM))
    #df_creprod <- mutate(df_creprod, TXCLM = TXCLM / 10)
    #df_creprod <- mutate(df_creprod, TXCLM = TXCLM * 1000)
    
    # colocar filtro para gravar somente em baseCarProd o mês corrente
    # (pois não pode alterae valores dos meses anteriores. 
    # Depois questionar Rafael sobre isso.)
    #-------------OBS
    # orientação é forçar os meses anteriores como o mes corrente para bater com relatório
    # atual. Depois que alinhar como proceder, devemos alterar este procedimento
    df_creprod <-
      df_creprod %>%
      mutate(DATAPROC = paste0(substr(mm_aaaa,4,7),substr(mm_aaaa,1,2)))
    #df_creprod <-
    #  df_creprod %>%
    #  filter(DATAPROC == paste0(substr(mm_aaaa,4,7),substr(mm_aaaa,1,2)))
    # colocar teste para ver se filtrou alguma linha
    
    
    # grava arquivo csv
    #write.table(df_creprod, file = fileout1, quote = FALSE, sep = ";", row.names = FALSE)
    
    ###################
    # aba <mês>, DADOS DE PRODUÇÃO CARTÃO
    ###################
    # ler arquivos de janeiro/2015 a maio/2015
    #filein <- paste0(carteprod_in,sprintf("%02d",mes),".csv")
    df_carprod <- f_leCsvCARPROD_raw(carteprod_in,temHeader)
    # eliminando coluna COUNT_OF_SOCIEDADE
    df_carprod <- 
      df_carprod %>%
      select (-(COUNT_OF_SOCIEDADE))
    #-------------OBS
    # orientação é forçar os meses anteriores como o mes corrente para bater com relatório
    # atual. Depois que alinhar como proceder, devemos alterar este procedimento
    df_carprod <-
      df_carprod %>%
      mutate(DATAPROC = paste0(substr(mm_aaaa,4,7),substr(mm_aaaa,1,2)))
    # colocar filtro para gravar somente em baseCarProd o mês corrente
    # (pois não pode alterae valores dos meses anteriores. 
    # Depois questionar Rafael sobre isso.)
    #df_carprod <-
    #  df_carprod %>%
    #  filter(DATAPROC == paste0(substr(mm_aaaa,4,7),substr(mm_aaaa,1,2)))
    #new_names <- c("SOCIEDADE","DATAPROC", "CHAINEORI","PRODALP","LOCAL","NBFI","MTFI",
    #               "DURFIN","TAXFIN",  "TAXVDR")
    #names(df_creprod) <- new_names
    # rearranjando colunas
    #df_creprod <- 
    #  df_creprod %>%
    #  select(DATAPROC, SOCIEDADE, CHAINEORI,PRODALP,LOCAL,NBFI,MTFI,
    #         DURFIN,TAXFIN,TAXVDR)
    # wrinting the csv file. for Windows Excel 2007/10
    #write.table(df_carprod, file = fileout2, quote = FALSE, sep = ";", row.names = FALSE)
    ####################
    # aba <mês>, DADOS DE ADESÃO E BASE DE SEGUROS
    ##################
    # ler arquivos de janeiro/2015 a maio/2015
    #filein <- paste0(carteseguros_in,sprintf("%02d",mes),".csv")
    df_seguro <- f_leCsvCARPROD_raw(carteseguros_in,temHeader)
    #-------------OBS
    # orientação é forçar os meses anteriores como o mes corrente para bater com relatório
    # atual. Depois que alinhar como proceder, devemos alterar este procedimento
    df_seguro <-
      df_seguro %>%
      mutate(DATAPROC = paste0(substr(mm_aaaa,4,7),substr(mm_aaaa,1,2)))
    # colocar filtro para gravar somente em baseCarProd o mês corrente
    # (pois não pode alterae valores dos meses anteriores. 
    # Depois questionar Rafael sobre isso.)
    #df_seguro <-
    #  df_seguro %>%
    #  filter(DATAPROC == paste0(substr(mm_aaaa,4,7),substr(mm_aaaa,1,2)))
    # wrinting the csv file. for Windows Excel 2007/10
    #write.table(df_seguro, file = fileout3, quote = FALSE, sep = ";", row.names = FALSE)
    ###################
    # aba <mês>, DADOS DE CARTÃO
    ########################
    # ler arquivos de janeiro/2015 a maio/2015
    #filein <- paste0(cartecartoes_in,sprintf("%02d",mes),".csv")
    df_cartecartoes <- f_leCsvCARPROD_raw(cartecartoes_in,temHeader)
    #-------------OBS
    # orientação é forçar os meses anteriores como o mes corrente para bater com relatório
    # atual. Depois que alinhar como proceder, devemos alterar este procedimento
    df_cartecartoes <-
      df_cartecartoes %>%
      mutate(DATAPROC = paste0(substr(mm_aaaa,4,7),substr(mm_aaaa,1,2)))
    # colocar filtro para gravar somente em baseCarProd o mês corrente
    # (pois não pode alterae valores dos meses anteriores. 
    # Depois questionar Rafael sobre isso.)
    #df_cartecartoes <-
    #  df_cartecartoes %>%
    #  filter(DATAPROC == paste0(substr(mm_aaaa,4,7),substr(mm_aaaa,1,2)))
    # wrinting the csv file. for Windows Excel 2007/10
    #write.table(df_cartecartoes, file = fileout4, quote = FALSE, sep = ";", row.names = FALSE)
    ##################
    # aba DadosAmort, DADOS DE AMORTIZACAO
    #####################
    # ler arquivos de janeiro/2015 a maio/2015
    #filein <- paste0(carteamort_in,sprintf("%02d",mes),".csv")
    df_carteamort <- f_leCsvCARPROD_raw(carteamort_in,temHeader)
    #-------------OBS
    # orientação é forçar os meses anteriores como o mes corrente para bater com relatório
    # atual. Depois que alinhar como proceder, devemos alterar este procedimento
    df_carteamort <-
      df_carteamort %>%
      mutate(DATAPROC = paste0(substr(mm_aaaa,4,7),substr(mm_aaaa,1,2)))
    # colocar filtro para gravar somente em baseCarProd o mês corrente
    # (pois não pode alterae valores dos meses anteriores. 
    # Depois questionar Rafael sobre isso.)
    #df_carteamort <-
    #  df_carteamort %>%
    #  filter(DATAPROC == paste0(substr(mm_aaaa,4,7),substr(mm_aaaa,1,2)))
    # wrinting the csv file. for Windows Excel 2007/10write.csv(df_carteamort, file = fileout5, row.names = TRUE,fileEncoding = "UTF-16LE")
    #write.table(df_carteamort, file = fileout5, quote = FALSE, sep = ";", row.names = FALSE)
    
    #-----------------------------------
    # chamar aqui funcao temporaria que lê as abas de Jan a Abr da planilha TDB Producao
    # e concatena com os dataframes criados aqui
    # Somente após concatenados gravar os csvs acima
    #l <- f_carProd_temp()
    #l <- list()
    # concatenando dataframes
    #df_creprodData_ano <- l[[1]]
    #l$CREPROD <- rbind(df_creprod,l$CREPROD)
    #l$CARPROD <- rbind(df_carprod,l$CARPROD)
    #l$CARTSEG <- rbind(df_seguro,l$CARTSEG)
    #l$CARTCARD <- rbind(df_cartecartoes,l$CARTCARD)
    #l$AMORT <- rbind(df_carteamort,l$AMORT)
    
    # grava arquivo csv
    #write.table(l$CREPROD, file = fileout1, quote = FALSE, sep = ";", row.names = FALSE)
    #write.table(l$CARPROD, file = fileout2, quote = FALSE, sep = ";", row.names = FALSE)
    #write.table(l$CARTSEG, file = fileout3, quote = FALSE, sep = ";", row.names = FALSE)
    #write.table(l$CARTCARD, file = fileout4, quote = FALSE, sep = ";", row.names = FALSE)
    #write.table(l$AMORT, file = fileout5, quote = FALSE, sep = ";", row.names = FALSE)
    
    
   # gravando os arquivos .csv
    write.csv2(df_creprod, file = fileout1)
    write.csv2(df_carprod, file = fileout2)
    write.csv2(df_seguro, file = fileout3)
    write.csv2(df_cartecartoes, file = fileout4)
    write.csv2(df_carteamort, file = fileout5)
    # gravando em planilha
    #write.xlsx(l, file = fileout, colNames = TRUE, borders = "columns")
    #return(df_creprod)
    
    #-----------------------------------------------
  },
  error = function(cond) {
    message("\n>>> ERRO! Geração dos arquivos TIDY \n")
    message(cond)
    return(-1)
  },
  warning = function(cond) {
    message("\n>>> WARNING! Geração dos arquivos TIDY \n")
    message(cond)
    return(NULL)
  },
  finally = {
    message("\n>>> Processamento dos arquivos TIDY finalizado")
  }
  )

  return(out)
}
