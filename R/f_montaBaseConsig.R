f_montaBaseConsig() {
  filein_cuboprod <- paste0(tidyDir,"/", mm_aaaa,"/", "riscbgn_cubo_producao_",anoMesDia, "_tidy.csv")
  filein_cuboliqu <- paste0(tidyDir,"/", mm_aaaa,"/", "riscbgn_cubo_liquidacao_",anoMesDia, "_tidy.csv")
  filein_basecart <- paste0(tidyDir,"/", mm_aaaa,"/", "riscbgn_base_carteira_",anoMesDia, "_tidy.csv")
  fileout_baseconsig <- paste0(tidyDir,"/", mm_aaaa,"/", "riscbgn_base_consignado_",anoMesDia, "_tidy.csv")
  
  df_prod <- read.csv(filein_cuboprod, header = TRUE, sep = ";")
  df_liqu <- read.csv(filein_cuboliqu, header = TRUE, sep = ";")
  df_cart <- read.csv(filein_basecart, header = TRUE, sep = ";")
  
  # monta baseConsignado para planilha TDB Producao
  # seleciona somente as colunas necessárias de cubo producao
  df_prod <-
    df_prod %>%
      select (DT_REF,DS_CNAL_VNDA,DS_EPDR,DS_GRPO_PRMT,DS_PRDT,DS_CRTR,
              QTD_PRODUCAO,VLR_PRODUCAO_BRUTA,VLR_IOF,VLR_FUTURO,VLR_PRODUCAO_LIQUIDA,
              VLR_PRODUCAO_REFIN, MT_BRUTO_PRAZO_PRD, MT_BRUTO_PRAZO_JUROS_PRD)
  # seleciona somente as colunas necessárias de cubo liquidacao
  df_liqu <-
    df_liqu %>%
    select (DT_REF,DS_CNAL_VNDA,DS_EPDR,DS_GRPO_PRMT,DS_PRDT,DS_CRTR,
            VLR_PROD_LIQ, VLR_LIQ, VLR_LIQ_TAX, VLR_PROD_LIQ_PRAZO)
  # seleciona somente as colunas necessárias de base carteira
  df_cart <-
    df_cart %>%
    select (DT_REF,DS_CNAL_VNDA,DS_EPDR,DS_GRPO_PRMT,DS_PRDT,DS_CRTR,
            MT_BRUTO_PRAZO, MT_BRUTO_PRAZO_JUROS, SLD_CARTEIRA)
  # merge das três bases
  df_baseConsignado <- merge(df_prod, df_liqu,by=c("DT_REF","DS_CNAL_VNDA", "DS_EPDR", "DS_GRPO_PRMT",
                                                   "DS_PRDT", "DS_CRTR"), all = TRUE)
  
  df_baseConsignado <- merge(df_baseConsignado, df_cart,by=c("DT_REF","DS_CNAL_VNDA", "DS_EPDR", "DS_GRPO_PRMT",
                                                   "DS_PRDT", "DS_CRTR"), all = TRUE)
  
  write.csv2(df_baseConsignado, file = fileout_baseconsig)
 
}