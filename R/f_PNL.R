# funcáo que implementa macro PNL de Consignado.sas
# para homologar, rodar para anoMesdia = 20150430 e anoMesDia_ant = 20150331
# obs: para gerar dados dos meses anteriores para base_carteira, usar anoMesDia = 20150331
# e gravar na pasta do mês respectivo em rawdata o dataframe df_riscbgn_base_carteira gerado
# PASSOS:
# 1. lê mês ATU do Oracle e grava base carteira de mês ATU
# 2. lê base carteira de mês ANT, merge com base carteira do mês ATU e grava cudo de mês ATU
f_PNL <- function(anoMesDia, anoMesDia_ant)
{
    # caminhos e arquivos
    rawDir <- "C:/MIS Asserth/TESTE/pkgctlm/rawdata"
    filein_fund <- paste0(rawDir,"/temp_fundinginputs.csv")
    filein_rcoef <- paste0(rawDir,"/temp_riscocoef.csv")

    mm_aaaa <- paste0(substr(anoMesDia,5,6),".",substr(anoMesDia,1,4))
    mm_aaaa_ant <- paste0(substr(anoMesDia_ant,5,6),".",substr(anoMesDia_ant,1,4))

    tidyDir <- "C:/MIS Asserth/TESTE/pkgctlm/tidydata"
    fileout_atu <- paste0(rawDir,"/", mm_aaaa,"/", "riscbgn_base_carteira_",anoMesDia, ".csv")
    filein_ant <- paste0(rawDir,"/", mm_aaaa_ant,"/", "riscbgn_base_carteira_",anoMesDia_ant, ".csv")
    fileout_cubo_cart <- paste0(rawDir,"/", mm_aaaa,"/", "riscbgn_cubo_carteira_",anoMesDia, ".csv")

    # conexao Oracle
    caminho <- "DWCTLPRD"
    userid <- "usr_pbgn_ltra"
    passwd <- "usr_pbgn_ltra"


    # libraries
    library(RODBC)
    library (dplyr)

    # --------------------------------------------------------------------------
    # --- 1. cria dataframe temporário de carteira, filtrando por anoMesDia
    # --- 2. merge com arquivo pre gerados de fundinginputs
    # --- 3. merge com arquivo pre gerados de riscocoef
    # --- 4. cria dataframe df_riscbgn_base_carteira a partir de temporario carteira
    # --- 5. grava base carteira em arquivo csv referente a anoMesDia
    # --------------------------------------------------------------------------

    # ------------- PASSO 17 (OK)
    channel <- odbcConnect(caminho,uid=userid, pwd=passwd, believeNRows=FALSE)
    # cria tabela riscbgn.base_carteira
    cSQL_cart <- paste0("select tb_fat_oprc_mnsl.CD_OPRC,",
                   "tb_fat_oprc_mnsl.id_oprc_rlzd,",
                   "tb_dim_grpo_prmt.ds_grpo_prmt,",
                   "tb_dim_prdt.ds_prdt,",
                   "tb_dim_crtr.CD_CRTR as CD_CRTR_TEMP,",
                   "tb_fat_oprc_mnsl.id_faix_atrs,",
                   "tb_dim_epdr.ds_epdr,",
                   "tb_dim_flal.ds_flal,",
                   "(tb_fat_oprc_mnsl.mt_sldo_dvdr/100) as mt_sldo_dvdr,",
                   "(tb_fat_oprc_mnsl.mt_sldo_cntb_cdda/100) as mt_sldo_cntb_cdda,",
                   "(tb_fat_oprc_mnsl.mt_sldo_cntb_nao_cdda/100) as mt_sldo_cntb_nao_cdda,",
                   "tb_dim_prmt.ds_prmt,",
                   "tb_dim_crtr.ds_crtr,",
                   "tb_dim_cnal_vnda.ds_cnal_vnda,",
                   "tb_fat_oprc_mnsl.nr_dias_atrs,",
                   "(tb_fat_oprc_rlzd.vl_taxa_cl_ames/100) as vl_taxa_cl_ames,",
                   "tb_fat_oprc_rlzd.dt_prmr_vcto,",
                   "tb_fat_oprc_mnsl.id_ultm_dia_mes,",
                   "tb_fat_oprc_mnsl.dt_ctrl,",
                   "tb_fat_oprc_rlzd.qt_prcl,",
                   "(tb_fat_oprc_rlzd.vl_taxa_ames/100) as vl_taxa_ames,",
                   "tb_fat_oprc_mnsl.dt_fim_ctrt,",
                   "tb_fat_oprc_mnsl.dt_base as data_base",
                   " from usr_pbgn_load.tb_fat_oprc_mnsl,",
                   "usr_pbgn_load.tb_fat_oprc_rlzd,",
                   "usr_pbgn_load.tb_dim_prdt,",
                   "usr_pbgn_load.tb_dim_flal,",
                   "usr_pbgn_load.tb_dim_epdr,",
                   "usr_pbgn_load.tb_dim_grpo_prmt,",
                   "usr_pbgn_load.tb_dim_prmt,",
                   "usr_pbgn_load.tb_dim_crtr,",
                   "usr_pbgn_load.tb_dim_cnal_vnda",
                   " where tb_fat_oprc_rlzd.id_oprc_rlzd = tb_fat_oprc_mnsl.id_oprc_rlzd",
                   " and tb_dim_grpo_prmt.id_grpo_prmt = tb_fat_oprc_mnsl.id_grpo_prmt",
                   " and tb_dim_prdt.id_prdt = tb_fat_oprc_mnsl.id_prdt",
                   " and tb_fat_oprc_mnsl.id_epdr = tb_dim_epdr.id_epdr",
                   " and tb_dim_flal.id_flal = tb_fat_oprc_mnsl.id_flal",
                   " and tb_dim_prmt.id_prmt = tb_fat_oprc_mnsl.id_prmt",
                   " and tb_dim_crtr.id_crtr = tb_fat_oprc_mnsl.id_crtr",
                   " and tb_dim_cnal_vnda.id_cnal_vnda = tb_fat_oprc_mnsl.id_cnal_vnda",
                   " and tb_fat_oprc_mnsl.id_ultm_dia_mes = ", anoMesDia,
                   " and tb_fat_oprc_mnsl.mt_sldo_cntb_nao_cdda > 0;")

    # query abaixo rodou em 7 minutos no Windows Cetelem 32b
    df_temp_carteira <- sqlQuery(channel,cSQL_cart, errors = TRUE)
    # fecha conexão com Oracle ao sair
    odbcClose(channel)

    # -------------- PASSO 13 (OK)
    df_temp_carteira <-
        df_temp_carteira %>%
        arrange(ID_OPRC_RLZD) %>%
        mutate (SLD_CARTEIRA = 0,
            SLD_PREJUIZO = 0,
            MT_BRUTO_PRAZO = 0,
            MT_BRUTO_PRAZO_JUROS = 0,
            JUROS = 0,
            SLD_PREJUIZO = ifelse(ID_FAIX_ATRS == 10, MT_SLDO_CNTB_NAO_CDDA,0),
            SLD_CARTEIRA = ifelse(ID_FAIX_ATRS != 10, MT_SLDO_CNTB_NAO_CDDA,0),
            DT_REF=trunc(ID_ULTM_DIA_MES/100),
            DT_BASE=trunc(DATA_BASE/100),
            DT_INICIO_PGTO=trunc(DT_PRMR_VCTO/100),
            NR_PRCL_PEND = QT_PRCL - ((trunc(DT_REF/100)-trunc(DT_INICIO_PGTO/100))*12+((DT_REF %% 100)-(DT_INICIO_PGTO %% 100))),
            NR_PRCL_PEND = ifelse(NR_PRCL_PEND  < 0, 0, NR_PRCL_PEND),
            MT_BRUTO_PRAZO=(SLD_CARTEIRA*NR_PRCL_PEND),
            MT_BRUTO_PRAZO_JUROS=(SLD_CARTEIRA*NR_PRCL_PEND*as.numeric(VL_TAXA_AMES)),
            JUROS = ifelse(is.na(NR_DIAS_ATRS) | NR_DIAS_ATRS == 0, SLD_CARTEIRA*as.numeric(VL_TAXA_AMES),JUROS),
            ATRASO = ifelse(is.na(NR_DIAS_ATRS) | NR_DIAS_ATRS == 0, "EM DIA",
                     ifelse(NR_DIAS_ATRS <= 30, "  1-30",
                     ifelse(NR_DIAS_ATRS <= 59, " 31-59",
                     ifelse(NR_DIAS_ATRS <= 90, " 60-90","  > 90")))),
            FAIXA_REPORT = ifelse(NR_DIAS_ATRS <= 6, "R0",
                           ifelse(NR_DIAS_ATRS >= 7 &  NR_DIAS_ATRS <= 30, "R1",
                           ifelse(NR_DIAS_ATRS >= 31 &  NR_DIAS_ATRS <= 60, "R2",
                           ifelse(NR_DIAS_ATRS >= 61 &  NR_DIAS_ATRS <= 90, "R3",
                           ifelse(NR_DIAS_ATRS >= 91 &  NR_DIAS_ATRS <= 120, "R4",
                           ifelse(NR_DIAS_ATRS >= 121 &  NR_DIAS_ATRS <= 150, "R5",
                           ifelse(NR_DIAS_ATRS >= 151 &  NR_DIAS_ATRS <= 180, "R6","R7"))))))),
            CD_CRTR = ifelse(!(CD_CRTR_TEMP %in% c(50,52,53,55)), 99, CD_CRTR_TEMP)) %>%
        select (-(DATA_BASE)) %>%
        arrange(DT_BASE,QT_PRCL)

    # ------------- PASSO 14 e 15 (OK)
    # aqui ler csv de fundinginputs pre gerado
    df_fundinginputs <- read.csv(filein_fund, sep = ",")

    # --------- MERGES com arquivos preexistentes de fundinginputs e riscocoef
    # all.y -> se TRUE, traz todas as linhas de y e somente as de x que tem correspondente em y
    df_temp_carteira <- merge(df_fundinginputs, df_temp_carteira,by=c("DT_BASE", "QT_PRCL"), all.y = TRUE)

    df_temp_carteira <-
        df_temp_carteira %>%
        mutate(TX_FUNDING = ifelse(is.na(TX_FUNDING),0,TX_FUNDING),
               FUNDING = TX_FUNDING * SLD_CARTEIRA,
                FUNDING = ifelse(is.na(FUNDING),0,FUNDING)) %>%
        arrange(DT_REF,CD_CRTR, FAIXA_REPORT)

    # ------------- PASSO 16 (OK)

    # aqui ler csv de riscocoef pre gerado
    df_riscocoef <- read.csv(filein_rcoef, sep = ",")

    # all.y -> se TRUE, traz todas as linhas de y e somente as de x que tem correspondente em y
    df_temp_carteira <- merge(df_riscocoef, df_temp_carteira,by=c("DT_REF", "CD_CRTR", "FAIXA_REPORT"), all.y = TRUE)

    df_temp_carteira <-
        df_temp_carteira %>%
        mutate(COEFICIENTE = ifelse(is.na(COEFICIENTE),0,COEFICIENTE),
                PDD = COEFICIENTE * SLD_CARTEIRA,
                PDD = ifelse(is.na(PDD),0,PDD)) %>%
        arrange(DT_REF,CD_CRTR, FAIXA_REPORT)

    # ------------- PASSO 17 (OK)

    df_riscbgn_base_carteira <-
        df_temp_carteira %>%
        select (ID_OPRC_RLZD, DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
                DS_PRDT,DS_CRTR,CD_CRTR, ATRASO,FAIXA_REPORT,ID_FAIX_ATRS,MT_BRUTO_PRAZO,
                MT_BRUTO_PRAZO_JUROS,SLD_CARTEIRA,JUROS,PDD,FUNDING,SLD_PREJUIZO) %>%
        group_by(ID_OPRC_RLZD, DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
                DS_PRDT,DS_CRTR,CD_CRTR, ATRASO,FAIXA_REPORT,ID_FAIX_ATRS) %>%
        summarize(MT_BRUTO_PRAZO = sum(MT_BRUTO_PRAZO),
                MT_BRUTO_PRAZO_JUROS = sum(MT_BRUTO_PRAZO_JUROS),
                SLD_CARTEIRA = sum(SLD_CARTEIRA),
                JUROS = sum(JUROS),
                PDD = sum(PDD),
                FUNDING = sum(FUNDING),
                SLD_PREJUIZO = sum(SLD_PREJUIZO))

    # aqui gravar em riscbgn_base_carteira_AAAAMMDD
    write.csv2(df_riscbgn_base_carteira, file = fileout_atu)

    # remove base não usada
    rm(df_temp_carteira)

    # --------------------------------------------------------------------------
    # --- 1. le arquivo do mês anterior de base de carteira, referente a anoMesDia_ant
    # --- 2. merge com arquivo base de carteira do mês atual (anoMesDia)
    # --- 3. gera cubo carteira a partir arquivo resultado do merge para anoMesDia
    # --- 4. cria dataframe df_riscbgn_cubo_carteira a partir de bases combinadas acima
    # --- 5. grava em arquivo csv referente a anoMesDia
    # --------------------------------------------------------------------------




    # ------------- PASSO 18 (OK)
    # aqui ler csv de anoMEsDia_Ant para fazer o merge abaixo
    df_riscbgn_base_carteira_ant <- read.csv(filein_ant, sep = ";")
    # merge da base carteira de risco deste mês com a do mês anterior
    df_nova_base_risco_fim <- merge(df_riscbgn_base_carteira, df_riscbgn_base_carteira_ant,by="ID_OPRC_RLZD", suffixes = c("", "_ANT"), all = TRUE)

# removendo dataframes nao mais usados
rm (df_riscbgn_base_carteira_ant)

df_nova_base_risco_fim <-
    df_nova_base_risco_fim %>%
    mutate(PDD_ANT = ifelse(is.na(PDD_ANT), 0, PDD_ANT),
           PDD = ifelse(is.na(PDD), 0, PDD),
           SLD_CARTEIRA = ifelse(is.na(SLD_CARTEIRA), 0, SLD_CARTEIRA),
           MT_BRUTO_PRAZO = ifelse(is.na(MT_BRUTO_PRAZO), 0, MT_BRUTO_PRAZO),
           MT_BRUTO_PRAZO_JUROS = ifelse(is.na(MT_BRUTO_PRAZO_JUROS), 0, MT_BRUTO_PRAZO_JUROS),
           JUROS = ifelse(is.na(JUROS), 0, JUROS),
           FUNDING = ifelse(is.na(FUNDING), 0, FUNDING),
           SLD_PREJUIZO = ifelse(is.na(SLD_PREJUIZO), 0, SLD_PREJUIZO),
           DELTA_PDD = PDD_ANT - PDD,
           PERDA = 0,
           PERDA = ifelse(ID_FAIX_ATRS == 10 & ID_FAIX_ATRS_ANT != 10,SLD_PREJUIZO,PERDA),
           FAIXA_REPORT = ifelse(FAIXA_REPORT %in% (''),"R7", FAIXA_REPORT),
           ATRASO = ifelse(ATRASO %in% (''),"  > 90", ATRASO),
           DT_BASE = ifelse(DT_BASE == 0 | is.na(DT_BASE), DT_BASE_ANT, DT_BASE),
           DS_CNAL_VNDA = ifelse(DS_CNAL_VNDA %in% (''), DS_CNAL_VNDA_ANT, DS_CNAL_VNDA),
           DS_EPDR = ifelse(DS_EPDR %in% (''), DS_EPDR_ANT, DS_EPDR),
           DS_FLAL = ifelse(DS_FLAL %in% (''), DS_FLAL_ANT, DS_FLAL),
           DS_GRPO_PRMT = ifelse(DS_GRPO_PRMT %in% (''), DS_GRPO_PRMT_ANT, DS_GRPO_PRMT),
           DS_PRDT = ifelse(DS_PRDT %in% (''), DS_PRDT_ANT, DS_PRDT),
           CD_CRTR = ifelse(CD_CRTR %in% (''), CD_CRTR_ANT, CD_CRTR),
           DS_CRTR = ifelse(DS_CRTR %in% (''), DS_CRTR_ANT, DS_CRTR),
           ID_FAIX_ATRS = ifelse(ID_FAIX_ATRS %in% (''), ID_FAIX_ATRS_ANT, ID_FAIX_ATRS),
           DT_REF = ifelse(DT_REF == 0 | is.na(DT_REF), DT_REF_ANT, DT_REF)) %>%
           select (ID_OPRC_RLZD,DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
                   DS_PRDT,CD_CRTR,DS_CRTR,ID_FAIX_ATRS,PDD,DT_REF_ANT,DT_BASE_ANT,
                   DS_CNAL_VNDA_ANT,DS_EPDR_ANT,DS_FLAL_ANT,DS_GRPO_PRMT_ANT,
                   DS_PRDT_ANT,CD_CRTR_ANT,DS_CRTR_ANT,ID_FAIX_ATRS_ANT,PDD_ANT,ATRASO,
                   FAIXA_REPORT,MT_BRUTO_PRAZO,MT_BRUTO_PRAZO_JUROS, JUROS, FUNDING,SLD_PREJUIZO,
                   PERDA, DELTA_PDD, SLD_CARTEIRA)


# --------------- PASSO 19
df_riscbgn_cubo_carteira <-
    df_nova_base_risco_fim %>%
    select(DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
           DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,ID_FAIX_ATRS,MT_BRUTO_PRAZO,
           MT_BRUTO_PRAZO_JUROS,SLD_CARTEIRA, JUROS, PDD,PDD_ANT,DELTA_PDD,
           PERDA, FUNDING,SLD_PREJUIZO) %>%
    group_by(DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
             DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,ID_FAIX_ATRS) %>%
    summarize(MT_BRUTO_PRAZO = sum(MT_BRUTO_PRAZO),
              MT_BRUTO_PRAZO_JUROS = sum(MT_BRUTO_PRAZO_JUROS),
              SLD_CARTEIRA = sum(SLD_CARTEIRA),
              JUROS = sum(JUROS),
              PDD = sum(PDD),
              PDD_ANT = sum(PDD_ANT),
              DELTA_PDD = sum(DELTA_PDD),
              PERDA = sum(PERDA),
              FUNDING = sum(FUNDING),
              SLD_PREJUIZO = sum(SLD_PREJUIZO))

# aqui gravar em riscbgn_cubo_carteira_AAAAMMDD
write.csv2(df_riscbgn_cubo_carteira, file = fileout_cubo_cart)

}

