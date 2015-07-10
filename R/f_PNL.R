# funcáo que implementa macro PNL de Consignado.sas
f_PNL <- function(anoMesDia, anoMesDia_ant)
{
    mm_aaaa <- paste0(substr(anoMesDia,5,6),".",substr(anoMesDia,1,4))
    mm_aaaa_ant <- paste0(substr(anoMesDia_ant,5,6),".",substr(anoMesDia_ant,1,4))
    
    # caminhos e arquivos
    rawDir <- "C:/MISAsserth/TESTE/MISCTLM/rawdata"
    tidyDir <- "C:/MISAsserth/TESTE/MISCTLM/tidydata"
    fileout_atu <- paste(rawDir, mm_aaaa, "riscbgn_base_carteira_",anoMesDia, ".csv", sep = "/")
    filein_ant <- paste(rawDir, mm_aaaa, "riscbgn_base_carteira_",anoMesDia_ant, ".csv", sep = "/")
    
    # conexao Oracle
    caminho <- "DWCTLPRD"
    userid <- "usr_pbgn_ltra"
    passwd <- "usr_pbgn_ltra"
    
    
    # conexao Oracle
    library(RODBC)
    # ------------- PASSO 15
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
# --------------- ATENCAO ---- ATENCAO -------
# ATENCAO: 
# AO final do processamentode df_riscbgn_base_carteira, devo grava-la em .csv. Na execuçao do próximo mês, 
# antes do merge do PASSO 18, devo ler o arquivo para fazer merge com o processado no mês atual
# cria tabela riscbgn.base_carteira do Oracle

df_temp_carteira <- sqlQuery(channel,cSQL_cart, errors = TRUE)
# fecha conexão com Oracle ao sair
odbcClose(channel)
df_temp_carteira <-
    df_temp_carteira %>%
    arrange(id_oprc_rlzd) %>%
    mutate (SLD_CARTEIRA = 0,
            SLD_PREJUIZO = 0,
            MT_BRUTO_PRAZO = 0,
            MT_BRUTO_PRAZO_JUROS = 0,
            JUROS = 0,
            SLD_PREJUIZO = ifelse(id_faix_atrs == 10, mt_sldo_cntb_nao_cdda,0),
            SLD_CARTEIRA = ifelse(id_faix_atrs != 10, mt_sldo_cntb_nao_cdda,0),
            DT_REF=trunc(id_ultm_dia_mes/100),
            DT_BASE=trunc(data_base/100),
            DT_INICIO_PGTO=trunc(dt_prmr_vcto/100),
            nr_prcl_pend=qt_prcl-((trunc(DT_REF/100)-trunc(DT_INICIO_PGTO/100))*12+((DT_REF %% 100)-(DT_INICIO_PGTO %% 100))),
            nr_prcl_pend = ifelse(nr_prcl_pend  < 0, 0, nr_prcl_pend),
            MT_BRUTO_PRAZO=(SLD_CARTEIRA*nr_prcl_pend),
            MT_BRUTO_PRAZO_JUROS=(SLD_CARTEIRA*nr_prcl_pend*VL_TAXA_AMES),
            JUROS = ifelse(is.na(nr_dias_atrs) | nr_dias_atrs == 0, SLD_CARTEIRA*VL_TAXA_AMES,JUROS),
            ATRASO = ifelse(is.na(nr_dias_atrs) | nr_dias_atrs == 0, "EM DIA",
                     ifelse(nr_dias_atrs <= 30, "  1-30",
                     ifelse(nr_dias_atrs <= 59, " 31-59",
                     ifelse(nr_dias_atrs <= 90, " 60-90","  > 90")))),
            FAIXA_REPORT = ifelse(nr_dias_atrs <= 6, "R0",
                           ifelse(nr_dias_atrs >= 7 &  nr_dias_atrs <= 30, "R1",
                           ifelse(nr_dias_atrs >= 31 &  nr_dias_atrs <= 60, "R2",
                           ifelse(nr_dias_atrs >= 61 &  nr_dias_atrs <= 90, "R3",
                           ifelse(nr_dias_atrs >= 91 &  nr_dias_atrs <= 120, "R4",
                           ifelse(nr_dias_atrs >= 121 &  nr_dias_atrs <= 150, "R5",
                           ifelse(nr_dias_atrs >= 151 &  nr_dias_atrs <= 180, "R6","R7"))))))),
            CD_CRTR = ifelse(!(CD_CRTR_TEMP %in% c(50,52,53,55)), 99, CD_CRTR_TEMP)) %>%
    select (-(data_base)) %>%
    arrange(DT_BASE,qt_prcl)
# --------- ATENCAO
# aqui deveria vir tratamento de funding e risco.Coef mas não temos as tabelas origens por enquanto
# ------------- PASSO 17
df_riscbgn_base_carteira <-
    df_temp_carteira %>%
    select (id_oprc_rlzd, DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
            DS_PRDT,DS_CRTR,CD_CRTR, ATRASO,FAIXA_REPORT,id_faix_atrs) %>%
    group_by(id_oprc_rlzd, DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
             DS_PRDT,DS_CRTR,CD_CRTR, ATRASO,FAIXA_REPORT,id_faix_atrs) %>%
    summarize(MT_BRUTO_PRAZO = sum(MT_BRUTO_PRAZO),
              MT_BRUTO_PRAZO_JUROS = sum(MT_BRUTO_PRAZO_JUROS),
              SLD_CARTEIRA = sum(SLD_CARTEIRA),
              JUROS = sum(JUROS),
              PDD = sum(PDD),
              FUNDING = sum(FUNDING),
              SLD_PREJUIZO = sum(SLD_PREJUIZO))

# aqui gravar em riscbgn_base_carteira_AAAAMMDD
write.csv2(df_riscbgn_base_carteira, file = fileout_atu)

# ------------- PASSO 18 e 19 e 20
# aqui ler csv de anoMEsDia_Ant para fazer o merge abaixo
df_riscbgn_base_carteira_ant <- read.csv(filein_ant, header = head, sep = ";")
# merge da base carteira de risco deste mês com a do mês anterior
df_nova_base_risco_fim <- merge(df_riscbgn_base_carteira, df_riscbgn_base_carteira_ant,by="ID_OPRC_RLZD", suffixes = c("", "_ANT"), all = TRUE)

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
                   DS_PRDT_ANT,CD_CRTR_ANT,DS_CRTR_ANT,ID_FAIX_ATRS_ANT,PDD_ANT)


# --------------- PASSO 19
df_riscbgn_cubo_carteira <-
    df_nova_base_risco_fim %>%
    select(DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
           DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,id_faix_atrs) %>%
    group_by(DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
             DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,id_faix_atrs) %>%
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
}

