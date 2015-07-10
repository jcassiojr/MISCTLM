# script de carga dos dados de produ??o consignado

# datas de processamento (mudar as duas abaixo a cada processamento)
anoMesDia <- "20150630" # último dia do mês de processamento. Mudar a cada mês
anoMesDia_ant <- "20150531" # último dia do mês anterior de processamento. Mudar a cada mês
anoMesDia_2ant <- "20150430" # último dia do segundo mês anterior de processamento. Mudar a cada mês

mm_aaaa <- paste0(substr(anoMesDia,5,6),".",substr(anoMesDia,1,4))
mm_aaaa_ant <- paste0(substr(anoMesDia_ant,5,6),".",substr(anoMesDia_ant,1,4))

# caminhos e arquivos
rawDir <- "C:/MISAsserth/TESTE/MISCTLM/rawdata"
tidyDir <- "C:/MISAsserth/TESTE/MISCTLM/tidydata"
fileout_cuboprod <- paste(rawDir, mm_aaaa, "riscbgn_cubo_producao_",anoMesDia, ".csv", sep = "/")
fileout_cuboliqu <- paste(rawDir, mm_aaaa, "riscbgn_cubo_liquidacao_",anoMesDia, ".csv", sep = "/")

# conexao Oracle
caminho <- "DWCTLPRD"
userid <- "usr_pbgn_ltra"
passwd <- "usr_pbgn_ltra"


# conexao Oracle
library(RODBC)
channel <- odbcConnect(caminho,uid=userid, pwd=passwd, believeNRows=FALSE)

# consulta sql
#consulta <- "SELECT * FROM usr_pbgn_load.tb_dim_cnal_vnda"
#consulta <- "SELECT * FROM USR_PBGN_LOAD.TB_FAT_OPRC_RLZD"
#df <- sqlQuery(channel,consulta)

# ------------- PASSO 1 (OK)

# cria tabela temp.producao a partir do Oracle
cSQL <- paste0("Select round(a11.dt_base/100) as dt_base,",
               "round(a11.dt_crga/100) as dt_ref,",
               "A12.DS_CNAL_VNDA,",
               "A13.DS_EPDR,",
               "A14.DS_FLAL,",
               "A14.Cd_Flal,",
               " A15.ds_prdt,",
               "a16.DS_PRMT,",
               "A22.DS_GRPO_PRMT,",
               "a18.ds_crtr,",
               "a18.cd_crtr as CD_CRTR_TEMP,",
               "count(A11.ID_OPRC_RLZD) as QTD_PRODUCAO,",
               "sum(MT_OPRC/100) as VLR_PRODUCAO_BRUTA,",
               "sum(MT_LBRD/100) as VLR_PRODUCAO_BRUTA_SEM_TAX,",
               "sum(MT_IOC/100) as VLR_IOF,",
               "sum(MT_PRCL/100) as VLR_PARCELA,",
               "sum(MT_BRTO/100) as VLR_FUTURO,",
               "sum(Mt_lqdo_cmcl/100) as VLR_PRODUCAO_LIQUIDA,",
               "sum(((A11.MT_OPRC/100-A11.MT_LQDO_CMCL/100))) as VLR_PRODUCAO_REFIN,",
               "sum(((MT_LBRD/100)-(A11.MT_OPRC/100-A11.MT_LQDO_CMCL/100))) as VLR_PRODUCAO_REFIN_SEM_TAX,",
               "sum((MT_OPRC*QT_PRCL)/100) as MT_BRUTO_PRAZO_PRD,",
               "sum((A11.MT_OPRC*A11.QT_PRCL*A11.VL_TAXA_AMES)/10000) as MT_BRUTO_PRAZO_JUROS_PRD",
               " From USR_PBGN_LOAD.TB_FAT_OPRC_RLZD A11,",
               " USR_PBGN_LOAD.TB_DIM_CNAL_VNDA A12,",
               " USR_PBGN_LOAD.TB_DIM_EPDR A13,",
               " USR_PBGN_LOAD.TB_DIM_FLAL A14,",
               " USR_PBGN_LOAD.TB_DIM_PRDT A15,",
               " usr_pbgn_load.tb_dim_prmt a16,",
               " USR_PBGN_LOAD.TB_DIM_GRNT_RGNL A17,",
               " USR_PBGN_LOAD.TB_DIM_CRTR A18,",
               " USR_PBGN_LOAD.TB_DIM_GRNT A19,",
               " USR_PBGN_LOAD.TB_DIM_CRDN A20,",
               " USR_PBGN_LOAD.TB_DIM_OPRD A21,",
               " usr_pbgn_load.tb_dim_grpo_prmt a22",
               " Where A11.ID_CNAL_VNDA = A12.ID_CNAL_VNDA and",
               " A11.ID_EPDR = A13.ID_EPDR and",
               " A11.ID_FLAL = A14.ID_FLAL and",
               " A11.ID_PRMT = a16.ID_PRMT and",
               " A11.id_prdt = A15.id_prdt and",
               " A11.id_grnt_rgnl = A17.id_grnt_rgnl and",
               " A11.ID_CRTR = A18.ID_CRTR and",
               " A11.id_grnt = A19.id_grnt and",
               " A11.id_crdn = A20.id_crdn and",
               " A11.id_oprd = A21.id_oprd and",
               " A11.id_grpo_prmt = A22.ID_GRPO_PRMT and",
               " round(a11.dt_base/100) >= 201301 and",
               " A11.DT_BASE < to_char(SYSDATE,'YYYY')*10000 + to_char(SYSDATE,'MM')*100 + to_char(SYSDATE,'DD')",
               " Group By round(a11.dt_base/100),",
               "round(a11.dt_crga/100),",
               "A12.DS_CNAL_VNDA,",
               "A13.DS_EPDR,",
               "A14.DS_FLAL,",
               "A14.Cd_Flal,",
               "A15.ds_prdt,",
               "a16.DS_PRMT,",
               "A22.DS_GRPO_PRMT,",
               "a18.ds_crtr,",
               "a18.cd_crtr;"
)
df_temp_producao <- sqlQuery(channel,consSQl, errors = TRUE)
# fecha conexão com Oracle ao sair
odbcClose(channel)

# ------------- PASSO 2 (OK)
df_temp_producao <-
    df_temp_producao %>%
    mutate (ATRASO = "Em Dia",
            FAIXA_REPORT = "R0",
            id_faix_atrs = -1,
            CD_CRTR = ifelse(!(CD_CRTR_TEMP %in% c(50,52,53,55)), 99, CD_CRTR_TEMP))

# ------------- PASSO 3 (OK)
# cria tabela riscbgn.cubo_producao a partir da tabela temp.producao
# entrada df_temp_producao
# saida: df_riscbgn_cubo_producao
df_riscbgn_cubo_producao <-
    df_temp_producao %>%
    select (DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
            DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,id_faix_atrs, QTD_PRODUCAO,
            VLR_PRODUCAO_BRUTA,VLR_PRODUCAO_BRUTA_SEM_TAX,VLR_IOF,
            VLR_PARCELA,VLR_FUTURO,VLR_PRODUCAO_LIQUIDA,VLR_PRODUCAO_REFIN,
            VLR_PRODUCAO_REFIN_SEM_TAX,MT_BRUTO_PRAZO_PRD,MT_BRUTO_PRAZO_JUROS_PRD) %>%
    group_by(DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
             DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,id_faix_atrs) %>%
    summarize(QTD_PRODUCAO = sum(QTD_PRODUCAO),
              VLR_PRODUCAO_BRUTA = sum(VLR_PRODUCAO_BRUTA),
              VLR_PRODUCAO_BRUTA_SEM_TAX = sum(VLR_PRODUCAO_BRUTA_SEM_TAX),
              VLR_IOF = sum(VLR_IOF),
              VLR_PARCELA = sum(VLR_PARCELA),
              VLR_FUTURO = sum(VLR_FUTURO),
              VLR_PRODUCAO_LIQUIDA = sum(VLR_PRODUCAO_LIQUIDA),
              VLR_PRODUCAO_REFIN = sum(VLR_PRODUCAO_REFIN),
              VLR_PRODUCAO_REFIN_SEM_TAX = sum(VLR_PRODUCAO_REFIN_SEM_TAX),
              MT_BRUTO_PRAZO_PRD = sum(MT_BRUTO_PRAZO_PRD),
              MT_BRUTO_PRAZO_JUROS_PRD = sum(MT_BRUTO_PRAZO_JUROS_PRD))

# aqui gravar em riscbgn_cubo_producao_AAAAMMDD
write.csv2(df_riscbgn_cubo_producao, file = fileout_cuboprod)

# ------------- PASSO 4
# entrada: tabelas oracle
# saida: df_temp_liquidacao
# cria tabela temp.producao
# abre conex?o
channel <- odbcConnect(caminho,uid=userid, pwd=passwd, believeNRows=FALSE)
#cria consulta
cSQL_liq <- paste0("Select round(r.dt_crga/100) as dt_ref,", # -- Extrai ano e m?s (exemplo: 20140605 -> 201406)
                   "r.cd_tipo_mvmt,",
                   "x.cd_hist_fncr,",
                   "x.ds_hist_fncr,",
                   "a.cd_cnal_vnda,",
                   "a.ds_cnal_vnda,",
                   "g.ds_oprd,",
                   "b.ds_crdn,",
                   "d.ds_grnt,",
                   "c.ds_grnt_rgnl,",
                   "e.ds_epdr,",
                   "f.ds_grpo_prmt,",
                   "h.ds_prdt,",
                   "i.ds_prmt,",
                   "k.DS_FLAL,",
                   "j.ds_crtr,",
                   "j.cd_crtr as cd_crtr_temp,",
                   "round(t.dt_base/100) as dt_base,", # -- Extrai ano e m?s (exemplo: 20140605 -> 201406)
                   "(round(r.dt_crga/10000) - round(t.dt_base/10000)) * 12 + mod(round(r.dt_crga/100),100) - mod(round(t.dt_base/100),100) as MOB,",
                   "sum(round(t.MT_OPRC/100,2)) as Producao,",
                   "case x.cd_hist_fncr when '104' then sum(round(r.vl_mvmt/-100,2)) else sum(round(r.vl_mvmt/ 100,2)) end as vl_mvmt,",
                   "case x.cd_hist_fncr when '104' then sum(round((r.vl_mvmt*t.qt_prcl*t.vl_taxa_ames)/-10000,2)) else sum(round((r.vl_mvmt*t.qt_prcl*t.vl_taxa_ames)/ 10000,2)) end as vl_mvmt_qtd_tx,",
                   "case x.cd_hist_fncr when '104' then sum(round((r.vl_mvmt*t.vl_taxa_ames)/-10000,2)) else sum(round((r.vl_mvmt*t.vl_taxa_ames)/10000,2)) end as vl_mvmt_tx",
                   " From usr_pbgn_load.tb_fat_mvmt_fncr r,",
                   "usr_pbgn_load.tb_dim_hist_fncr x,",
                   "usr_pbgn_load.Tb_Fat_Oprc_Rlzd t,",
                   "usr_pbgn_load.tb_dim_cnal_vnda a,",
                   "usr_pbgn_load.tb_dim_crdn b,",
                   "usr_pbgn_load.tb_dim_grnt_rgnl c,",
                   "usr_pbgn_load.tb_dim_grnt d,",
                   "usr_pbgn_load.tb_dim_epdr e,",
                   "usr_pbgn_load.tb_dim_grpo_prmt f,",
                   "usr_pbgn_load.tb_dim_oprd g,",
                   "usr_pbgn_load.Tb_Dim_Prdt H,",
                   "usr_pbgn_load.tb_dim_prmt i,",
                   "usr_pbgn_load.tb_dim_flal k,",
                   "usr_pbgn_load.tb_dim_crtr j",
                   " Where r.id_oprc_rlzd = t.id_oprc_rlzd and",
                   " t.id_crtr = j.id_crtr and",
                   " t.id_flal = k.id_flal and",
                   " t.id_cnal_vnda = a.id_cnal_vnda and",
                   " t.id_oprd = g.id_oprd and",
                   " t.id_crdn = b.id_crdn and",
                   " t.id_grnt_rgnl = c.id_grnt_rgnl and",
                   " t.id_grnt = d.id_grnt and",
                   " t.id_epdr = e.id_epdr and",
                   " t.id_grpo_prmt = f.id_grpo_prmt and",
                   " t.id_prdt = h.id_prdt and",
                   " t.id_prmt = i.id_prmt and",
                   " r.id_hist_fncr = x.id_hist_fncr and",
                   " r.dt_crga >= 20130131 and",
                   " x.cd_hist_fncr in ('104','219','220','251','840','218','250')",
                   " Group By round(r.dt_crga/100),",
                   "r.cd_tipo_mvmt,",
                   "x.cd_hist_fncr,",
                   "x.ds_hist_fncr,",
                   "a.cd_cnal_vnda,",
                   "a.ds_cnal_vnda,",
                   "g.ds_oprd,",
                   "b.ds_crdn,",
                   "d.ds_grnt,",
                   "c.ds_grnt_rgnl,",
                   "e.ds_epdr,",
                   "f.ds_grpo_prmt,",
                   "h.ds_prdt,",
                   "i.ds_prmt,",
                   "k.DS_FLAL,",
                   "j.ds_crtr,",
                   "j.cd_crtr,",
                   "round(t.dt_base/100),",
                   "(round(r.dt_crga/10000)-round(t.dt_base/10000))*12+mod(round(r.dt_crga/100),100)-mod(round(t.dt_base/100),100));"
)
df_temp_liquidacao <- sqlQuery(channel,cSQL_liq, errors = TRUE)
# fecha conex?o com Oracle ao sair
odbcClose(channel)

# ------------- PASSO 5
df_temp_liquidacao <-
    df_temp_liquidacao %>%
    mutate (ATRASO = "Em Dia",
            FAIXA_REPORT = "R0",
            id_faix_atrs = -1,
            CD_CRTR = ifelse(!(CD_CRTR_TEMP %in% c(50,52,53,55)), 99, CD_CRTR_TEMP))

# ------------- PASSO 6
# entrada df_temp_liquidacao
# saida: df_riscbgn_cubo_liquidacao
df_riscbgn_cubo_liquidacao <-
    df_temp_liquidacao %>%
    select (DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
            DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,id_faix_atrs, Prazo_med_liq = MOB) %>%
    group_by(DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
             DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,id_faix_atrs, Prazo_med_liq) %>%
    summarize(VLR_PROD_LIQ = sum(Producao),
              VLR_LIQ = sum(vl_mvmt),
              VLR_LIQ_TAX = sum(vl_mvmt_tx),
              VLR_PROD_LIQ_PRAZO = sum(Producao*MOB))

# aqui gravar em riscbgn_cubo_producao_AAAAMMDD
write.csv2(df_riscbgn_cubo_liquidacao, file = fileout_cuboliqu)

# ------------- PASSO 7 (aparentemente não neste momento)
#channel <- odbcConnect(caminho,uid=userid, pwd=passwd, believeNRows=FALSE)
#cria consulta -- CREATE TABLE temp.base_marcacao
#cSQL_marc <- paste0("Select tb_fat_oprc_rlzd.cd_oprc as OPERACAO_EY,",
#                   "tb_fat_oprc_rlzd.id_oprc_rlzd,",
#                   "tb_dim_cnal_vnda.ds_cnal_vnda,",
#                   "tb_dim_epdr.ds_epdr,",
#                  "tb_dim_flal.ds_flal,",
#                   "tb_dim_crtr.CD_CRTR as CD_CRTR_TEMP,",
#                   "tb_dim_crtr.DS_CRTR,",
#                   "tb_dim_grpo_prmt.ds_grpo_prmt,",
#                   "tb_dim_prdt.ds_prdt,",
#                   "tb_fat_oprc_rlzd.dt_base as data_base",
#                   " From usr_pbgn_load.tb_fat_oprc_rlzd,",
#                   "usr_pbgn_load.tb_dim_cnal_vnda,",
#                   "usr_pbgn_load.tb_dim_flal,",
#                   "usr_pbgn_load.tb_dim_epdr,",
#                   "usr_pbgn_load.tb_dim_grpo_prmt,",
#                   "usr_pbgn_load.tb_dim_prdt,",
#                   "usr_pbgn_load.tb_dim_crtr",
#                   " Where tb_dim_cnal_vnda.id_cnal_vnda = tb_fat_oprc_rlzd.id_cnal_vnda",
#                   " and tb_dim_crtr.id_crtr = tb_fat_oprc_rlzd.id_crtr",
#                   " and tb_dim_epdr.id_epdr = tb_fat_oprc_rlzd.id_epdr",
#                   " and tb_dim_flal.id_flal = tb_fat_oprc_rlzd.id_flal",
#                   " and tb_dim_grpo_prmt.id_grpo_prmt = tb_fat_oprc_rlzd.id_grpo_prmt",
#                   " and tb_dim_prdt.id_prdt = tb_fat_oprc_rlzd.id_prdt")

#df_temp_base_marcacao <- sqlQuery(channel,cSQL_marc, errors = TRUE)
# fecha conex?o com Oracle ao sair
#odbcClose(channel)

# ------------- PASSO 8 e 9 (9 parcial, não encontrados datasets temp.fundinginputs e temp.RiscoCoef no consignado.sas)
#df_temp_base_marcacao <-
#    df_temp_base_marcacao %>%
#    mutate (ATRASO = "Em Dia",
#            FAIXA_REPORT = "R0",
#            id_faix_atrs = -1,
#            CD_CRTR = ifelse(!(CD_CRTR_TEMP %in% c(50,52,53,55)), 99, CD_CRTR_TEMP)) %>%
#    select (-(CD_CRTR_TEMP)) %>%
#    arrange(OPERACAO_EY)

# ------------- PASSO 10, 11, 12, 13, 14 (NÃO USADO por falta de tabela funding), 15 e 16 (idem)
    
# --------------- PASSO 20 (executa a macro que implementa o que já foi executado acima)
# chama com mes anterior e mes anterior do anterior
f_PNL(anoMesDia_ant,anoMesDia_2ant)
# chama com mes corrente e mes anterior
f_PNL(anoMesDia,anoMesDia_ant)
# --------------- PASSO 24
# chama com mes anterior e mes anterior do anterior
f_PNL2(anoMesDia_ant,anoMesDia_2ant)
# chama com mes corrente e mes anterior
f_PNL2(anoMesDia,anoMesDia_ant)

# --------------- PASSO 25

# comissão. Não será usada por hora (obs. no SAS cria macro com mesmo nome uma
# segunda vez: PNL ??? no Consignado.sas. Quando for criar aqui mudar para f_PNL3!!)
    
# ----------- Fim do processamento consignado

