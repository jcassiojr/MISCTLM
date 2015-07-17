# teste de conexao Oracle
library(RODBC)
channel <- odbcConnect("DWCTLPRD",uid="usr_pbgn_ltra", pwd="usr_pbgn_ltra", believeNRows=FALSE)
df <- sqlQuery(channel,"SELECT * FROM usr_pbgn_load.tb_dim_cnal_vnda")
odbcClose(channel)
#############################
# OK abaixo
#cSQL <- paste0("Select count(*) ",
#                  " From USR_PBGN_LOAD.TB_FAT_OPRC_RLZD A11,",
#                  " USR_PBGN_LOAD.TB_DIM_CNAL_VNDA A12",
#                  " Where A11.ID_CNAL_VNDA = A12.ID_CNAL_VNDA"
#               )

# abaixo funcionou!
cSQL <- paste0("Select count(*) ",
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
               " A11.id_grpo_prmt = A22.ID_GRPO_PRMT;"
)

# inserindo clausula Group by
cSQL <- paste0("Select count(*) ",
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
               " A11.id_grpo_prmt = A22.ID_GRPO_PRMT",
               " Group By round(a11.dt_base/100),",
               "round(a11.dt_crga/100),",
               "A12.DS_CNAL_VNDA,",
               "A13.DS_EPDR,",
               "A14.DS_FLAL,",
               "A14.Cd_Flal,",
               "A15.ds_prdt,",
               "a16.DS_PRMT,",
               "A22.DS_GRPO_PRMT,",
               "a18.ds_crtr ,",
               "a18.cd_crtr;"
)
# OK: inserir o where completo
cSQL <- paste0("Select count(*) ",
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
               "a18.ds_crtr ,",
               "a18.cd_crtr;"
)

#  inserir select completo
cSQL <- paste0("Select round(a11.dt_base/100) as dt_base,",
               "round(a11.dt_crga/100) as dt_ref,",
               "A12.DS_CNAL_VNDA,",
               "A13.DS_EPDR,",
               "A14.DS_FLAL,",
               "A14.Cd_Flal,",
               " A15.ds_prdt,",
               "a16.DS_PRMT,",
               "A22.DS_GRPO_PRMT,a18.ds_crtr,",
               "a18.cd_crtr as CD_CRTR_TEMP,",
               "count(A11.ID_OPRC_RLZD) as QTD_PROCUCAO,",
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
               "a18.ds_crtr ,",
               "a18.cd_crtr;"
)

# teste do consignado sql 4
# obs. aparentemente clausula CASE/WHEN não funciona em RODBC. Trocando para clausula DECODE funciona!
# query abaixo rodou em 17 minutos
cSQL <- paste0("Select round(r.dt_crga/100) as dt_ref,", # -- Extrai ano e m?s (exemplo: 20140605 -> 201406)
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
               "DECODE (x.cd_hist_fncr, '104',sum(round(r.vl_mvmt/-100,2)),sum(round((r.vl_mvmt*t.qt_prcl*t.vl_taxa_ames)/ 10000,2))) as vl_mvmt_qtd_tx,",
               "DECODE (x.cd_hist_fncr, '104',sum(round((r.vl_mvmt*t.qt_prcl*t.vl_taxa_ames)/-10000,2)),sum(round(r.vl_mvmt/ 100,2))) as vl_mvmt,",
               "DECODE (x.cd_hist_fncr, '104',sum(round((r.vl_mvmt*t.vl_taxa_ames)/-10000,2)),sum(round(r.vl_mvmt/ 100,2))) as vl_mvmt_tx",
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
               "(round(r.dt_crga/10000)-round(t.dt_base/10000))*12+mod(round(r.dt_crga/100),100)-mod(round(t.dt_base/100),100);"

)
df <- sqlQuery(channel,cSQL)
  #sqlQuery(channel,"Select count(*) FROM USR_PBGN_LOAD.TB_FAT_OPRC_RLZD A11;") # OK
system.time(sqlQuery(channel,cSQL))
memory.limit(4000)
memory.size(max=T)

# ler e comparar arquivos consignado SAS x R
sasDir <- "C:/MIS Asserth/migracao/consignado/arquivos SAS para homologar/"
filein <- paste0(sasDir,"temp_producao-07-2015.csv")
df2 <- read.csv(filein, sep = ",")
# rodar passo a passo o carregaConsignado, usando anoMesDia = "20150630" e comparar datasets
# 1. valores de temp_producao batem, mas Oracle traz com virgula e R com ponto. Por
# isso dá diferença quando gera arquivo cubo, provavelmente. tb dá diferença na qtde trazida.
#
# 2. s
df_temp_producao[1,13]
df2[1,13] # sas temp.producao
x <-
    df_temp_producao %>%
    select(DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
                   DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,id_faix_atrs, QTD_PRODUCAO,
                   VLR_PRODUCAO_BRUTA) %>%
    group_by(DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
             DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,id_faix_atrs) %>%
    summarize(VLR_PRODUCAO_BRUTA = sum(as.numeric(VLR_PRODUCAO_BRUTA)))

# salvando temp_producao em csv para testar em casa
write.csv2(df_riscbgn_cubo_producao, file = "C:/MIS Asserth/migracao/cubo_producao_tst.csv")

# testes em casa. forçar carga com valores decimais com virgula
# para testar passar a ponto decimal
rawDir <- "./rawdata"
filein <- paste0(rawDir,"/temp_producao_R.csv")
df <- read.csv(filein, dec = ".", sep = ";") # forçar vírgula ao ler para dataframe
df <- read.csv2(filein) # lê com ponto decimal
# remove coluna X
df <-
    df %>%
    select(-(X))
#x <-
#    df %>%
#    mutate(VLR_PRODUCAO_BRUTA = VLR_PRODUCAO_BRUTA * 2)
# atenção: convertendo fator com is.numeric não dá certo para valores decimais, mesmo com ponto
# mas sem conversão a operaçao com ponto funciona e com virgula não!!

# mudar vírgula para ponto decimal somente nos campos decimais
# não aplica corretamente para campos character, por isso:
# inx: número de colunas a mudar
# trocar inx <- 1:4 para inx <- c(1,3,7, etc) caso colunas não adjacentes
inx = 13:22
fc <- function(x, inx){
    #nm <- names(x)[inx]
    nm <- names(x)[inx]
    gsub(pattern = ",", replacement = ".", x = x[,nm])
}
  
df2 <- data.frame(lapply(df, function(x) gsub(",", ".", x, fixed = TRUE)))         
