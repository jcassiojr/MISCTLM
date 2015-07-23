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

# função e comando para mudar vírgula para ponto decimal somente nos campos decimais
# não aplica corretamente para campos character, por isso:
# inx: número de colunas a mudar
# trocar inx <- 1:4 para inx <- c(1,3,7, etc) caso colunas não adjacentes
inx = 13:22
fc <- function(x, inx){
    nm <- names(x)[inx]
    gsub(pattern = ",", replacement = ".", x = x[,nm])
}

df_temp_producao <- data.frame(lapply(df_temp_producao, function(x) gsub(",", ".", x, fixed = TRUE)))

# ler arquivo gerado pelo SAS para comparar valores riscbgn_cubo_producao
sasDir <- "C:/MIS Asserth/migracao/consignado/arquivos SAS para homologar/"
filein <- paste0(sasDir,"riscbgn_cubo_producao-07-2015.csv")
df_riscbgn_cubo_producao_SAS <- read.csv(filein, sep = ",")

# diferenças!!
# 1.ver como está quebrando no SQL do SAS de temp_producao para riscbgn_cubo_producao
# 2. carregar o do SAS no excel para replicar o agrupamento
# gravar em formato csv o df_temp_producao gerado no R para comparar com o do SAS
write.csv2(df_temp_producao, file = "C:/MIS Asserth/migracao/tem_producao_R_tst.csv")
# os conteúdos batem quando aplico os filtros que sumarizam, tanto no R como SAS para
# gerar df_riscbgn_cubo_producao, mas olhando nas planilhas de temp_producao
# agora chegar os riscbgn_cubo SAS e R carregados em R para ver qual está certo com este
# mesmo agrupamento

# funcionou! ao transformar valores inteiros e decimais, originalmente factors, para character e
# depois transformar em numerico com as.numeric (valores numéricos)) na propria soma

# agora: testar porque trouxe diferente número de linhas já no temp_producao
# Resposta: diferença somente nos dados de junho/2015, demais meses bateram os totais!!!
# está ok, pois foram gerados em tempos diferentes!!!

# testando valores de cubo_liquidacao
# colunas VL_MVMT e VL_MVMT_TX tem os mesmo valores e não deveria!!!
# VL_MVMT_TX não calcula. Replica valor de VL_MVMT ???!!!
# cubo liquidação bate! era erro no sql digitado no R


# próxuimo teste: ver se na planilha que Rafael passou os valores batem ao menos para os meses anteriores
#  com os valores de
# os valores da base de abril/2015 que Rafael passou parecem estar sumarizando os valores de
# riscbgn.cubo.producao do SAS por DS_FLAL!!. Mas melhor deixar discriminado e filtrar no excel!!
# tb, o riscbgn.cubo.producao somente tem valores de " Em dia" enquanto a base tem todos.
# aparentemente traz zero qd não em dia!!!! entrão tudo bem " Em dia"
# identifiquei os campos na base que vem de cubo_producao: QTD_PRODUCAO, VLR_PRODUCAO_BRUTA, etc

# agora entra em f_PNL
# conferir temp_carteira
# carregando o do sas (não cabe em planilha) (do R já calculado)
sasDir <- "C:/MIS Asserth/migracao/consignado/arquivos SAS para homologar/"
filein <- paste0(sasDir,"temp_carteira20150430.csv")
df_temp_carteira_sas <- read.csv(filein, sep = ",")

df_temp_carteira_sas %>%
    group_by(DS_CRTR) %>%
    summarise(s = sum(as.numeric(MT_SLDO_DVDR)))

df_temp_carteira_sas %>%
#x %>%
    group_by(CD_CRTR) %>%
    summarise(s = round(sum(as.numeric(MT_SLDO_DVDR)),2))
# o CD_CRTR_TEMP do R está ok com SAS. O problrma é no ifelse sobre CD_CRTR_TEMP!
# no R está jogando tudo no 99!!!
# corrigido! testar FAIXA_REPORT para ver se ok!!!
df_temp_carteira_sas %>%
#x %>%
    group_by(ID_FAIX_ATRS) %>%
    summarise(s = round(sum(as.numeric(MT_SLDO_DVDR)),2))
# com FAIXA_REPORT e ATRASO parece ocorrer mesmo erro q acima com CD_CRTR
# corrigido para os dois!!!!

# testando para id_faix_atras
# ok para ID_FAIX_ATRS!

# para teste completo, criar script que mostra todos os campos sumarizados pelos acima

# Abordagem: testar como acima para todos os grupamentos e bater os totais
# obs. porque na soma não mostra o decimal? acho q não tem mesmo

# -----------------------------------------------------------------
# testes em 22-07 -
#------------------------------------------------------------------
#------------------------------------------------------------------
#------------------------------------------------------------------

# --------------------------------------------------------------------------
# testando contra planilha base para colunas que vem de riscbgn_cubo_producao
# --------------------------------------------------------------------------

x <- df_riscbgn_cubo_producao %>%
    group_by(DT_REF) %>%
    summarise(S_QTD_PRODUCAO = sum(as.numeric(QTD_PRODUCAO)), # ok
              S_VLR_PRODUCAO_BRUTA = sum(as.double(VLR_PRODUCAO_BRUTA)), # arredondando centavos neste sumarização! dados discriminados ok!!
              S_VLR_IOF = sum(as.numeric(VLR_IOF)), # arredondando centavos. idem
              S_VLR_FUTURO = sum(as.numeric(VLR_FUTURO)), # arredondando centavos. idem
              S_VLR_PRODUCAO_LIQUIDA = sum(as.numeric(VLR_PRODUCAO_LIQUIDA)), # arredondando centavos. idem
              S_MT_BRUTO_PRAZO_PRD = sum(as.numeric(MT_BRUTO_PRAZO_PRD)), # ok
              S_VLR_PRODUCAO_REFIN = sum(as.numeric(VLR_PRODUCAO_REFIN)), # arredondando centavos. idem
              S_MT_BRUTO_PRAZO_JUROS_PRD = sum(as.numeric(MT_BRUTO_PRAZO_JUROS_PRD))) # arredondando centavos. idem

# nos testes acima, valores batem com gerados no SAS para valores da base oficial

# --------------------------------------------------------------------------
# testando contra planilha base para colunas que vem de riscbgn_cubo_liquidacao
# --------------------------------------------------------------------------

x <- df_riscbgn_cubo_liquidacao %>%
    group_by(DT_REF) %>%
    summarise(S_VLR_PROD_LIQ = sum(as.numeric(VLR_PROD_LIQ)),
              S_VLR_LIQ = sum(as.double(VLR_LIQ)),
              S_VLR_LIQ_TAX = sum(as.numeric(VLR_LIQ_TAX)))


# todos os valores acima batem com a planilha base fornecida com dados até 04/2015
# última vez que foi gerada, segundo Rafael

# --------------------------------------------------------------------------
# para rodar f_PNL para testes abaixo, usado anoMesDia = "20150430"
#---------------------------------------------------------------------------
# comparar df_riscbgn_base_carteira gerado aqui com o lido do sas (ler aqui)
# comparar valores sumarizados
# --------------------------------------------------------------------------

sasDir <- "C:/MIS Asserth/migracao/consignado/arquivos SAS para homologar/"
filein <- paste0(sasDir,"riscbgn_base_carteira20150430.csv")
df_base_carteira_sas <- read.csv(filein, sep = ",")

df_base_carteira_sas %>%
    group_by(CD_CRTR) %>%
    summarise(S_MT_BRUTO_PRAZO = sum(as.numeric(MT_BRUTO_PRAZO)),
              #S_MT_BRUTO_PRAZO_JUROS = sum(as.numeric(MT_BRUTO_PRAZO_JUROS)),
              #S_SLD_CARTEIRA = sum(as.numeric(SLD_CARTEIRA)),
              S_JUROS = sum(as.numeric(JUROS)),
              S_FUNDING = sum(as.numeric(FUNDING)), # com diferença pequena! possível problema de precisão. Não usado no momento
              S_SLD_PREJUIZO = sum(as.numeric(SLD_PREJUIZO)),
              S_PDD = sum(as.numeric(PDD))) # # com diferença pequena! possível problema de precisão. Não usado no momento

df_riscbgn_base_carteira %>%
    group_by(CD_CRTR) %>%
    summarise(S_MT_BRUTO_PRAZO = sum(as.numeric(MT_BRUTO_PRAZO)),
              #S_MT_BRUTO_PRAZO_JUROS = sum(as.numeric(MT_BRUTO_PRAZO_JUROS)),
              #_SLD_CARTEIRA = sum(as.numeric(SLD_CARTEIRA)),
              S_JUROS = sum(as.numeric(JUROS)),
              S_FUNDING = sum(as.numeric(FUNDING)),
              S_SLD_PREJUIZO = sum(as.numeric(SLD_PREJUIZO)),
              S_PDD = sum(as.numeric(PDD)))

# nos testes acima, grupamento por FAIXA_REPORT ok, ATRASO ok, CD_CRTR ok
# valores batem com gerados no SAS para riscbgn.base.carteira
# para fundings e pdd, existe pequena diferença que pode ser por
# arredondamento nos cálculos das taxas

# --------------------------------------------------------------------------
# testando contra planilha base para colunas que vem de df_riscbgn_base_carteira
# antes de fazer merge com mesma base do mês anterior
# --------------------------------------------------------------------------
df_riscbgn_base_carteira %>%
    group_by(DT_REF,FAIXA_REPORT) %>%
    summarise(S_QTD_PRODUCAO = sum(MT_BRUTO_PRAZO), # ok
              S_MT_BRUTO_PRAZO_JUROS = sum(MT_BRUTO_PRAZO_JUROS), # arredondando centavos. idem
              S_MT_BRUTO_PRAZO = sum(MT_BRUTO_PRAZO), # arredondando centavos neste sumarização! dados discriminados ok!!
              S_SLD_CARTEIRA = sum(SLD_CARTEIRA))

df_base_carteira_sas %>%
    group_by(FAIXA_REPORT) %>%
    summarise(S_QTD_PRODUCAO = sum(MT_BRUTO_PRAZO), # ok
              S_MT_BRUTO_PRAZO_JUROS = sum(MT_BRUTO_PRAZO_JUROS), # arredondando centavos. idem
              S_MT_BRUTO_PRAZO = sum(MT_BRUTO_PRAZO), # arredondando centavos neste sumarização! dados discriminados ok!!
              S_SLD_CARTEIRA = sum(SLD_CARTEIRA))

# teste acima dando diferença não muito grande quando sumarizado por FAIXA_REPORT
# para 201504 para base R testando com base sas, resultados batem!!!!

# --------------------------------------------------------------------------
# testando contra planilha base para colunas que vem de df_riscbgn_base_carteira
# após de fazer merge com mesma base do mês anterior
# --------------------------------------------------------------------------
df_nova_base_risco_fim %>%
    group_by(DT_REF,FAIXA_REPORT) %>%
    summarise(S_QTD_PRODUCAO = sum(MT_BRUTO_PRAZO), # ok
              S_MT_BRUTO_PRAZO_JUROS = sum(MT_BRUTO_PRAZO_JUROS),
              S_MT_BRUTO_PRAZO_JUROS_ANT = sum(MT_BRUTO_PRAZO_JUROS_ANT))

# teste acima dando diferença não muito grande quando sumarizado por FAIXA_REPORT
# para 201504 para base R testando com base sas, resultados batem!!!!

# --------------------------------------------------------------------------
# testando contra planilha base para colunas que vem de df_riscbgn_cubo_carteira
# após de fazer as primeira chamada a f_PNL
# --------------------------------------------------------------------------
df_riscbgn_cubo_carteira %>%
    group_by(DT_REF) %>%
    summarise(S_MT_BRUTO_PRAZO_JUROS = sum(MT_BRUTO_PRAZO_JUROS), # arredondando centavos. idem
              S_MT_BRUTO_PRAZO = sum(MT_BRUTO_PRAZO), # arredondando centavos neste sumarização! dados discriminados ok!!
              S_SLD_CARTEIRA = sum(SLD_CARTEIRA))

df_riscbgn_base_carteira %>%
    group_by(FAIXA_REPORT) %>%
    summarise(S_MT_BRUTO_PRAZO_JUROS = sum(MT_BRUTO_PRAZO_JUROS), # arredondando centavos. idem
              S_MT_BRUTO_PRAZO = sum(MT_BRUTO_PRAZO), # arredondando centavos neste sumarização! dados discriminados ok!!
              S_SLD_CARTEIRA = sum(SLD_CARTEIRA))



# FINAL!!!
# em conversa com rafael, identificamos que a diferença que estava dando nas colunas de carteira:
# MT_BRUTO_PRAZO_JUROS, MT_BRUTO_PRAZO e SLD_CARTEIRA em relação a planilha base se devem a que
# na minha pesquisa entrava tb CD_PRDT (produto) automóvel, que não saia na versão anterior
# ele pediu que deixe saindo. Então todas as colunas usadas até agora estão ok!!!!!

# Estratégia final

# esboçar entendimento de negócio dos dados para cada sql feito
# e colocar nos comentários junto aos sqls!

