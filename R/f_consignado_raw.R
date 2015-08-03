#' @description
#' funcao de carga dos dados de producao, liquidação e carteira de consignado
#'
#' @details
#' obtém os dados de consignado do mês corrente para ser acrescentado aos dados dos meses
#' anteriores na planilha TDB Produção
#' @param anoMesDia string com ano mês dia do último dia do mês de processamento no formato
#' AAAAMMDD
#' @return não se aplica
#' @examples
#' f_consignado_raw("20150331")
#' f_consignado_raw("20150430")
f_consignado_raw <- function(anoMesDia)
{
    # datas de processamento (mudar as duas abaixo a cada processamento)
    # anoMesDia <- "20150430" # último dia do mês de processamento. Mudar a cada mês
    #anoMesDia_ant <- "20150331" # último dia do mês anterior de processamento. Mudar a cada mês
    #anoMesDia_2ant <- "20150228" # último dia do segundo mês anterior de processamento. Mudar a cada mês
    
    
    #mm_aaaa_ant <- paste0(substr(anoMesDia_ant,5,6),".",substr(anoMesDia_ant,1,4))

    # caminhos e arquivos
    aaaamm <- substr(anoMesDia,1,6)
    mm_aaaa <- paste0(substr(anoMesDia,5,6),".",substr(anoMesDia,1,4))
    rawDir <- "./rawdata"
    tidyDir <- "./tidydata"
    fileout_cuboprod <- paste0(tidyDir,"/", mm_aaaa,"/", "riscbgn_cubo_producao_",anoMesDia, "_tidy.csv")
    fileout_cuboliqu <- paste0(tidyDir,"/", mm_aaaa,"/", "riscbgn_cubo_liquidacao_",anoMesDia, "_tidy.csv")
    fileout_basecart <- paste0(tidyDir,"/", mm_aaaa,"/", "riscbgn_base_carteira_",anoMesDia, "_tidy.csv")

    # conexao Oracle
    caminho <- "DWCTLPRD"
    userid <- "usr_pbgn_ltra"
    passwd <- "usr_pbgn_ltra"

    # conexao Oracle
    # if(!require(RODBC)){install.packages("RODBC")}
    # if(!require(dplyr)){install.packages("dplyr")}

    # abre conexão com Oracle
    channel <- odbcConnect(caminho,uid=userid, pwd=passwd, believeNRows=FALSE)

    # ----------------------------------------------
    # TABELA PRODUCAO
    # ----------------------------------------------
    # ------------- PASSO 1
    # cria tabela temp.producao a partir do Oracle, segundo pesquisa sas já existente
    # em consignado.sas
    # Descrição:
    # seleciona a partir das tabelas de fatos de Operações Mensais e Operações Realizadas
    # os valores de montante de saldos contábeis não ???
    # Também obtém as seguinte dimensões para poder realizar as sumarizações e 
    # filtragens:
    #   tb_dim_grpo_prmt (correspondente bancário)
    #   tb_dim_prdt (produto)
    #   tb_dim_epdr (empresa cliente?)
    #   tb_dim_flal (filial BGN)
    #   tb_dim_prmt (Parceiro?)
    #   tb_dim_crtr (tipo de consignado?)
    #   tb_dim_cnal_vnda (canal de venda)
    # Período selecionado:
    #   seleciona sempre para todas as datas bases acima de janeiro/2013
    
    cSQL_temp <- paste0("Select round(a11.dt_base/100) as dt_base,",
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

    # executa a consulta
    # a consulta abaixo roda em média em 6 minutos no Windows Cetelem 32b
    df_temp_producao <- sqlQuery(channel,cSQL_temp, errors = TRUE)

    # fecha conexão com Oracle
    odbcClose(channel)

    # ------------- PASSO 2 (OK) Valores conferem com SAS
    # força para todos os registros:
    #    condição de ATRASO = "Em Dia"
    #    FAIXA REPORT = "R0"
    #    acumula ocorrências de código de consignado fora dos grupos como geral 99-CONSIGNADO
    df_temp_producao <-
        df_temp_producao %>%
        mutate (ATRASO = "Em Dia",
                FAIXA_REPORT = "R0",
                id_faix_atrs = -1,
                CD_CRTR = ifelse(!(CD_CRTR_TEMP %in% c(50,52,53,55)), 99, CD_CRTR_TEMP))

    # ------------ PASSO ADICIONAL PARA TEMP_PRODUCAO
    # troca vírgula por ponto em colunas selecionadas de data.frame
    df_temp_producao <- data.frame(lapply(df_temp_producao, function(x) gsub(",", ".", x, fixed = TRUE)), stringsAsFactors = FALSE)

    # ------------- PASSO 3 (OK) Valores conferem com SAS
    # cria tabela riscbgn.cubo_producao a partir da tabela temp.producao
    # entrada df_temp_producao
    # saida: df_riscbgn_cubo_producao
    # obs: para que a soma abaixo funcione é preciso gerar dataframe sem factors para
    # valores numéricos e usar cláusula as.numeric para transformar de character para numerico!!!
    df_riscbgn_cubo_producao <-
        df_temp_producao %>%
        select (DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
                DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,id_faix_atrs, QTD_PRODUCAO,
                VLR_PRODUCAO_BRUTA,VLR_PRODUCAO_BRUTA_SEM_TAX,VLR_IOF,
                VLR_PARCELA,VLR_FUTURO,VLR_PRODUCAO_LIQUIDA,VLR_PRODUCAO_REFIN,
                VLR_PRODUCAO_REFIN_SEM_TAX,MT_BRUTO_PRAZO_PRD,MT_BRUTO_PRAZO_JUROS_PRD) %>%
        group_by(DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
                DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,id_faix_atrs) %>%
        summarize(QTD_PRODUCAO = sum(as.numeric(QTD_PRODUCAO)),
                VLR_PRODUCAO_BRUTA = sum(as.numeric(VLR_PRODUCAO_BRUTA)),
                VLR_PRODUCAO_BRUTA_SEM_TAX = sum(as.numeric(VLR_PRODUCAO_BRUTA_SEM_TAX)),
                VLR_IOF = sum(as.numeric(VLR_IOF)),
                VLR_PARCELA = sum(as.numeric(VLR_PARCELA)),
                VLR_FUTURO = sum(as.numeric(VLR_FUTURO)),
                VLR_PRODUCAO_LIQUIDA = sum(as.numeric(VLR_PRODUCAO_LIQUIDA)),
                VLR_PRODUCAO_REFIN = sum(as.numeric(VLR_PRODUCAO_REFIN)),
                VLR_PRODUCAO_REFIN_SEM_TAX = sum(as.numeric(VLR_PRODUCAO_REFIN_SEM_TAX)),
                MT_BRUTO_PRAZO_PRD = sum(as.numeric(MT_BRUTO_PRAZO_PRD)),
                MT_BRUTO_PRAZO_JUROS_PRD = sum(as.numeric(MT_BRUTO_PRAZO_JUROS_PRD)))
    # filtrar somente o mês corrente para gerar a base para ser incrementada na base do TDB Produção
    df_riscbgn_cubo_producao <-
        df_riscbgn_cubo_producao %>%
            filter (DT_REF == aaaamm)
    
    # aqui gravar arquivo raw em riscbgn_cubo_producao_AAAAMMDD_raw.csv na pasta do mês de processamento
    # este arquivo deve ser incrementado aos dados da aba base usada na planilha TDB Produção
    write.csv2(df_riscbgn_cubo_producao, file = fileout_cuboprod)

    # remove bases temporarias
    rm(df_temp_producao, df_riscbgn_cubo_producao)
    # ----------------------------------------------
    # TABELA LIQUIDACAO
    # ----------------------------------------------

    # ------------- PASSO 4 (OK)
    # entrada: tabelas oracle
    # saida: df_temp_liquidacao
    # cria tabela temp.producao

    # abre conexao com oracle
    channel <- odbcConnect(caminho,uid=userid, pwd=passwd, believeNRows=FALSE)

    # cria tabela temp.liquidacao a partir do Oracle, segundo pesquisa sas já existente
    # em consignado.sas
    # Descrição:
    # seleciona a partir das tabelas de fatos de Movimentações Financeiras Realizadas
    # e Operações Realizadas os valores de montante de saldos contábeis não ???
    # Também obtém as seguinte dimensões para poder realizar as sumarizações e 
    # filtragens:
    #   tb_dim_hist_fncr (histórico financeiro)
    #   tb_dim_grpo_prmt (correspondente bancário)
    #   tb_dim_prdt (produto)
    #   tb_dim_epdr (empresa cliente?)
    #   tb_dim_flal (filial BGN)
    #   tb_dim_prmt (Parceiro?)
    #   tb_dim_crtr (tipo de consignado?)
    #   tb_dim_cnal_vnda (canal de venda)
    #   tb_dim_crdn (?)
    #   tb_dim_grnt_rgnl (?)
    #   tb_dim_grnt (?)
    #   tb_dim_oprd (?)
    # Período selecionado:
    #   seleciona sempre para todas as datas de carga acima de janeiro/2013
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
                   "round(t.dt_base/100) as dt_base,", # -- Extrai ano e mês (exemplo: 20140605 -> 201406)
                   "(round(r.dt_crga/10000) - round(t.dt_base/10000)) * 12 + mod(round(r.dt_crga/100),100) - mod(round(t.dt_base/100),100) as MOB,",
                   "sum(round(t.MT_OPRC/100,2)) as Producao,",
                   "DECODE (x.cd_hist_fncr, '104',sum(round(r.vl_mvmt/-100,2)),sum(round((r.vl_mvmt*t.qt_prcl*t.vl_taxa_ames)/ 10000,2))) as vl_mvmt_qtd_tx,",
                   "DECODE (x.cd_hist_fncr, '104',sum(round((r.vl_mvmt*t.qt_prcl*t.vl_taxa_ames)/-10000,2)),sum(round(r.vl_mvmt/ 100,2))) as vl_mvmt,",
                   "DECODE (x.cd_hist_fncr, '104',sum(round((r.vl_mvmt*t.vl_taxa_ames)/-10000,2)),sum(round((r.vl_mvmt*t.vl_taxa_ames)/10000,2))) as vl_mvmt_tx",
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

    # executa a consulta
    # a consulta abaixo roda em média em 17 minutos no Windows Cetelem 32b
    df_temp_liquidacao <- sqlQuery(channel,cSQL_liq, errors = TRUE)

    # fecha conexao com Oracle
    odbcClose(channel)

    # ------------- PASSO 5 (OK)
    # força para todos os registros:
    #    condição de ATRASO = "Em Dia"
    #    FAIXA REPORT = "R0"
    #    acumula ocorrências de código de consignado fora dos grupos como geral 99-CONSIGNADO
    df_temp_liquidacao <-
        df_temp_liquidacao %>%
        mutate (ATRASO = "Em Dia",
                FAIXA_REPORT = "R0",
                id_faix_atrs = -1,
                CD_CRTR = ifelse(!(CD_CRTR_TEMP %in% c(50,52,53,55)), 99, CD_CRTR_TEMP))

    # ------------ PASSO ADICIONAL PARA TEMP_LIQUIDACAO

    # muda vírgula para ponto decimal somente nos campos decimais
    df_temp_liquidacao <- data.frame(lapply(df_temp_liquidacao, function(x) gsub(",", ".", x, fixed = TRUE)), stringsAsFactors = FALSE)

    # ------------- PASSO 6 (OK) Valores conferem com SAS
    # entrada df_temp_liquidacao
    # saida: df_riscbgn_cubo_liquidacao
    # sumariza os valores de liquidação, taxa de liquidação e prazo de liquidação de produção por
    #    data de referência 
    #    data de processamento
    #    canal de venda
    #    empresa cliente (?)
    #    filial BGN
    #    correspondente bancário
    #    produto
    #    tipo de consignado (?)
    #    faixa de atraso
    #    faixa de report
    #    identificação da faixa de atraso
    #    MOB (?)
    df_riscbgn_cubo_liquidacao <-
        df_temp_liquidacao %>%
        select (DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
                DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,id_faix_atrs, MOB,
                PRODUCAO, VL_MVMT, VL_MVMT_TX) %>%
        group_by(DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
                DS_PRDT,DS_CRTR,ATRASO,FAIXA_REPORT,id_faix_atrs, MOB) %>%
        summarize(VLR_PROD_LIQ = sum(as.numeric(PRODUCAO)),
                VLR_LIQ = sum(as.numeric(VL_MVMT)),
                VLR_LIQ_TAX = sum(as.numeric(VL_MVMT_TX)),
                VLR_PROD_LIQ_PRAZO = sum(as.numeric(PRODUCAO)*as.numeric(MOB))) %>%
        rename(Prazo_med_liq = MOB)

    # filtrar somente o mês corrente para gerar a base para ser incrementada na base do TDB Produção
    df_riscbgn_cubo_liquidacao <-
        df_riscbgn_cubo_liquidacao %>%
            filter (DT_REF == aaaamm)
    
    # aqui gravar arquivo raw em riscbgn_cubo_liquidacao_AAAAMMDD_raw.csv na pasta do mês de processamento
    # este arquivo deve ser incrementado aos dados da aba base usada na planilha TDB Produção
    write.csv2(df_riscbgn_cubo_liquidacao, file = fileout_cuboliqu)

    # remove bases usadas
    rm(df_temp_liquidacao, df_riscbgn_cubo_liquidacao)

    # ----------------------------------------------
    # TABELA CARTEIRA
    # ----------------------------------------------

    # conecta ao Oracle
    channel <- odbcConnect(caminho,uid=userid, pwd=passwd, believeNRows=FALSE)
    
    # cria tabela riscbgn.base_carteira a partir do Oracle, segundo pesquisa sas já existente
    # em consignado.sas
    # Descrição:
    # seleciona a partir das tabelas de fatos de Operações mensais e
    # Operações Realizadas os valores de montante de saldos contábeis não ???
    # Também obtém as seguinte dimensões para poder realizar as sumarizações e 
    # filtragens:
    #   tb_dim_hist_fncr (histórico financeiro)
    #   tb_dim_grpo_prmt (correspondente bancário)
    #   tb_dim_prdt (produto)
    #   tb_dim_epdr (empresa cliente?)
    #   tb_dim_flal (filial BGN)
    #   tb_dim_prmt (Parceiro?)
    #   tb_dim_crtr (tipo de consignado?)
    #   tb_dim_cnal_vnda (canal de venda)
    # período selecionado:
    #   seleciona para id_ultm_dia_mes igual ao último dia do mês de referência processado
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

    # executa a consulta
    # a consulta abaixo roda em média em 7 minutos no Windows Cetelem 32b
    df_temp_carteira <- sqlQuery(channel,cSQL_cart, errors = TRUE)

    # fecha conexão Oracle                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        # fecha conexão com Oracle ao sair
    odbcClose(channel)

    # ------------ PASSO ADICIONAL PARA TEMP_CARTEIRA

    # muda vírgula para ponto decimal somente nos campos decimais
    df_temp_carteira <- data.frame(lapply(df_temp_carteira, function(x) gsub(",", ".", x, fixed = TRUE)), stringsAsFactors = FALSE)

    # -------------- PASSO 13 (OK)
    df_temp_carteira <-
        df_temp_carteira %>%
        arrange(ID_OPRC_RLZD) %>%
        mutate (SLD_CARTEIRA = 0,
                SLD_PREJUIZO = 0,
                MT_BRUTO_PRAZO = 0,
                MT_BRUTO_PRAZO_JUROS = 0,
                JUROS = 0,
                SLD_PREJUIZO = ifelse(ID_FAIX_ATRS == "10", as.numeric(MT_SLDO_CNTB_NAO_CDDA),0),
                SLD_CARTEIRA = ifelse(ID_FAIX_ATRS != "10", as.numeric(MT_SLDO_CNTB_NAO_CDDA),0),
                DT_REF=trunc(as.numeric(ID_ULTM_DIA_MES)/100),
                DT_BASE=trunc(as.numeric(DATA_BASE)/100),
                DT_INICIO_PGTO=trunc(as.numeric(DT_PRMR_VCTO)/100),
                NR_PRCL_PEND = as.numeric(QT_PRCL) - ((trunc(as.numeric(DT_REF)/100)-trunc(as.numeric(DT_INICIO_PGTO)/100))*12+((as.numeric(DT_REF) %% 100)-(as.numeric(DT_INICIO_PGTO) %% 100))),
                NR_PRCL_PEND = ifelse(NR_PRCL_PEND  < 0, 0, NR_PRCL_PEND),
                MT_BRUTO_PRAZO=(SLD_CARTEIRA*NR_PRCL_PEND),
                MT_BRUTO_PRAZO_JUROS=(SLD_CARTEIRA*NR_PRCL_PEND*as.numeric(VL_TAXA_AMES)),
                JUROS = ifelse(is.na(NR_DIAS_ATRS) | as.numeric(NR_DIAS_ATRS) == 0, SLD_CARTEIRA*as.numeric(VL_TAXA_AMES),JUROS),
                ATRASO = ifelse(is.na(NR_DIAS_ATRS) | as.numeric(NR_DIAS_ATRS) == 0, "EM DIA",
                            ifelse(as.numeric(NR_DIAS_ATRS) <= 30, "  1-30",
                            ifelse(as.numeric(NR_DIAS_ATRS) <= 59, " 31-59",
                            ifelse(as.numeric(NR_DIAS_ATRS) <= 90, " 60-90","  > 90")))),
                FAIXA_REPORT = ifelse(as.numeric(NR_DIAS_ATRS) <= 6, "R0",
                            ifelse(as.numeric(NR_DIAS_ATRS) >= 7 &  as.numeric(NR_DIAS_ATRS) <= 30, "R1",
                            ifelse(as.numeric(NR_DIAS_ATRS) >= 31 &  as.numeric(NR_DIAS_ATRS) <= 60, "R2",
                            ifelse(as.numeric(NR_DIAS_ATRS) >= 61 &  as.numeric(NR_DIAS_ATRS) <= 90, "R3",
                            ifelse(as.numeric(NR_DIAS_ATRS) >= 91 &  as.numeric(NR_DIAS_ATRS) <= 120, "R4",
                            ifelse(as.numeric(NR_DIAS_ATRS) >= 121 &  as.numeric(NR_DIAS_ATRS) <= 150, "R5",
                            ifelse(as.numeric(NR_DIAS_ATRS) >= 151 &  as.numeric(NR_DIAS_ATRS) <= 180, "R6","R7"))))))),
                CD_CRTR = ifelse(!(CD_CRTR_TEMP %in% c("50","52","53","55")), "99", CD_CRTR_TEMP)) %>% # caso codigo CRTR lido temporario nao for da faixa, fica 99
        select (-(DATA_BASE)) %>%
        arrange(DT_BASE,QT_PRCL)

    # ------------- PASSO 14 e 15 (OK) Não precisa no momento destes passos
    # f_funding(df_temp_carteira)

    # ------------- PASSO 16 (OK) Não precisa no momento destes passos
    # f_coefrisco(df_temp_carteira)

    # ------------- PASSO 17 (OK)

    df_riscbgn_base_carteira <-
        df_temp_carteira %>%
        select (ID_OPRC_RLZD, DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
                DS_PRDT,DS_CRTR,CD_CRTR, ATRASO,FAIXA_REPORT,ID_FAIX_ATRS,MT_BRUTO_PRAZO,
                MT_BRUTO_PRAZO_JUROS,SLD_CARTEIRA,JUROS,SLD_PREJUIZO) %>%
        group_by(ID_OPRC_RLZD, DT_REF,DT_BASE,DS_CNAL_VNDA,DS_EPDR,DS_FLAL,DS_GRPO_PRMT,
                 DS_PRDT,DS_CRTR,CD_CRTR, ATRASO,FAIXA_REPORT,ID_FAIX_ATRS) %>%
        summarize(MT_BRUTO_PRAZO = sum(MT_BRUTO_PRAZO),
                  MT_BRUTO_PRAZO_JUROS = sum(MT_BRUTO_PRAZO_JUROS),
                  SLD_CARTEIRA = sum(SLD_CARTEIRA),
                  JUROS = sum(JUROS),
                  SLD_PREJUIZO = sum(SLD_PREJUIZO))

    # aqui gravar arquivo raw em riscbgn_base_carteira_AAAAMMDD_raw.csv na pasta do mês de processamento
    # este arquivo deve ser incrementado aos dados da aba base usada na planilha TDB Produção
    write.csv2(df_riscbgn_base_carteira, file = fileout_basecart)

    # remove bases usadas antes de sair
    rm(df_temp_carteira, df_riscbgn_base_carteira, df_riscbgn_cubo_liquidacao, df_riscbgn_cubo_producao)

    # ----------- Fim do processamento consignado
}

