# verifica se base carteira já gerada no Oracle 
# parâmetro: anoMesdia de processamento no formato AAAAMMDD
# retorna FALSE se vazio, TRUE se jã com conteúdo para o mês
f_checaBaseCart <- function(anoMesdia) {
  # conexao Oracle
  caminho <- "DWCTLPRD"
  userid <- "usr_pbgn_ltra"
  passwd <- "usr_pbgn_ltra"
  
  # abre conexão com Oracle
  channel <- odbcConnect(caminho,uid=userid, pwd=passwd, believeNRows=FALSE)
  #   seleciona para id_ultm_dia_mes igual ao último dia do mês de referência processado
  cSQL_cart <- paste0("select count(*)",
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
  
  # a consulta abaixo roda em média em 7 minutos no Windows Cetelem 32b
  df <- sqlQuery(channel,cSQL_cart, errors = TRUE)

  # fecha conexão Oracle                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        # fecha conexão com Oracle ao sair
  odbcClose(channel)
  if(df[1,1] == 0)
    return(FALSE)
  else
    return(TRUE)
  
}