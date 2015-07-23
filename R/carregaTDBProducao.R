#'  script para rodar TDB Produção
#'  quando: dia primeiro do mês seguinte ao de referência
#'  descrição: cria pasta para o mês corrente na pasta tidydata,
#'  carrega pasta com arquivos gerados a partir dos arquivos raw na pasta rawdata.
#'  Para relatório de Produção de Cartões (aba CONSIGNADO na planilha TDB Produção), 
#'  chama função f_SICLID_raw.
#'  Para relatório de consignado (aba CONSIGNADO na planilha TDB Produção),
#'  chama função f_consignado_raw.
#'  Antes de iniciar a execução, deve ser mudada a data do atributo "dataproc" abaixo,
#'  para a data do último dia do mês a ser processado no formato AAAAMMDD. 
#'  

# ATENÇÃO: data de processamento deve ser trocada abaixo a cada mês
# ----------------------
dataproc <- "20150630"
# ----------------------

# source scripts necessários
source("./R/f_leCsvCARPROD_raw.R")
source("./R/f_SICLID_raw.R")
source("./R/f_consignado_raw.R")

# verifica e prepara ambiente de libraries necessárias
if (!requireNamespace("openxlsx", quietly = TRUE)) {
  stop("openxlsx needed for this function to work. Please install it.",
       call. = FALSE)
} 
if(!require(openxlsx)){install.packages("openxlsx")}

if (!requireNamespace("dplyr", quietly = TRUE)) {
  stop("dplyr needed for this function to work. Please install it.",
       call. = FALSE)
}
if(!require(RODBC)){install.packages("RODBC")}
if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("dplyr needed for this function to work. Please install it.",
         call. = FALSE)
}
if(!require(RODBC)){install.packages("RODBC")}
 
# constantes
rawDir <- "./rawdata" # pasta com arquivos brutos (a partir da pasta de trabalho)
tidyDir <- "./tidydata" # pasta com arquivos processados (a partir da pasta de trabalho)
dtproc <- paste0(substr(dataproc,5,6),".",substr(dataproc,1,4))

# checa se diretorio raw já carregado para o mês, no formato "MM.AAAA"
if (!file.exists(paste0(rawDir,"/",dtproc))){
  stop("Diretório com arquivos brutos de origem do mês não está presente.",
       call. = FALSE)
}  

# criar diretório do mês no diretório tidydata, caso não exista
if (!file.exists(paste0(tidyDir,"/",dtproc))){
  dir.create(file.path(tidyDir, dtproc))
} 

# criar os arquivos processados para alimentar planilha TDB Produção, aba CARD
f_SICLID_raw(dataproc)

# criar os arquivos processados para alimentar planilha TDB Produção, aba CONSIGNADO
f_consignado_raw(dataproc)

# plota arquivos para checagem inicial
#plot(ll[[1]]$NBFI, type = "l")
