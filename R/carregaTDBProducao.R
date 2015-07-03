#'  script para rodar TDB Produção
#'  quando: dia primeiro do mês seguinte ao de referência
#'  descrição: cria pasta para o mês corrente na pasta tidydata,
#'  carrega pasta com arquivos gerados a partir dos arquivos raw na pasta rawdata
#'  chama função f_SICLID_raw passando como parâmetro a data do último dia do
#'  mês a ser processado no formato AAAAMMDD. Mudar a constante abaixo antes de processar
#'  

# ATENçÂO: data de processamento
dataproc <- "20150630"

# source scripts necessários
source("./R/f_leCsvCARPROD_raw.R")
source("./R/f_SICLID_raw.R")
# verifica e prepara ambiente de libraries necessárias
if (!requireNamespace("openxlsx", quietly = TRUE)) {
  stop("openxlsx needed for this function to work. Please install it.",
       call. = FALSE)
} else {
  if(!require(openxlsx)){install.packages("openxlsx")}
}  
if (!requireNamespace("dplyr", quietly = TRUE)) {
  stop("dplyr needed for this function to work. Please install it.",
       call. = FALSE)
} else {
  if(!require(dplyr)){install.packages("dplyr")}
}  
# constants
rawDir <- "C:/MISAsserth/TESTE/MISCTLM/rawdata"
tidyDir <- "C:/MISAsserth/TESTE/MISCTLM/tidydata"
dtproc <- paste0(substr(dataproc,5,6),".",substr(dataproc,1,4))

# chegar se diretorio raw já carregado para o mês
if (!file.exists(paste0(rawDir,"/",dtproc))){
  stop("Diretório com arquivos origem do mês não está presente.",
       call. = FALSE)
}  
# criar diretório do mês no diretório tidydata
# caso não exista cira. 
# caso exista executa carga    
if (!file.exists(paste0(tidyDir,"/",dtproc))){
  dir.create(file.path(tidyDir, dtproc))
} 
# criar os arquivos tidy para alimentar planilha TDB Produção
f_SICLID_raw(dataproc)

# plota arquivos para checagem inicial
#plot(ll[[1]]$NBFI, type = "l")
