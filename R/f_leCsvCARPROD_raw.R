#' OBJETIVO:
#' ler arquivos csv de geração de carprod de acordo com mês de processamento
#' Passos:
#' 1.forçando todos os nomes das colunas em maiusculo e tocando chainefin por chaineori
#' 2.inserindo coluna de tipo da tabela para cada xx_ano e DadosAmort
#' 3.gravar cada tabela gerada em arquivo csv diferente com raw data
#' obs: para ler os arquivos de entrada padronizar nomes gerados para AAAAMM para todos
#' obs2. manter os arquivos dos meses anteriores no ano nesta pasta"
#' entrada: 
#'          filein - path e parte do nome do arquivo de entrada sem o mes
#'          head - variável lójica indicando se arquivo lido tem ou não nomes das variáveis
#'                  na primeira linha
########################
f_leCsvCARPROD_raw <- function(filein, head)
{
  out <- tryCatch(
  {  
    # libraries
    # verifica e prepara ambiente de libraries necessárias
    #if (!requireNamespace("openxlsx", quietly = TRUE)) {
    #  stop("openxlsx needed for this function to work. Please install it.",
    #     call. = FALSE)
    #} else {
    #  if(!require(openxlsx)){install.packages("openxlsx")}
    #}  
    #if (!requireNamespace("dplyr", quietly = TRUE)) {
    #  stop("dplyr needed for this function to work. Please install it.",
    #       call. = FALSE)
    #} else {
    #  if(!require(dplyr)){install.packages("dplyr")}
    #}  
      

    # ler arquivos de mes passado como parâmetro
    df <- read.csv(filein, header = head, sep = ";")
    # reading all the monthly sheets into a list of data frames
    #l_meses <- list()
    #j <- 0
    #for (i in 1:mesproc) {
    #    j <- j + 1
    #    l_meses[[j]] <- read.csv(filein, header = TRUE)
    #}
    # merging all monthly data frames into one data frame
    #df <- do.call(rbind, l_meses)
    # forcing uppercase names
    names(df) <- toupper(names(df))
    # wrinting the csv file. for Windows Excel 2007/10
    return(df)
  },
  error = function(cond) {
    message("\n>>> ERRO! Geração dos arquivos RAW \n")
    message(cond)
    return(-1)
  },
  warning = function(cond) {
    message(paste0("\n>>> WARNING! Geração dos arquivos RAW \n", filein))
    message(cond)
    return(NULL)
  },
  finally = {
    message("\n>>> Processamento dos arquivos RAW finalizado")
  }
  )

  return(out)
}
