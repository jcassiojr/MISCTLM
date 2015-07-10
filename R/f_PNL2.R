# funcáo que implementa macro PNL2 de Consignado.sas
f_PNL2 <- function(anoMesDia, anoMesDia_ant)
{

# ------------- PASSO 21, 22
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
                   DS_PRDT_ANT,CD_CRTR_ANT,DS_CRTR_ANT,ID_FAIX_ATRS_ANT,PDD_ANT)


# --------------- PASSO 23
df_ricbgn_cubo_carteira <-
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

