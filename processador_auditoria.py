import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ── Mapeamento de filiais (igual ao Power Query / get_df_empresas) ─────────────
MAPA_FILIAIS = {
    "Tools 00":     "Tools - Matriz",
    "Tools 01":     "Tools - Filial",
    "Maquinas 00":  "Maquinas - Matriz",
    "Maquinas 01":  "Maquinas - Filial",
    "Maquinas 02":  "Maquinas - Jundiai",
    "Robotica 00":  "Robotica - Matriz",
    "Robotica 01":  "Robotica - Jaragua",
    "Service 01":   "Service - Matriz",
    "Service 02":   "Service - Filial",
    "Service 03":   "Service - Caxias",
    "Service 04":   "Service - Jundiai",
}


def _limpar_codigo(serie: pd.Series, zfill: int) -> pd.Series:
    return (
        serie.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.zfill(zfill)
    )


def _ler_wms(arquivo) -> pd.DataFrame:
    """
    Lê o Excel do WMS.
    Espera colunas: Empresa, Filial, Produto, Armazem, Localização, Descrição,
                    Saldo Atual (por localização), C Unitario
    """
    df = pd.read_excel(arquivo)
    df.columns = df.columns.str.strip()

    # Renomeia variações comuns de nome
    renomes = {
        "C Unitario": "Vl Unit",
        "C_Unitario": "Vl Unit",
        "Vlr.Final":  "Vl Total WMS",
        "Saldo Atual": "Saldo WMS",
    }
    df = df.rename(columns={k: v for k, v in renomes.items() if k in df.columns})

    # Garante numéricos
    for col in ["Saldo WMS", "Vl Unit", "Vl Total WMS"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Filtra saldo > 0
    if "Saldo WMS" in df.columns:
        df = df[df["Saldo WMS"] > 0].copy()

    # Normaliza códigos
    if "Produto" in df.columns:
        df["Produto"] = _limpar_codigo(df["Produto"], 6)
    if "Filial" in df.columns:
        df["Filial"] = _limpar_codigo(df["Filial"], 2)
    if "Armazem" in df.columns:
        df["Armazem"] = _limpar_codigo(df["Armazem"], 2)

    # Resolve nome da filial via mapeamento
    if "Empresa" in df.columns and "Filial" in df.columns:
        chave = df["Empresa"].astype(str).str.strip() + " " + df["Filial"].astype(str).str.strip()
        df["Filial"] = chave.map(MAPA_FILIAIS).fillna(chave)
        df = df.drop(columns=["Empresa"], errors="ignore")

    # Remove duplicatas de chave (Filial + Produto + Armazem + Localização)
    subset = [c for c in ["Filial", "Produto", "Armazem", "Localização"] if c in df.columns]
    df = df.drop_duplicates(subset=subset)

    return df


def _ler_erp(arquivo) -> pd.DataFrame:
    """
    Lê o Excel do ERP.
    Espera colunas: Empresa, Filial, Produto, Armazem, Descrição, Saldo Atual, C Unitario
    O ERP traz UMA linha por produto (saldo total, sem localização).
    """
    df = pd.read_excel(arquivo)
    df.columns = df.columns.str.strip()

    renomes = {
        "C Unitario":  "Vl Unit",
        "C_Unitario":  "Vl Unit",
        "Saldo Atual": "Saldo ERP",
        "Vlr.Final":   "Vl Total ERP",
    }
    df = df.rename(columns={k: v for k, v in renomes.items() if k in df.columns})

    for col in ["Saldo ERP", "Vl Unit", "Vl Total ERP"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "Produto" in df.columns:
        df["Produto"] = _limpar_codigo(df["Produto"], 6)
    if "Filial" in df.columns:
        df["Filial"] = _limpar_codigo(df["Filial"], 2)
    if "Armazem" in df.columns:
        df["Armazem"] = _limpar_codigo(df["Armazem"], 2)

    if "Empresa" in df.columns and "Filial" in df.columns:
        chave = df["Empresa"].astype(str).str.strip() + " " + df["Filial"].astype(str).str.strip()
        df["Filial"] = chave.map(MAPA_FILIAIS).fillna(chave)
        df = df.drop(columns=["Empresa"], errors="ignore")

    # ERP não tem localização — remove duplicatas por Filial+Produto+Armazem
    subset = [c for c in ["Filial", "Produto", "Armazem"] if c in df.columns]
    df = df.drop_duplicates(subset=subset)

    return df


def cruzar_wms_erp(arquivo_wms, arquivo_erp) -> pd.DataFrame:
    """
    Replica a lógica do Power Query:

    1. WMS  → N linhas por produto (uma por localização), saldo real por local
    2. ERP  → 1 linha por produto, saldo total
    3. Join por Filial + Produto + Armazem
    4. Agrupamento → Total_WMS, Total_ERP, Qtd_Locais
    5. Status  : OK se Total_WMS == Total_ERP, senão Divergente
    6. Rateado : se OK → Saldo WMS (espelho visual perfeito)
                 se Divergente → Total_ERP / Qtd_Locais
    7. Divergência = Saldo WMS - Saldo ERP (Rateado)
    8. Vl Divergência = Divergência * Vl Unit
    """
    df_wms = _ler_wms(arquivo_wms)
    df_erp = _ler_erp(arquivo_erp)

    if df_wms.empty:
        logger.warning("cruzar_wms_erp: arquivo WMS vazio ou sem dados válidos.")
        return pd.DataFrame()
    if df_erp.empty:
        logger.warning("cruzar_wms_erp: arquivo ERP vazio ou sem dados válidos.")
        return pd.DataFrame()

    # ── Join WMS ← ERP por Filial + Produto + Armazem ─────────────────────
    chaves = [c for c in ["Filial", "Produto", "Armazem"] if c in df_wms.columns and c in df_erp.columns]

    # Colunas do ERP que entram no merge (evita colisão com WMS)
    cols_erp = chaves + [c for c in ["Saldo ERP", "Vl Unit", "Vl Total ERP", "Descrição"]
                         if c in df_erp.columns and c not in df_wms.columns]
    # Descrição prefere WMS; se WMS não tiver, usa ERP
    if "Descrição" in df_wms.columns:
        cols_erp = [c for c in cols_erp if c != "Descrição"]

    df = df_wms.merge(df_erp[cols_erp], on=chaves, how="left")

    # Vl Unit: prioriza ERP se não veio do WMS
    if "Vl Unit_x" in df.columns:
        df["Vl Unit"] = df["Vl Unit_x"].fillna(df.get("Vl Unit_y", 0))
        df = df.drop(columns=["Vl Unit_x", "Vl Unit_y"], errors="ignore")

    # Saldo ERP nulo → 0 (produto existe no WMS mas não no ERP)
    df["Saldo ERP"] = pd.to_numeric(df.get("Saldo ERP", 0), errors="coerce").fillna(0)

    # ── Agrupamento por Filial + Produto + Armazem ────────────────────────
    grp = df.groupby(chaves, as_index=False).agg(
        Total_WMS  =("Saldo WMS", "sum"),
        Total_ERP  =("Saldo ERP", "max"),   # ERP repete o total em todas as linhas
        Qtd_Locais =("Saldo WMS", "count"),
    )

    df = df.merge(grp, on=chaves, how="left")

    # ── Status ────────────────────────────────────────────────────────────
    df["Status"] = np.where(df["Total_WMS"] == df["Total_ERP"], "OK", "Divergente")

    # ── Saldo ERP (Rateado) ───────────────────────────────────────────────
    # OK        → espelha o Saldo WMS (visual limpo: cada local mostra o que tem)
    # Divergente → Total_ERP ÷ Qtd_Locais (rateio igual entre locais)
    df["Saldo ERP (Rateado)"] = np.where(
        df["Status"] == "OK",
        df["Saldo WMS"],
        (df["Total_ERP"] / df["Qtd_Locais"]).round(2),
    )

    # ── Divergência por localização ───────────────────────────────────────
    df["Divergência"] = np.where(
        df["Status"] == "OK",
        0,
        (df["Saldo WMS"] - df["Saldo ERP (Rateado)"]).round(4),
    )

    # ── Valores financeiros ───────────────────────────────────────────────
    vl_unit = pd.to_numeric(df.get("Vl Unit", 0), errors="coerce").fillna(0)
    df["Vl Divergência"] = (df["Divergência"] * vl_unit).round(2)
    df["Vl Total ERP"]   = (df["Total_ERP"]   * vl_unit).round(2)

    # ── Coluna Empresa (extraída da Filial para compatibilidade com o app) ─
    df["Empresa"] = df["Filial"].str.split(" - ").str[0]

    # ── Organização final ─────────────────────────────────────────────────
    colunas_finais = [
        "Status", "Empresa", "Filial", "Localização", "Armazem",
        "Produto", "Descrição",
        "Saldo ERP (Total)",        # = Total_ERP — saldo real do produto
        "Saldo ERP (Rateado)",      # = rateado por localização
        "Saldo WMS",                # = saldo físico por localização
        "Divergência",
        "Vl Unit",
        "Vl Divergência",
        "Vl Total ERP",
        "Qtd_Locais",
    ]

    # Renomeia Total_ERP para o nome final
    df = df.rename(columns={"Total_ERP": "Saldo ERP (Total)"})

    # Garante só as colunas que existem
    colunas_ok = [c for c in colunas_finais if c in df.columns]
    df = df[colunas_ok].reset_index(drop=True)

    logger.info(
        "cruzar_wms_erp: %d linhas geradas (%d produtos únicos).",
        len(df), df["Produto"].nunique()
    )
    return df
