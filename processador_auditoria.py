import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ── Mapeamento empresa+cod → nome completo (igual ao processador_movs) ────────
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


def _limpar_produto(serie):
    return (
        serie.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.zfill(6)
    )


def _limpar_cod(serie, digitos=2):
    return (
        serie.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.zfill(digitos)
    )


def _extrair_armazem(serie):
    """
    Extrai armazém da Localização WMS.
    Padrão: 'A01.C01.P01.N01' → '01'
    """
    return (
        serie.astype(str)
        .str.extract(r"^A(\d+)", expand=False)
        .str.zfill(2)
        .fillna("01")
    )


def _ler_wms(arquivo):
    """
    Lê Excel do WMS.
    Colunas esperadas: Empresa\Filial | Localização | Produto | Descrição |
                       Capacidade | Utilizado | Disponível
    - Saldo WMS = Disponível
    - Armazem   = extraído da Localização (A01... → 01)
    - Filial    = nome após primeiro traço de 'Empresa\Filial'
                  ex: '01-Tools - Filial' → 'Tools - Filial'
    """
    df = pd.read_excel(arquivo)
    df.columns = df.columns.str.strip()

    # Renomeia coluna combinada empresa/filial
    for col in list(df.columns):
        if "Empresa" in col and "Filial" in col:
            df = df.rename(columns={col: "Filial_Raw"})
            break

    # Saldo WMS = Utilizado (quantidade física na localização)
    for nome in ["Utilizado"]:
        if nome in df.columns:
            df = df.rename(columns={nome: "Saldo WMS"})
            break

    for col in ["Saldo WMS", "Capacidade", "Utilizado"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df[df["Saldo WMS"] > 0].copy()
    if df.empty:
        return pd.DataFrame()

    if "Produto" in df.columns:
        df["Produto"] = _limpar_produto(df["Produto"])

    for nome in ["Localização", "Localizacao"]:
        if nome in df.columns:
            if nome != "Localização":
                df = df.rename(columns={nome: "Localização"})
            df["Armazem"] = _extrair_armazem(df["Localização"])
            break

    if "Filial_Raw" in df.columns:
        # '01-Tools - Filial' → 'Tools - Filial'
        df["Filial"] = df["Filial_Raw"].astype(str).str.split("-", n=1).str[1].str.strip()
        df = df.drop(columns=["Filial_Raw"])

    df["Empresa"] = df["Filial"].str.split(" - ").str[0].str.strip()

    subset = [c for c in ["Filial", "Produto", "Armazem", "Localização"] if c in df.columns]
    df = df.drop_duplicates(subset=subset)
    return df


def _ler_erp(arquivo):
    """
    Lê Excel do ERP.
    Estrutura: 3 abas (Tools, Service, Maquinas) — nome da aba = empresa
    Colunas: Filial | Produto | Armazem | Descrição | Saldo Atual | C Unitario | Vlr.Final
    - Filial = código numérico ('01', '02'...) → resolvido pelo MAPA_FILIAIS
    """
    xls = pd.ExcelFile(arquivo)
    abas = []
    for sheet in xls.sheet_names:
        df_aba = pd.read_excel(xls, sheet_name=sheet)
        df_aba.columns = df_aba.columns.str.strip()
        if df_aba.empty:
            continue
        df_aba["Empresa_Aba"] = sheet.strip()
        abas.append(df_aba)

    if not abas:
        return pd.DataFrame()

    df = pd.concat(abas, ignore_index=True)

    renomes = {
        "Saldo Atual": "Saldo ERP",
        "C Unitario":  "Vl Unit",
        "C_Unitario":  "Vl Unit",
        "Vlr.Final":   "Vl Total ERP",
    }
    df = df.rename(columns={k: v for k, v in renomes.items() if k in df.columns})

    for col in ["Saldo ERP", "Vl Unit", "Vl Total ERP"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df[df["Saldo ERP"] > 0].copy()

    if "Produto" in df.columns:
        df["Produto"] = _limpar_produto(df["Produto"])
    if "Filial" in df.columns:
        df["Filial_Cod"] = _limpar_cod(df["Filial"], 2)
    if "Armazem" in df.columns:
        df["Armazem"] = _limpar_cod(df["Armazem"], 2)

    df["Chave"] = df["Empresa_Aba"].str.strip() + " " + df["Filial_Cod"].str.strip()
    df["Filial"] = df["Chave"].map(MAPA_FILIAIS).fillna(df["Chave"])
    df["Empresa"] = df["Filial"].str.split(" - ").str[0].str.strip()
    df = df.drop(columns=["Chave", "Filial_Cod", "Empresa_Aba"], errors="ignore")

    subset = [c for c in ["Filial", "Produto", "Armazem"] if c in df.columns]
    df = df.drop_duplicates(subset=subset)
    return df


def cruzar_wms_erp(arquivo_wms, arquivo_erp):
    """
    Cruza WMS x ERP aplicando a lógica de rateio do Power Query:

    1. WMS  → N linhas por produto (uma por localização)
    2. ERP  → 1 linha por produto+armazem (saldo total)
    3. Join por Filial + Produto + Armazem
    4. Agrupa → Total_WMS, Total_ERP, Qtd_Locais
    5. Status      : OK se Total_WMS == Total_ERP
    6. ERP Rateado : OK → espelha Saldo WMS | Divergente → Total_ERP / Qtd_Locais
    7. Divergência : Saldo WMS - ERP Rateado
    """
    df_wms = _ler_wms(arquivo_wms)
    df_erp = _ler_erp(arquivo_erp)

    if df_wms.empty:
        raise ValueError("WMS sem dados válidos. Verifique o arquivo.")
    if df_erp.empty:
        raise ValueError("ERP sem dados válidos. Verifique o arquivo.")

    chaves = [c for c in ["Filial", "Produto", "Armazem"]
              if c in df_wms.columns and c in df_erp.columns]

    cols_erp_merge = chaves + [c for c in ["Saldo ERP", "Vl Unit", "Vl Total ERP"]
                                if c in df_erp.columns]
    if "Descrição" not in df_wms.columns and "Descrição" in df_erp.columns:
        cols_erp_merge.append("Descrição")

    df = df_wms.merge(df_erp[cols_erp_merge], on=chaves, how="left")

    if "Vl Unit_x" in df.columns:
        df["Vl Unit"] = df["Vl Unit_x"].fillna(df.get("Vl Unit_y", 0))
        df = df.drop(columns=["Vl Unit_x", "Vl Unit_y"], errors="ignore")

    df["Saldo ERP"] = pd.to_numeric(df.get("Saldo ERP", 0), errors="coerce").fillna(0)
    df["Vl Unit"]   = pd.to_numeric(df.get("Vl Unit",   0), errors="coerce").fillna(0)

    grp = df.groupby(chaves, as_index=False).agg(
        Total_WMS  =("Saldo WMS", "sum"),
        Total_ERP  =("Saldo ERP", "max"),
        Qtd_Locais =("Saldo WMS", "count"),
    )
    df = df.merge(grp, on=chaves, how="left")

    df["Status"] = np.where(
        df["Total_WMS"].round(4) == df["Total_ERP"].round(4),
        "OK", "Divergente"
    )

    df["Saldo ERP (Rateado)"] = np.where(
        df["Status"] == "OK",
        df["Saldo WMS"],
        (df["Total_ERP"] / df["Qtd_Locais"]).round(2),
    )

    df["Divergência"] = np.where(
        df["Status"] == "OK",
        0,
        (df["Saldo WMS"] - df["Saldo ERP (Rateado)"]).round(4),
    )

    df["Vl Divergência"] = (df["Divergência"] * df["Vl Unit"]).round(2)
    df["Vl Total ERP"]   = (df["Total_ERP"]   * df["Vl Unit"]).round(2)

    df = df.rename(columns={
        "Total_ERP":  "Saldo ERP (Total)",
        "Qtd_Locais": "Qtd Locais",
    })
    df = df.drop(columns=["Total_WMS", "Saldo ERP"], errors="ignore")

    ordem = [
        "Status", "Empresa", "Filial", "Localização", "Armazem",
        "Produto", "Qtd Locais", "Descrição", "Vl Unit",
        "Saldo ERP (Total)", "Saldo ERP (Rateado)", "Saldo WMS",
        "Divergência", "Vl Divergência", "Vl Total ERP",
    ]
    colunas_ok = [c for c in ordem if c in df.columns]
    resto = [c for c in df.columns if c not in colunas_ok]
    df = df[colunas_ok + resto].reset_index(drop=True)

    logger.info(
        "cruzar_wms_erp: %d linhas | %d produtos | %d divergentes",
        len(df), df["Produto"].nunique(), int((df["Status"] == "Divergente").sum()),
    )
    return df
