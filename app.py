import streamlit as st
import pandas as pd
import numpy as np
import io
from sqlalchemy import create_engine
from processador_movs import (
    tratar_notas_fiscais,
    buscar_movimentacoes_nuvem,
    remover_acentos,
    limpar_id_produto,
    limpar_id_geral,
    get_df_empresas,
)

# 1. Configuração da Página
st.set_page_config(page_title="Gestão Integrada I9", layout="wide")

# 2. CSS CUSTOMIZADO (CORES OFICIAIS #005562 e #EC6E21)
st.markdown(
    """
    <style>
    .stApp { background-color: #005562 !important; }
    [data-testid="stHeader"] { background-color: #005562 !important; }
    section[data-testid="stSidebar"] { display: block !important; background-color: #004550 !important; }

    .main-title { border-left: 6px solid #EC6E21; padding-left: 15px; color: #ffffff; font-weight: 700; font-size: 2.2rem; margin-bottom: 25px; }
    
    div[data-testid="stMetric"] { border: 2px solid #EC6E21 !important; background-color: #004550 !important; border-radius: 10px; padding: 15px; text-align: center; }
    div[data-testid="stMetricValue"] > div { color: #ffffff !important; font-size: 2.2rem !important; font-weight: 800 !important; }
    
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] { background-color: #004550 !important; color: white !important; border-radius: 5px 5px 0px 0px; padding: 10px 20px; }
    .stTabs [aria-selected="true"] { background-color: #EC6E21 !important; }

    div[data-testid="stRadio"] > div { background-color: #004550 !important; border: 1px solid #007687 !important; border-radius: 12px; padding: 8px 15px; gap: 15px; }
    div[data-testid="stRadio"] label, .stTextInput label { color: white !important; }
    .stTextInput input { background-color: #004550 !important; color: white !important; border: 1px solid #007687 !important; border-radius: 10px; }

    .stDownloadButton button { background-color: #EC6E21 !important; color: white !important; border: none !important; border-radius: 8px !important; font-weight: bold !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- FUNÇÕES DE APOIO ---
def para_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Planilha')
    return output.getvalue()

def to_float_br(serie):
    return pd.to_numeric(serie.astype(str).str.replace(r"[^\d,.-]", "", regex=True).str.replace(".", "", regex=False).str.replace(",", ".", regex=False), errors="coerce")

def estilizar_tabela(df):
    fmt_cols = {}
    # CORREÇÃO: Adicionada as colunas de Divergência e Total ERP na formatação R$
    for col in df.columns:
        if any(x in col for x in ["Saldo", "Divergência", "Qtd"]) and "Vl" not in col: 
            fmt_cols[col] = "{:,.2f}"
        elif any(x in col for x in ["Vl Unit", "Vl Total", "Preço", "Vl Divergência", "Vl Total ERP"]): 
            fmt_cols[col] = "R$ {:,.2f}"

    def colorir_linha(row):
        return ['background-color: #005562; color: #ffffff; font-size: 0.84rem;'] * len(row)

    def colorir_status(val):
        if val == "Divergente": return 'background-color: #722f1d; color: #ffffff; font-weight: bold; border: 1px solid #EC6E21;'
        if val == "OK": return 'background-color: #1a4a32; color: #b3ffcc; font-weight: bold;'
        return ''

    styled = df.style.apply(colorir_linha, axis=1)
    if "Status" in df.columns: styled = styled.applymap(colorir_status, subset=["Status"])
    
    styled = styled.set_table_styles([
        {'selector': 'thead th', 'props': [('background-color', '#004550'), ('color', '#ffffff'), ('border-bottom', '2px solid #EC6E21'), ('text-transform', 'uppercase')]},
        {'selector': 'td', 'props': [('padding', '8px 12px'), ('border-bottom', '1px solid rgba(255,255,255,0.05)')]}
    ])
    if fmt_cols: styled = styled.format(fmt_cols, na_rep="-")
    return styled

# --- CONEXÃO E CARGA ---
def get_engine():
    try: return create_engine(st.secrets["connections"]["postgresql"]["url"])
    except: return None

def carregar_do_banco(tabela):
    engine = get_engine()
    if engine is None: return None
    try: return pd.read_sql(f"SELECT * FROM {tabela}", engine)
    except: return None

def formatar_br(valor):
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# --- INTERFACE SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Atualizar Bases")
    with st.expander("1. Auditoria"):
        u_wms = st.file_uploader("WMS", type=["xlsx"])
        u_erp = st.file_uploader("ERP", type=["xlsx"])
        if u_wms and u_erp and st.button("🚀 Enviar"): st.success("Enviado!")
    with st.expander("2. Notas Fiscais"):
        u_movs = st.file_uploader("Notas", type=["xlsx"], accept_multiple_files=True)
        if u_movs and st.button("📦 Processar"): st.success("Processado!")

st.markdown('<div class="main-title">Gestão Integrada I9</div>', unsafe_allow_html=True)
df_base = carregar_do_banco("auditoria")

if df_base is not None:
    c1, c2, c3 = st.columns(3)
    with c1: f_emp = st.radio("🏢 Empresa", ["Todas"] + sorted(df_base["Empresa"].unique().tolist()), horizontal=True)
    df_t1 = df_base if f_emp == "Todas" else df_base[df_base["Empresa"] == f_emp]
    with c2:
        dict_filiais = {"Todas": "Todas"}
        for f in sorted(df_t1["Filial"].unique().tolist()): dict_filiais[f.split(" - ")[-1] if " - " in f else f] = f
        f_fil_curta = st.radio("📍 Filial", list(dict_filiais.keys()), horizontal=True)
        f_fil_longa = dict_filiais[f_fil_curta]
    df_t2 = df_t1 if f_fil_longa == "Todas" else df_t1[df_t1["Filial"] == f_fil_longa]
    with c3: f_stat = st.radio("✔️ Status", ["Todos", "OK", "Divergente"], horizontal=True)

    f_code = st.text_input("🔍 Consulta por Código", placeholder="Digite o código...")
    dff = df_t2 if f_stat == "Todos" else df_t2[df_t2["Status"] == f_stat]
    if f_code: dff = dff[dff["Produto"].astype(str).str.contains(f_code, na=False)]

    lista_joinville = ["Maquinas - Filial", "Service - Matriz", "Service - Filial", "Tools - Filial"]
    dff_jlle = dff[dff["Filial"].isin(lista_joinville)].copy()
    dff_outras = dff[~dff["Filial"].isin(lista_joinville)].copy()
    dff_jlle["Filial"] = dff_jlle["Filial"].str.split(" - ").str[-1]
    dff_outras["Filial"] = dff_outras["Filial"].str.split(" - ").
