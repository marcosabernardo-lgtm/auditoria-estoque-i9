import streamlit as st
import pandas as pd
import numpy as np
import logging
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

# 2. CSS CUSTOMIZADO - PALETA OFICIAL (#005562 e #EC6E21)
st.markdown(
    """
    <style>
    /* FUNDO TOTAL DA PÁGINA */
    .stApp {
        background-color: #005562 !important;
    }
    [data-testid="stHeader"] {
        background-color: #005562 !important;
    }
    [data-testid="stSidebar"] {
        background-color: #004550 !important;
    }

    /* TÍTULO COM BARRA LATERAL LARANJA */
    .main-title {
        border-left: 6px solid #EC6E21;
        padding-left: 15px;
        color: #ffffff;
        font-weight: 700;
        font-size: 2.2rem;
        margin-bottom: 25px;
    }

    /* CARDS DE MÉTRICAS */
    div[data-testid="stMetric"] {
        border: 2px solid #EC6E21 !important;
        background-color: #004550 !important;
        border-radius: 10px;
        padding: 15px;
    }
    div[data-testid="stMetricValue"] > div {
        color: #ffffff !important;
        font-weight: 800 !important;
    }

    /* NAVBAR / ABAS (ESTILO DASHBOARD) */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
        background-color: transparent !important;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #004550 !important;
        color: white !important;
        border-radius: 5px 5px 0px 0px;
        padding: 10px 25px;
        border: none !important;
    }
    .stTabs [aria-selected="true"] {
        background-color: #EC6E21 !important;
        font-weight: bold !important;
    }

    /* --- AJUSTE DA TABELA (PARA SAIR DO PRETO E FICAR AZUL) --- */
    div[data-testid="stDataFrame"], 
    div[data-testid="stDataFrame"] [role="grid"],
    div[data-testid="stDataFrame"] [data-testid="stTable"] {
        background-color: #004550 !important;
    }

    /* Cabeçalho da Tabela com borda Laranja */
    div[data-testid="stDataFrame"] [role="columnheader"] {
        background-color: #005562 !important;
        color: white !important;
        border-bottom: 2px solid #EC6E21 !important;
    }

    /* Celulas da Tabela */
    div[data-testid="stDataFrame"] [role="gridcell"] {
        background-color: #004550 !important;
        color: white !important;
        border: 0.1px solid rgba(255,255,255,0.1) !important;
    }

    /* FILTROS (RADIOS) */
    div[data-testid="stRadio"] > div {
        background-color: #004550 !important;
        border: 1px solid #007687 !important;
        border-radius: 12px;
        padding: 5px 15px;
    }
    div[data-testid="stRadio"] label {
        color: white !important;
    }

    /* INPUT DE BUSCA */
    .stTextInput input {
        background-color: #004550 !important;
        color: white !important;
        border: 1px solid #007687 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- FUNÇÕES DE BANCO ---
def get_engine():
    try:
        conn_url = st.secrets["connections"]["postgresql"]["url"]
        return create_engine(conn_url)
    except:
        return None

def carregar_do_banco(tabela):
    engine = get_engine()
    if engine is None: return None
    try:
        return pd.read_sql(f"SELECT * FROM {tabela}", engine)
    except:
        return None

def formatar_br(valor):
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# --- INTERFACE ---

st.markdown('<div class="main-title">Gestão Integrada I9</div>', unsafe_allow_html=True)

df_base = carregar_do_banco("auditoria")

if df_base is not None:
    # FILTROS NO TOPO
    st.write("### 🛠️ Filtros de Seleção")
    c1, c2, c3 = st.columns(3)
    with c1:
        f_emp = st.radio("🏢 Empresa", ["Todas"] + sorted(df_base["Empresa"].unique().tolist()), horizontal=True)
    df_t1 = df_base if f_emp == "Todas" else df_base[df_base["Empresa"] == f_emp]

    with c2:
        dict_filiais = {"Todas": "Todas"}
        for f in sorted(df_t1["Filial"].unique().tolist()):
            dict_filiais[f.split(" - ")[-1] if " - " in f else f] = f
        f_fil_curta = st.radio("📍 Filial", list(dict_filiais.keys()), horizontal=True)
        f_fil_longa = dict_filiais[f_fil_curta]
    df_t2 = df_t1 if f_fil_longa == "Todas" else df_t1[df_t1["Filial"] == f_fil_longa]

    with c3:
        f_stat = st.radio("✔️ Status", ["Todos", "OK", "Divergente"], horizontal=True)

    f_code = st.text_input("🔍 Consulta por Código", placeholder="Digite o produto...")

    # APLICAÇÃO DE FILTROS
    dff = df_t2 if f_stat == "Todos" else df_t2[df_t2["Status"] == f_stat]
    if f_code:
        dff = dff[dff["Produto"].astype(str).str.contains(f_code, na=False)]

    # SEPARAÇÃO JOINVILLE
    lista_joinville = ["Maquinas - Filial", "Service - Matriz", "Service - Filial", "Tools - Filial"]
    dff_jlle = dff[dff["Filial"].isin(lista_joinville)].copy()
    dff_outras = dff[~dff["Filial"].isin(lista_joinville)].copy()
    
    # LIMPEZA VISUAL FILIAL
    dff_jlle["Filial"] = dff_jlle["Filial"].str.split(" - ").str[-1]
    dff_outras["Filial"] = dff_outras["Filial"].str.split(" - ").str[-1]

    # ABAS (NAVBAR)
    tab1, tab2, tab3, tab4 = st.tabs(["📍 Joinville", "🚛 Filiais", "📊 Indicadores", "🕒 Movimentações"])

    fmt_num = {"Saldo ERP (Total)": "{:,.2f}", "Saldo ERP (Rateado)": "{:,.2f}", "Vl Unit": "R$ {:,.2f}", "Saldo WMS": "{:,.2f}", "Divergência": "{:,.2f}", "Vl Divergência": "R$ {:,.2f}", "Vl Total ERP": "R$ {:,.2f}"}

    def preparar_view(df):
        if df.empty: return df
        df_v = df.rename(columns={"C Unitario": "Vl Unit"})
        cols = [c for c in df_v.columns if c != "Vl Unit"]
        if "Descrição" in cols: cols.insert(cols.index("Descrição") + 1, "Vl Unit")
        return df_v[cols]

    with tab1:
        st.subheader("Auditoria - Unidades Joinville")
        v_jlle = preparar_view(dff_jlle)
        st.dataframe(v_jlle.style.format(fmt_num, decimal=",", thousands="."), use_container_width=True, hide_index=True)

    with tab2:
        st.subheader("Auditoria - Outras Filiais")
        v_outras = preparar_view(dff_outras)
        st.dataframe(v_outras.style.format(fmt_num, decimal=",", thousands="."), use_container_width=True, hide_index=True)

    with tab3:
        if not dff_jlle.empty:
            v_total = dff_jlle["Vl Total ERP"].sum()
            v_err = dff_jlle["Vl Divergência"].abs().sum()
            ac_v = (1 - (v_err/v_total))*100 if v_total > 0 else 0
            
            k1, k2, k3 = st.columns(3)
            k1.metric("VALOR EM ESTOQUE", f"R$ {formatar_br(v_total)}")
            k2.metric("IMPACTO DIVERGENTE", f"R$ {formatar_br(v_err)}")
            k3.metric("ACURACIDADE VALOR", f"{ac_v:.2f}%")

    with tab4:
        if f_code and len(f_code) >= 3:
            df_nf = buscar_movimentacoes_nuvem(get_engine(), f_code)
            st.dataframe(df_nf, use_container_width=True, hide_index=True)
else:
    st.info("💡 Carregue os dados na barra lateral para começar.")
