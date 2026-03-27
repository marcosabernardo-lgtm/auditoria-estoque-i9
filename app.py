import streamlit as st
import pandas as pd
import numpy as np
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
    /* 1. FUNDO GERAL */
    .stApp {
        background-color: #005562 !important;
    }
    [data-testid="stHeader"] {
        background-color: #005562 !important;
    }
    
    /* GARANTIR QUE A SIDEBAR APAREÇA */
    section[data-testid="stSidebar"] {
        display: block !important;
        background-color: #004550 !important;
    }

    /* 2. TÍTULOS */
    .main-title {
        border-left: 6px solid #EC6E21;
        padding-left: 15px;
        color: #ffffff;
        font-weight: 700;
        font-size: 2.2rem;
        margin-bottom: 25px;
    }
    .section-title {
        color: #ffffff;
        font-size: 1.2rem;
        font-weight: 600;
        margin-top: 20px;
        margin-bottom: 10px;
    }

    /* 3. CARDS DE MÉTRICAS */
    div[data-testid="stMetric"] {
        border: 2px solid #EC6E21 !important;
        background-color: #004550 !important;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
    }
    div[data-testid="stMetricValue"] > div {
        color: #ffffff !important;
        font-size: 2.2rem !important;
        font-weight: 800 !important;
    }
    div[data-testid="stMetric"] label {
        color: #ffffff !important;
        opacity: 0.8;
    }

    /* 4. ABAS (NAVBAR) */
    .stTabs [data-baseweb="tab-list"] {
        gap: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #004550 !important;
        color: white !important;
        border-radius: 5px 5px 0px 0px;
        padding: 10px 20px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #EC6E21 !important;
    }

    /* 5. TABELAS (FORÇAR AZUL PETRÓLEO EM TODAS AS CÉLULAS) */
    /* Isso remove o fundo preto que o Streamlit coloca por padrão */
    div[data-testid="stDataFrame"], 
    div[data-testid="stDataFrame"] > div {
        background-color: #004550 !important;
    }
    
    /* Cabeçalhos da tabela */
    div[data-testid="stDataFrame"] [role="columnheader"] {
        background-color: #005562 !important;
        color: white !important;
        border-bottom: 2px solid #EC6E21 !important;
    }

    /* 6. FILTROS E INPUTS */
    div[data-testid="stRadio"] > div {
        background-color: #004550 !important;
        border: 1px solid #007687 !important;
        border-radius: 10px;
    }
    div[data-testid="stRadio"] label, .stTextInput label {
        color: white !important;
    }
    .stTextInput input {
        background-color: #004550 !important;
        color: white !important;
        border: 1px solid #007687 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- FUNÇÕES DE BANCO E LÓGICA ---
def get_engine():
    try:
        conn_url = st.secrets["connections"]["postgresql"]["url"]
        return create_engine(conn_url)
    except: return None

def carregar_do_banco(tabela):
    engine = get_engine()
    if engine is None: return None
    try: return pd.read_sql(f"SELECT * FROM {tabela}", engine)
    except: return None

def formatar_br(valor):
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

@st.cache_data
def processar_auditoria(file_wms, file_estoque):
    # Lógica de processamento (WMS/ERP) conforme o resumo original
    # (Omitido aqui por brevidade, mas deve ser a sua função completa)
    pass

# --- INTERFACE SIDEBAR (NAVBAR DE INPUT) ---
with st.sidebar:
    st.header("⚙️ Atualizar Bases")
    with st.expander("1. Auditoria (WMS/ERP)"):
        u_wms = st.file_uploader("Upload WMS", type=["xlsx"])
        u_erp = st.file_uploader("Upload ERP", type=["xlsx"])
        if u_wms and u_erp and st.button("🚀 Enviar Auditoria"):
            # Lógica de processamento e salvar_auditoria_no_banco
            st.success("Auditoria enviada!")

    with st.expander("2. Movimentações"):
        u_movs = st.file_uploader("Notas Fiscais", type=["xlsx"], accept_multiple_files=True)
        if u_movs and st.button("📦 Enviar Notas"):
            # Lógica tratar_notas_fiscais e salvar_no_banco
            st.success("Notas enviadas!")

# --- CORPO PRINCIPAL ---
st.markdown('<div class="main-title">Gestão Integrada I9</div>', unsafe_allow_html=True)

df_base = carregar_do_banco("auditoria")

if df_base is not None:
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

    # Aplicação de filtros
    dff = df_t2 if f_stat == "Todos" else df_t2[df_t2["Status"] == f_stat]
    if f_code:
        dff = dff[dff["Produto"].astype(str).str.contains(f_code, na=False)]

    # Separação Joinville
    lista_joinville = ["Maquinas - Filial", "Service - Matriz", "Service - Filial", "Tools - Filial"]
    dff_jlle = dff[dff["Filial"].isin(lista_joinville)].copy()
    dff_outras = dff[~dff["Filial"].isin(lista_joinville)].copy()
    
    # Limpeza visual filial
    dff_jlle["Filial"] = dff_jlle["Filial"].str.split(" - ").str[-1]
    dff_outras["Filial"] = dff_outras["Filial"].str.split(" - ").str[-1]

    # ABAS
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
            # --- LINHA 1: FINANCEIRO ---
            st.markdown('<div class="section-title">💰 Financeiro (Joinville)</div>', unsafe_allow_html=True)
            v_total = dff_jlle["Vl Total ERP"].sum()
            v_err = dff_jlle["Vl Divergência"].abs().sum()
            ac_v = (1 - (v_err/v_total))*100 if v_total > 0 else 0
            
            k1, k2, k3 = st.columns(3)
            k1.metric("VALOR EM ESTOQUE", f"R$ {formatar_br(v_total)}")
            k2.metric("IMPACTO DIVERGENTE", f"R$ {formatar_br(v_err)}")
            k3.metric("ACURACIDADE VALOR", f"{ac_v:.2f}%")

            # --- LINHA 2: QUANTIDADES (RESTAURADO) ---
            st.markdown('<div class="section-title">📦 Itens (Joinville)</div>', unsafe_allow_html=True)
            df_unq = dff_jlle.drop_duplicates(subset=["Empresa", "Filial", "Armazem", "Produto"])
            total_it = len(df_unq)
            it_div = len(df_unq[df_unq["Status"] == "Divergente"])
            ac_it = (1 - (it_div/total_it))*100 if total_it > 0 else 0

            k4, k5, k6 = st.columns(3)
            k4.metric("TOTAL DE ITENS", f"{total_it:,}".replace(",", "."))
            k5.metric("ITENS COM ERRO", f"{it_div:,}".replace(",", "."))
            k6.metric("ACURACIDADE ITENS", f"{ac_it:.2f}%")

    with tab4:
        st.info("Utilize a consulta por código para ver movimentações.")

else:
    st.info("💡 Carregue os arquivos na barra lateral para iniciar.")
