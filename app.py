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

st.set_page_config(page_title="Gestão Integrada I9", layout="wide")

# --- CSS BASEADO NA SUA PALETA OFICIAL (#005562 e #EC6E21) ---
st.markdown(
    f"""
    <style>
    /* Cor de Fundo Geral - Azul Petróleo Oficial */
    [data-testid="stAppViewContainer"] {{
        background-color: #005562;
    }}
    [data-testid="stHeader"] {{
        background-color: #005562;
    }}
    [data-testid="stSidebar"] {{
        background-color: #004550; /* Um tom levemente mais escuro para a lateral */
    }}

    /* Título Principal com borda Laranja Oficial */
    .main-title {{
        border-left: 6px solid #EC6E21;
        padding-left: 15px;
        color: #ffffff;
        font-weight: 700;
        font-size: 2.2rem;
        margin-bottom: 25px;
    }}

    /* Cards de Métricas (Borda Laranja Oficial) */
    div[data-testid="stMetric"] {{
        border: 2px solid #EC6E21; 
        padding: 20px; 
        border-radius: 12px;
        background-color: #004550; 
        box-shadow: 4px 4px 15px rgba(0,0,0,0.2);
        text-align: center;
    }}
    div[data-testid="stMetricValue"] > div {{
        color: #ffffff !important;
        font-size: 2.2rem !important;
        font-weight: 800 !important;
    }}
    div[data-testid="stMetric"] label {{
        color: #ffffff !important;
        font-size: 0.9rem !important;
        text-transform: uppercase;
        opacity: 0.8;
    }}

    /* Filtros (Radios) - Azul Petróleo contrastante */
    div[data-testid="stRadio"] > div {{
        flex-direction: row; 
        border: 1px solid #007687; 
        padding: 6px 14px;
        border-radius: 15px; 
        background-color: #004550;
    }}
    div[data-testid="stRadio"] label {{
        color: #ffffff !important;
    }}

    /* Campo de Busca */
    .stTextInput input {{
        background-color: #004550 !important;
        color: white !important;
        border: 1px solid #007687 !important;
    }}

    /* Estilização das Abas (Ativa em Laranja Oficial) */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 10px;
    }}
    .stTabs [data-baseweb="tab"] {{
        background-color: #004550;
        border-radius: 8px 8px 0px 0px;
        color: #ffffff;
        padding: 10px 25px;
    }}
    .stTabs [aria-selected="true"] {{
        background-color: #EC6E21 !important;
        color: white !important;
        font-weight: 700 !important;
    }}

    /* Ajuste de Tabelas */
    .stDataFrame {{
        background-color: #004550;
        border-radius: 10px;
    }}
    
    /* Botão de Download Laranja */
    .stDownloadButton button {{
        background-color: #EC6E21 !important;
        color: white !important;
        border: none !important;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# BANCO DE DADOS (Lógica original preservada)
# ---------------------------------------------------------------------------

def get_engine():
    try:
        conn_url = st.secrets["connections"]["postgresql"]["url"]
        return create_engine(conn_url, connect_args={"options": "-c fts.prepare_threshold=0"})
    except Exception as exc:
        st.error(f"❌ Falha ao conectar: {exc}")
        return None

def carregar_do_banco(tabela):
    engine = get_engine()
    if engine is None: return None
    try:
        return pd.read_sql(f"SELECT * FROM {tabela}", engine)
    except Exception as exc:
        st.error(f"❌ Erro ao carregar: {exc}")
        return None

def formatar_br(valor):
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# ---------------------------------------------------------------------------
# INTERFACE
# ---------------------------------------------------------------------------

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

    st.write("### 🔍 Consulta por Código")
    f_code = st.text_input("", placeholder="Digite o código do produto...")

    # Filtros Finais
    dff = df_t2 if f_stat == "Todos" else df_t2[df_t2["Status"] == f_stat]
    if f_code:
        dff = dff[dff["Produto"].astype(str).str.contains(f_code, na=False)]

    # Separação Joinville
    lista_joinville = ["Maquinas - Filial", "Service - Matriz", "Service - Filial", "Tools - Filial"]
    dff_jlle = dff[dff["Filial"].isin(lista_joinville)].copy()
    dff_outras = dff[~dff["Filial"].isin(lista_joinville)].copy()
    dff_jlle["Filial"] = dff_jlle["Filial"].str.split(" - ").str[-1]
    dff_outras["Filial"] = dff_outras["Filial"].str.split(" - ").str[-1]

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
        st.download_button("📥 Exportar Joinville", v_jlle.to_csv(index=False).encode('utf-8-sig'), "joinville.csv", "text/csv")

    with tab2:
        st.subheader("Auditoria - Outras Filiais")
        v_out = preparar_view(dff_outras)
        st.dataframe(v_out.style.format(fmt_num, decimal=",", thousands="."), use_container_width=True, hide_index=True)
        st.download_button("📥 Exportar Filiais", v_out.to_csv(index=False).encode('utf-8-sig'), "filiais.csv", "text/csv")

    with tab3:
        if not dff_jlle.empty:
            v_total = dff_jlle["Vl Total ERP"].sum()
            v_err = dff_jlle["Vl Divergência"].abs().sum()
            ac_v = (1 - (v_err/v_total))*100 if v_total > 0 else 0
            df_unq = dff_jlle.drop_duplicates(subset=["Empresa", "Filial", "Armazem", "Produto"])
            total_it, it_div = len(df_unq), len(df_unq[df_unq["Status"] == "Divergente"])
            ac_it = (1 - (it_div/total_it))*100 if total_it > 0 else 0
            
            st.write("#### 💰 Financeiro (Joinville)")
            k1, k2, k3 = st.columns(3)
            k1.metric("VALOR EM ESTOQUE", f"R$ {formatar_br(v_total)}")
            k2.metric("IMPACTO DIVERGENTE", f"R$ {formatar_br(v_err)}")
            k3.metric("ACURACIDADE VALOR", f"{ac_v:.2f}%")

            st.write("#### 📦 Itens (Joinville)")
            k4, k5, k6 = st.columns(3)
            k4.metric("TOTAL DE ITENS", f"{total_it:,}".replace(",", "."))
            k5.metric("ITENS COM ERRO", f"{it_div:,}".replace(",", "."))
            k6.metric("ACURACIDADE ITENS", f"{ac_it:.2f}%")

    with tab4:
        st.info("Utilize a consulta por código para visualizar o histórico de movimentações.")

else:
    st.info("💡 Carregue os dados na barra lateral para começar.")
