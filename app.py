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

# --- CSS REFINADO: AZUL PETRÓLEO VIBRANTE (ESTILO DASHBOARD PROFISSIONAL) ---
st.markdown(
    """
    <style>
    /* Cor de Fundo Geral - Azul Petróleo mais vivo */
    [data-testid="stAppViewContainer"] {
        background-color: #0e3d42;
    }
    [data-testid="stHeader"] {
        background-color: #0e3d42;
    }
    [data-testid="stSidebar"] {
        background-color: #0a2e32;
    }

    /* Título Principal */
    .main-title {
        border-left: 6px solid #ff8c00;
        padding-left: 15px;
        color: #ffffff;
        font-weight: 700;
        font-size: 2.2rem;
        margin-bottom: 25px;
    }

    /* Cards de Métricas (Fundo azul profundo, borda laranja) */
    div[data-testid="stMetric"] {
        border: 1px solid #ff8c00; 
        padding: 20px; 
        border-radius: 12px;
        background-color: #0d2f33; 
        box-shadow: 4px 4px 15px rgba(0,0,0,0.3);
    }
    div[data-testid="stMetric"] label {
        color: #00d4df !important; /* Ciano suave para o label */
        font-size: 0.95rem !important;
        font-weight: 500;
        text-transform: uppercase;
    }
    div[data-testid="stMetricValue"] > div {
        color: #ffffff !important;
        font-size: 2rem !important;
        font-weight: 700 !important;
    }

    /* Filtros (Radios) - Estilo Pílula Integrada */
    div[data-testid="stRadio"] > div {
        flex-direction: row; 
        border: 1px solid #1a5e65; 
        padding: 6px 14px;
        border-radius: 15px; 
        background-color: #134e54; /* Azul petróleo médio */
    }
    div[data-testid="stRadio"] label {
        color: #ffffff !important;
        font-size: 0.9rem;
    }

    /* Campo de Busca (Input) */
    .stTextInput input {
        background-color: #134e54 !important;
        color: white !important;
        border: 1px solid #1a5e65 !important;
        border-radius: 8px;
        height: 45px;
    }

    /* Estilização das Abas (Tabs) */
    .stTabs [data-baseweb="tab-list"] {
        gap: 12px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #134e54;
        border-radius: 8px 8px 0px 0px;
        color: #ffffff;
        padding: 10px 25px;
        font-weight: 400;
        border-bottom: 3px solid transparent;
    }
    .stTabs [aria-selected="true"] {
        background-color: #ff8c00 !important;
        color: white !important;
        font-weight: 700 !important;
    }

    /* Tabelas (Dataframe) - Integrar fundo */
    .stDataFrame {
        border: 1px solid #1a5e65;
        border-radius: 8px;
        background-color: #0d2f33;
    }

    /* Botão Exportar (Laranja) */
    .stDownloadButton button {
        background-color: #ff8c00 !important;
        color: white !important;
        border-radius: 8px;
        border: none;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ---------------------------------------------------------------------------
# BANCO DE DADOS E LÓGICA (MANTIDA ORIGINAL)
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
        st.error(f"❌ Erro: {exc}")
        return None

def formatar_br(valor):
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def to_float_br(serie):
    return pd.to_numeric(serie.astype(str).str.replace(r"[^\d,.-]", "", regex=True).str.replace(".", "", regex=False).str.replace(",", ".", regex=False), errors="coerce")

# ---------------------------------------------------------------------------
# INTERFACE PRINCIPAL
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
        opcoes_filiais = sorted(df_t1["Filial"].unique().tolist())
        dict_filiais = {"Todas": "Todas"}
        for f in opcoes_filiais:
            dict_filiais[f.split(" - ")[-1] if " - " in f else f] = f
        f_fil_curta = st.radio("📍 Filial", list(dict_filiais.keys()), horizontal=True)
        f_fil_longa = dict_filiais[f_fil_curta]
    df_t2 = df_t1 if f_fil_longa == "Todas" else df_t1[df_t1["Filial"] == f_fil_longa]

    with c3:
        f_stat = st.radio("✔️ Status", ["Todos", "OK", "Divergente"], horizontal=True)

    dff_parcial = df_t2 if f_stat == "Todos" else df_t2[df_t2["Status"] == f_stat]
    
    st.write("### 🔍 Consulta por Código")
    f_code = st.text_input("", placeholder="Ex: 001262")

    dff = dff_parcial.copy()
    if f_code:
        dff = dff[dff["Produto"].astype(str).str.contains(f_code, na=False)]

    # Separação Joinville
    lista_joinville = ["Maquinas - Filial", "Service - Matriz", "Service - Filial", "Tools - Filial"]
    dff_jlle = dff[dff["Filial"].isin(lista_joinville)].copy()
    dff_outras = dff[~dff["Filial"].isin(lista_joinville)].copy()

    # Limpeza visual da Filial
    dff_jlle["Filial"] = dff_jlle["Filial"].str.split(" - ").str[-1]
    dff_outras["Filial"] = dff_outras["Filial"].str.split(" - ").str[-1]

    # Abas
    tab1, tab2, tab3, tab4 = st.tabs(["📍 Joinville", "🚛 Filiais", "📈 Indicadores", "🚚 Movimentações"])

    # Formatação de Tabelas
    fmt_moeda = {"Saldo ERP (Total)": "{:,.2f}", "Saldo ERP (Rateado)": "{:,.2f}", "Vl Unit": "R$ {:,.2f}", "Saldo WMS": "{:,.2f}", "Divergência": "{:,.2f}", "Vl Divergência": "R$ {:,.2f}", "Vl Total ERP": "R$ {:,.2f}"}

    def preparar_view(df):
        if df.empty: return df
        df_v = df.rename(columns={"C Unitario": "Vl Unit"})
        cols = [c for c in df_v.columns if c != "Vl Unit"]
        if "Descrição" in cols: cols.insert(cols.index("Descrição") + 1, "Vl Unit")
        return df_v[cols]

    with tab1:
        v_jlle = preparar_view(dff_jlle)
        st.dataframe(v_jlle.style.format(fmt_moeda, decimal=",", thousands="."), use_container_width=True, hide_index=True)
        st.download_button("📥 Exportar Planilha Joinville", v_jlle.to_csv(index=False).encode('utf-8-sig'), "joinville.csv", "text/csv")

    with tab2:
        v_outras = preparar_view(dff_outras)
        st.dataframe(v_outras.style.format(fmt_moeda, decimal=",", thousands="."), use_container_width=True, hide_index=True)
        st.download_button("📥 Exportar Planilha Filiais", v_outras.to_csv(index=False).encode('utf-8-sig'), "filiais.csv", "text/csv")

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
        if f_code and len(f_code) >= 3:
            try:
                df_nf = buscar_movimentacoes_nuvem(get_engine(), f_code)
                if not df_nf.empty:
                    df_nf = df_nf.drop_duplicates()
                    st.write(f"Últimas Movimentações: **{f_code}**")
                    # Lógica de formatação de notas (reutilizada do seu código)
                    st.dataframe(df_nf, use_container_width=True, hide_index=True)
            except Exception as e:
                st.error(f"Erro ao buscar notas: {e}")
else:
    st.info("💡 Por favor, carregue os dados na barra lateral para começar.")
