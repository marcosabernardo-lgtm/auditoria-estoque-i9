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

# 2. CSS CUSTOMIZADO (TOTALMENTE INTEGRADO AO TEMA #005562 e #EC6E21)
st.markdown(
    """
    <style>
    /* FUNDO GERAL */
    .stApp {
        background-color: #005562 !important;
    }
    [data-testid="stHeader"] {
        background-color: #005562 !important;
    }
    
    /* SIDEBAR */
    section[data-testid="stSidebar"] {
        display: block !important;
        background-color: #004550 !important;
    }

    /* TÍTULOS */
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
        text-align: center;
    }

    /* NAVBAR / ABAS */
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

    /* VOLTANDO AS BORDAS DOS FILTROS (RADIOS) */
    div[data-testid="stRadio"] > div {
        background-color: #004550 !important;
        border: 1px solid #007687 !important; /* Borda do filtro */
        border-radius: 12px;
        padding: 8px 15px;
        gap: 15px;
    }
    div[data-testid="stRadio"] label {
        color: white !important;
    }

    /* INPUT DE BUSCA */
    .stTextInput input {
        background-color: #004550 !important;
        color: white !important;
        border: 1px solid #007687 !important;
        border-radius: 10px;
    }

    /* BOTÃO DE DOWNLOAD (LARANJA) */
    .stDownloadButton button {
        background-color: #EC6E21 !important;
        color: white !important;
        border: none !important;
        padding: 10px 20px !important;
        border-radius: 8px !important;
        font-weight: bold !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- FUNÇÃO PARA GERAR EXCEL ---
def para_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Auditoria')
    processed_data = output.getvalue()
    return processed_data

# --- FUNÇÃO DE ESTILIZAÇÃO DA TABELA (FUNDO = PÁGINA) ---
def estilizar_tabela(df):
    fmt_num = {}
    for col in df.columns:
        if col in ["Saldo ERP (Total)", "Saldo ERP (Rateado)", "Saldo WMS", "Divergência"]:
            fmt_num[col] = "{:,.2f}"
        elif col in ["Vl Unit", "Vl Divergência", "Vl Total ERP"]:
            fmt_num[col] = "R$ {:,.2f}"

    def colorir_linha(row):
        return ['background-color: #005562; color: #ffffff; font-size: 0.84rem;'] * len(row)

    def colorir_status(val):
        if val == "Divergente":
            return 'background-color: #722f1d; color: #ffffff; font-weight: bold; border: 1px solid #EC6E21;'
        elif val == "OK":
            return 'background-color: #1a4a32; color: #b3ffcc; font-weight: bold;'
        return ''

    styled = df.style.apply(colorir_linha, axis=1)
    if "Status" in df.columns:
        styled = styled.applymap(colorir_status, subset=["Status"])

    styled = styled.set_table_styles([
        {
            'selector': 'thead th',
            'props': [
                ('background-color', '#004550'),
                ('color', '#ffffff'),
                ('font-weight', '700'),
                ('border-bottom', '2px solid #EC6E21'),
                ('text-transform', 'uppercase'),
            ]
        },
        {
            'selector': 'td',
            'props': [
                ('padding', '8px 12px'),
                ('border-bottom', '1px solid rgba(255,255,255,0.05)'),
            ]
        }
    ])
    if fmt_num:
        styled = styled.format(fmt_num, na_rep="-")
    return styled

# --- FUNÇÕES DE BANCO ---
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

# --- INTERFACE SIDEBAR (NAVBAR RESTAURADO) ---
with st.sidebar:
    st.header("⚙️ Atualizar Bases")
    
    # 1. EXPANDER AUDITORIA
    with st.expander("1. Subir Auditoria (WMS/ERP)"):
        u_wms = st.file_uploader("Arquivo WMS", type=["xlsx"])
        u_erp = st.file_uploader("Arquivo ERP", type=["xlsx"])
        if u_wms and u_erp and st.button("🚀 Processar Auditoria"):
            # Aqui entraria sua lógica de processamento
            st.success("Arquivos recebidos!")

    # 2. EXPANDER NOTAS (RESTALRADO)
    with st.expander("2. Subir Movimentações (Notas)"):
        u_movs = st.file_uploader("Arquivos de Notas", type=["xlsx"], accept_multiple_files=True)
        if u_movs and st.button("📦 Processar Notas"):
            # Aqui entraria sua lógica de processamento das notas
            st.success("Notas recebidas!")

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

    f_code = st.text_input("🔍 Consulta por Código", placeholder="Digite o código do produto...")

    dff = df_t2 if f_stat == "Todos" else df_t2[df_t2["Status"] == f_stat]
    if f_code:
        dff = dff[dff["Produto"].astype(str).str.contains(f_code, na=False)]

    lista_joinville = ["Maquinas - Filial", "Service - Matriz", "Service - Filial", "Tools - Filial"]
    dff_jlle = dff[dff["Filial"].isin(lista_joinville)].copy()
    dff_outras = dff[~dff["Filial"].isin(lista_joinville)].copy()
    
    dff_jlle["Filial"] = dff_jlle["Filial"].str.split(" - ").str[-1]
    dff_outras["Filial"] = dff_outras["Filial"].str.split(" - ").str[-1]

    tab1, tab2, tab3, tab4 = st.tabs(["📍 Joinville", "🚛 Filiais", "📊 Indicadores", "🕒 Movimentações"])

    def preparar_view(df):
        if df.empty: return df
        df_v = df.rename(columns={"C Unitario": "Vl Unit"})
        cols = [c for c in df_v.columns if c != "Vl Unit"]
        if "Descrição" in cols: cols.insert(cols.index("Descrição") + 1, "Vl Unit")
        return df_v[cols]

    with tab1:
        st.subheader("Auditoria - Unidades Joinville")
        v_jlle = preparar_view(dff_jlle)
        if not v_jlle.empty:
            st.dataframe(estilizar_tabela(v_jlle), use_container_width=True, hide_index=True)
        st.download_button("📥 Baixar Excel", para_excel(v_jlle), "auditoria_joinville.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    with tab2:
        st.subheader("Auditoria - Outras Filiais")
        v_out = preparar_view(dff_outras)
        if not v_out.empty:
            st.dataframe(estilizar_tabela(v_out), use_container_width=True, hide_index=True)
        st.download_button("📥 Baixar Excel", para_excel(v_out), "auditoria_filiais.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    with tab3:
        if not dff_jlle.empty:
            v_total = dff_jlle["Vl Total ERP"].sum()
            v_err = dff_jlle["Vl Divergência"].abs().sum()
            ac_v = (1 - (v_err/v_total))*100 if v_total > 0 else 0
            
            k1, k2, k3 = st.columns(3)
            k1.metric("VALOR EM ESTOQUE", f"R$ {formatar_br(v_total)}")
            k2.metric("IMPACTO DIVERGENTE", f"R$ {formatar_br(v_err)}")
            k3.metric("ACURACIDADE VALOR", f"{ac_v:.2f}%")

            df_unq = dff_jlle.drop_duplicates(subset=["Empresa", "Filial", "Armazem", "Produto"])
            total_it = len(df_unq)
            it_div = len(df_unq[df_unq["Status"] == "Divergente"])
            ac_it = (1 - (it_div/total_it))*100 if total_it > 0 else 0

            k4, k5, k6 = st.columns(3)
            k4.metric("TOTAL ITENS", f"{total_it:,}".replace(",", "."))
            k5.metric("ITENS DIVERGENTES", f"{it_div:,}".replace(",", "."))
            k6.metric("ACURACIDADE ITENS", f"{ac_it:.2f}%")

    with tab4:
        if f_code and len(f_code) >= 3:
            try:
                engine = get_engine()
                df_nf = buscar_movimentacoes_nuvem(engine, f_code)
                if not df_nf.empty:
                    st.dataframe(estilizar_tabela(df_nf), use_container_width=True, hide_index=True)
            except: pass

else:
    st.info("💡 Carregue os dados para começar.")
