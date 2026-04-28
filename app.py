import streamlit as st
import pandas as pd
import io
from datetime import datetime as _dt
from sqlalchemy import create_engine
from processador_auditoria import cruzar_wms_erp

# IMPORTANDO AS NOVAS ABAS
from tabs import auditoria, indicadores, inventario_ciclico, ajustes_inventario

# 1. Configuração da Página
st.set_page_config(page_title="Gestão Integrada I9", layout="wide")

# 2. CSS CUSTOMIZADO (TEMA OFICIAL #005562 e #EC6E21)
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

# --- CONFIGURAÇÃO DE AMBIENTE ---
MODO_DEV = False  # True = Local (Teste) | False = Supabase (Produção)

# --- FUNÇÕES DE UTILIDADE ---
def para_excel(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Planilha')
    return output.getvalue()

def to_float_br(serie):
    return pd.to_numeric(serie.astype(str).str.replace(r"[^\d,.-]", "", regex=True).str.replace(".", "", regex=False).str.replace(",", ".", regex=False), errors="coerce")

def estilizar_tabela(df):
    fmt_cols = {}
    for col in df.columns:
        if any(x in col for x in ["Saldo", "Divergência", "Qtd"]) and "Vl" not in col: fmt_cols[col] = "{:,.2f}"
        elif any(x in col for x in ["Vl Unit", "Vl Total", "Preço", "Vl Divergência", "Vl Total ERP"]): fmt_cols[col] = "R$ {:,.2f}"
    styled = df.style.apply(lambda r: ['background-color: #005562; color: #ffffff; font-size: 0.84rem;'] * len(r), axis=1)
    if "Status" in df.columns:
        styled = styled.map(lambda v: 'background-color: #722f1d; color: #ffffff; font-weight: bold; border: 1px solid #EC6E21;' if v == "Divergente" else ('background-color: #1a4a32; color: #b3ffcc; font-weight: bold;' if v == "OK" else ''), subset=["Status"])
    styled = styled.set_table_styles([
        {'selector': 'thead th', 'props': [('background-color', '#004550'), ('color', '#ffffff'), ('border-bottom', '2px solid #EC6E21'), ('text-transform', 'uppercase')]},
        {'selector': 'td', 'props': [('padding', '8px 12px'), ('border-bottom', '1px solid rgba(255,255,255,0.05)')]}
    ])
    if fmt_cols: styled = styled.format(fmt_cols, na_rep="-")
    return styled

def get_engine():
    if MODO_DEV:
        return create_engine("sqlite:///auditoria_i9_TESTE")
    try:
        from sqlalchemy.pool import NullPool
        url = st.secrets["connections"]["postgresql"]["url"]
        url = url.replace(":6543/", ":5432/")
        return create_engine(url, poolclass=NullPool)
    except: return None

@st.cache_data(ttl=3600, show_spinner=False)
def carregar_empresas_filiais():
    engine = get_engine()
    if engine is None: return [], {}
    try:
        df = pd.read_sql('SELECT DISTINCT "Empresa", "Filial" FROM auditoria ORDER BY "Empresa", "Filial"', engine)
        empresas = sorted(df["Empresa"].dropna().unique().tolist())
        mapa = {e: sorted(df[df["Empresa"] == e]["Filial"].dropna().unique().tolist()) for e in empresas}
        return empresas, mapa
    except: return [], {}

@st.cache_data(ttl=300, show_spinner=False)
def carregar_auditoria_filtrada(empresa: str, filial: str):
    engine = get_engine()
    if engine is None: return None
    try:
        # Ajuste de sintaxe para SQLite vs PostgreSQL
        if MODO_DEV:
            query = 'SELECT * FROM auditoria WHERE "Empresa" = :empresa AND "Filial" = :filial'
        else:
            query = 'SELECT * FROM auditoria WHERE "Empresa" = %(empresa)s AND "Filial" = %(filial)s'
        
        df = pd.read_sql(query, engine, params={"empresa": empresa, "filial": filial})
        return df if not df.empty else None
    except Exception as e:
        st.error(f"Erro ao filtrar dados: {e}")
        return None

def formatar_br(valor):
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

@st.cache_data(ttl=300, show_spinner=False)
def preparar_view(df):
    if df.empty: return df
    df_v = df.copy()
    if "Produto" in df_v.columns: df_v["Qtd Locais"] = df_v.groupby("Produto")["Produto"].transform("count")
    ordem = ["Status", "Empresa", "Filial", "Localização", "Armazem", "Produto", "Qtd Locais", "Descrição", "Vl Unit", "Saldo ERP (Total)", "Saldo WMS", "Divergência", "Vl Divergência", "Vl Total ERP"]
    return df_v[[c for c in ordem if c in df_v.columns]]

# ─── SIDEBAR ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<div style="color:#EC6E21;font-weight:700;font-size:1.1rem;margin-bottom:12px;">⚙️ Atualizar Bases</div>', unsafe_allow_html=True)
    

    with st.expander("1. Auditoria"):
        u_wms = st.file_uploader("WMS", type=["xlsx"], key="up_wms")
        u_erp = st.file_uploader("ERP", type=["xlsx"], key="up_erp")
        if u_wms and u_erp and st.button("🚀 Processar e Enviar Auditoria"):
            with st.spinner("Cruzando WMS x ERP..."):
                try:
                    df_auditoria = cruzar_wms_erp(u_wms, u_erp)
                    if not df_auditoria.empty:
                        engine = get_engine()
                        df_auditoria.to_sql("auditoria", engine, if_exists="replace", index=False)
                        carregar_empresas_filiais.clear()
                        carregar_auditoria_filtrada.clear()
                        st.success(f"✅ {len(df_auditoria)} linhas gravadas no banco de TESTE!")
                        st.rerun()
                except Exception as e: st.error(f"Erro: {e}")

    if st.session_state.get("_app_empresa"):
        st.markdown("---")
        st.caption(f"🏢 **{st.session_state['_app_empresa']}**")
        st.caption(f"📍 {st.session_state.get('_app_filial', '—')}")
        if st.button("🔄 Trocar empresa/filial", use_container_width=True):
            for k in ["_app_empresa", "_app_filial", "_app_operador", "_data_auditoria"]: st.session_state.pop(k, None)
            st.rerun()

# ─── CORPO PRINCIPAL ─────────────────────────────────────────────────────────
st.markdown('<div class="main-title">Gestão Integrada I9</div>', unsafe_allow_html=True)

if not st.session_state.get("_app_empresa"):
    empresas, mapa_filiais = carregar_empresas_filiais()
    if not empresas:
        st.info("💡 Banco de TESTE vazio. Carregue WMS e ERP na sidebar.")
        st.stop()
    _, col_c, _ = st.columns([1, 2, 1])
    with col_c:
        st.markdown('<div style="background:#004550;border:2px solid #EC6E21;border-radius:16px;padding:24px 32px;margin-top:40px;margin-bottom:16px;"><div style="color:#fff;font-size:1.3rem;font-weight:700;margin-bottom:4px;">Selecionar empresa e filial</div></div>', unsafe_allow_html=True)
        emp_input = st.selectbox("🏢 Empresa", empresas, key="sel_empresa")
        filiais_disp = mapa_filiais.get(emp_input, [])
        filiais_labels = [f.split(" - ")[-1] if " - " in f else f for f in filiais_disp]
        fil_label = st.selectbox("📍 Filial", filiais_labels, key="sel_filial")
        fil_input = filiais_disp[filiais_labels.index(fil_label)] if fil_label in filiais_labels else fil_label
        operador_input = st.selectbox("👤 Operador", ["", "Marcos Bernardo", "Victor Ferreira", "Rodrigo Ignácio", "Douglas Felipe"], format_func=lambda x: "Selecione..." if x == "" else x)
        if st.button("▶  Entrar", type="primary", use_container_width=True, disabled=not operador_input):
            st.session_state["_app_empresa"] = emp_input
            st.session_state["_app_filial"] = fil_input
            st.session_state["_app_operador"] = operador_input
            st.session_state["_data_auditoria"] = _dt.now().strftime("%d/%m/%Y %H:%M")
            st.rerun()
    st.stop()

empresa_sel = st.session_state["_app_empresa"]
filial_sel  = st.session_state["_app_filial"]
df_base = carregar_auditoria_filtrada(empresa_sel, filial_sel)

if df_base is None or df_base.empty:
    st.warning(f"⚠️ Nenhum dado encontrado para **{empresa_sel} / {filial_sel}**. Recarregue os arquivos na sidebar.")
    st.stop()

# Filtros e Abas
c1, c2 = st.columns([2, 2])
with c1: f_stat = st.radio("✔️ Status", ["Todos", "OK", "Divergente"], horizontal=True)
with c2: f_code = st.text_input("🔍 Consulta", placeholder="Código ou Descrição...")

dff_base_abas = df_base if f_stat == "Todos" else df_base[df_base["Status"] == f_stat]
dff_auditoria = dff_base_abas.copy()
if f_code:
    dff_auditoria = dff_auditoria[dff_auditoria["Produto"].astype(str).str.contains(f_code, case=False) | dff_auditoria["Descrição"].astype(str).str.contains(f_code, case=False)]

dff_jlle = dff_base_abas.copy()
dff_jlle["Filial"] = dff_jlle["Filial"].str.split(" - ").str[-1]

v_jlle_view = preparar_view(dff_auditoria)
st.session_state["_engine"] = get_engine()

tab1, tab2, tab3, tab4 = st.tabs(["📋 Auditoria", "📊 Indicadores", "🔄 Inv. Cíclico", "📋 Ajustes"])
with tab1: auditoria.render(v_jlle_view, estilizar_tabela, para_excel)
with tab2: indicadores.render(dff_jlle, formatar_br)
with tab3: inventario_ciclico.render(dff_jlle, df_base, formatar_br)
with tab4: ajustes_inventario.render(empresa_sel, filial_sel, formatar_br)