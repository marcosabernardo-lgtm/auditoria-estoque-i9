import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import datetime as _dt
from sqlalchemy import create_engine, text
from processador_movs import tratar_notas_fiscais, buscar_movimentacoes_nuvem, buscar_movimentacoes_por_documento, remover_acentos, limpar_id_produto, limpar_id_geral, get_df_empresas
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
    try:
        from sqlalchemy.pool import NullPool
        url = st.secrets["connections"]["postgresql"]["url"]
        url = url.replace(":6543/", ":5432/")
        return create_engine(url, poolclass=NullPool)
    except: return None

@st.cache_data(ttl=3600, show_spinner=False)
def carregar_empresas_filiais():
    """Consulta leve — só Empresa+Filial distintos para montar a tela de seleção."""
    engine = get_engine()
    if engine is None: return [], {}
    try:
        df = pd.read_sql(
            'SELECT DISTINCT "Empresa", "Filial" FROM auditoria ORDER BY "Empresa", "Filial"',
            engine
        )
        empresas = sorted(df["Empresa"].dropna().unique().tolist())
        mapa = {e: sorted(df[df["Empresa"] == e]["Filial"].dropna().unique().tolist()) for e in empresas}
        return empresas, mapa
    except:
        return [], {}

@st.cache_data(ttl=300, show_spinner=False)
def carregar_auditoria_filtrada(empresa: str, filial: str):
    """Carrega só empresa+filial selecionada — cache de 5 min. Não faz SELECT *."""
    engine = get_engine()
    if engine is None: return None
    try:
        df = pd.read_sql(
            'SELECT * FROM auditoria WHERE "Empresa" = %(empresa)s AND "Filial" = %(filial)s',
            engine,
            params={"empresa": empresa, "filial": filial}
        )
        return df if not df.empty else None
    except:
        return None

def formatar_br(valor):
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

# ─── SIDEBAR — Upload sempre visível ────────────────────────────────────────
with st.sidebar:
    st.markdown(
        '<div style="color:#EC6E21;font-weight:700;font-size:1.1rem;margin-bottom:12px;">⚙️ Atualizar Bases</div>',
        unsafe_allow_html=True
    )

    with st.expander("1. Auditoria"):
        u_wms = st.file_uploader("WMS", type=["xlsx"], key="up_wms")
        u_erp = st.file_uploader("ERP", type=["xlsx"], key="up_erp")
        if u_wms and u_erp and st.button("🚀 Processar e Enviar Auditoria"):
            with st.spinner("Cruzando WMS x ERP..."):
                try:
                    df_auditoria = cruzar_wms_erp(u_wms, u_erp)
                    if df_auditoria.empty:
                        st.error("Cruzamento resultou em dados vazios. Verifique os arquivos.")
                    else:
                        resumo = df_auditoria.groupby("Empresa")["Produto"].count().reset_index()
                        resumo.columns = ["Empresa", "Linhas"]
                        st.dataframe(resumo, use_container_width=True, hide_index=True)
                        engine = get_engine()
                        if engine:
                            df_auditoria.to_sql("auditoria", engine, if_exists="replace", index=False)
                            # Invalida caches para refletir novos dados
                            carregar_empresas_filiais.clear()
                            carregar_auditoria_filtrada.clear()
                            st.success(f"✅ {len(df_auditoria)} linhas gravadas!")
                        else:
                            st.error("Sem conexão com o banco.")
                except Exception as e:
                    st.error(f"Erro ao processar: {e}")

    with st.expander("2. Notas Fiscais"):
        u_movs = st.file_uploader("Arquivos Protheus", type=["xlsx"], accept_multiple_files=True)
        if u_movs and st.button("📦 Processar Movimentações"):
            st.cache_data.clear()
            df_nf_novo = tratar_notas_fiscais(u_movs)
            if not df_nf_novo.empty:
                df_nf_novo.to_sql("movimentacoes", get_engine(), if_exists="append", index=False)
                st.success("Enviado!")

    # Botão "Trocar empresa/filial" — só aparece após seleção
    if st.session_state.get("_app_empresa"):
        st.markdown("---")
        st.caption(f"🏢 **{st.session_state['_app_empresa']}**")
        st.caption(f"📍 {st.session_state.get('_app_filial', '—')}")
        st.caption(f"👤 {st.session_state.get('_app_operador', '—')}")
        if st.button("🔄 Trocar empresa/filial", use_container_width=True):
            for k in ["_app_empresa", "_app_filial", "_app_operador", "_data_auditoria"]:
                st.session_state.pop(k, None)
            st.rerun()

# ─── CORPO PRINCIPAL ─────────────────────────────────────────────────────────
st.markdown('<div class="main-title">Gestão Integrada I9</div>', unsafe_allow_html=True)

# ── TELA DE SELEÇÃO ──────────────────────────────────────────────────────────
if not st.session_state.get("_app_empresa"):
    empresas, mapa_filiais = carregar_empresas_filiais()

    if not empresas:
        st.info("💡 Nenhum dado encontrado. Carregue os arquivos WMS e ERP na sidebar para começar.")
        st.stop()

    # Card de seleção centralizado
    _, col_c, _ = st.columns([1, 2, 1])
    with col_c:
        st.markdown(
            """<div style="background:#004550;border:2px solid #EC6E21;border-radius:16px;
                          padding:24px 32px;margin-top:40px;margin-bottom:16px;">
               <div style="color:#fff;font-size:1.3rem;font-weight:700;margin-bottom:4px;">
                 Selecionar empresa e filial
               </div>
               <div style="color:#aac8cc;font-size:0.88rem;">
                 Escolha o contexto para carregar os dados de auditoria.
               </div>
            </div>""",
            unsafe_allow_html=True
        )
        emp_input = st.selectbox("🏢 Empresa", empresas, key="sel_empresa")
        filiais_disp = mapa_filiais.get(emp_input, [])
        filiais_labels = [f.split(" - ")[-1] if " - " in f else f for f in filiais_disp]
        fil_label = st.selectbox("📍 Filial", filiais_labels, key="sel_filial")
        fil_input = filiais_disp[filiais_labels.index(fil_label)] if fil_label in filiais_labels else fil_label
        _operadores = ["", "Marcos Bernardo", "Victor Ferreira", "Rodrigo Ignácio", "Douglas Felipe"]
        operador_input = st.selectbox("👤 Operador", _operadores, key="sel_operador",
                                       format_func=lambda x: "Selecione seu nome..." if x == "" else x)
        _btn_ok = bool(operador_input)
        if st.button("▶  Entrar", type="primary", use_container_width=True,
                     key="btn_entrar", disabled=not _btn_ok):
            st.session_state["_app_empresa"]    = emp_input
            st.session_state["_app_filial"]     = fil_input
            st.session_state["_app_operador"]   = operador_input
            st.session_state["_data_auditoria"] = _dt.now().strftime("%d/%m/%Y %H:%M")
            st.rerun()
        if not _btn_ok:
            st.caption("⚠️ Selecione seu nome para continuar.")

    st.stop()

# ── DADOS JÁ SELECIONADOS ────────────────────────────────────────────────────
empresa_sel = st.session_state["_app_empresa"]
filial_sel  = st.session_state["_app_filial"]

# Cache: mesma empresa+filial = sem nova consulta ao banco
df_base = carregar_auditoria_filtrada(empresa_sel, filial_sel)

if df_base is None or df_base.empty:
    st.warning(
        f"⚠️ Nenhum dado encontrado para **{empresa_sel} / {filial_sel}**. "
        "Verifique se o upload foi feito corretamente."
    )
    st.stop()

st.session_state["_data_auditoria"] = st.session_state.get("_data_auditoria", _dt.now().strftime("%d/%m/%Y %H:%M"))

# Filtro de status e código (empresa/filial já fixados pela seleção)
c1, c2 = st.columns([2, 2])
with c1: f_stat = st.radio("✔️ Status", ["Todos", "OK", "Divergente"], horizontal=True)
with c2: f_code = st.text_input("🔍 Consulta por Código", placeholder="Digite o código...")

dff = df_base if f_stat == "Todos" else df_base[df_base["Status"] == f_stat]
if f_code: dff = dff[dff["Produto"].astype(str).str.contains(f_code, na=False)]

# Usa todos os dados da filial selecionada diretamente
dff_jlle = dff.copy()
dff_jlle["Filial"] = dff_jlle["Filial"].str.split(" - ").str[-1]
dff_outras = pd.DataFrame()  # não usado mais

def preparar_view(df):
    if df.empty: return df
    df_v = df.copy()
    if "Qtd_Locais" in df_v.columns:
        df_v = df_v.rename(columns={"Qtd_Locais": "Qtd Locais"})
    elif "Produto" in df_v.columns:
        df_v["Qtd Locais"] = df_v.groupby("Produto")["Produto"].transform("count").astype(int)
    ordem = [
        "Status", "Empresa", "Filial", "Localização", "Armazem",
        "Produto", "Qtd Locais", "Descrição", "Vl Unit",
        "Saldo ERP (Total)", "Saldo ERP (Rateado)", "Saldo WMS",
        "Divergência", "Vl Divergência", "Vl Total ERP",
    ]
    colunas_ok = [c for c in ordem if c in df_v.columns]
    resto = [c for c in df_v.columns if c not in colunas_ok]
    return df_v[colunas_ok + resto]

v_jlle_view   = preparar_view(dff_jlle)
v_outras_view = preparar_view(dff_outras)

# Injeta dependências para uso nas abas
from tabs.movimentacoes import _tratar_df as _tratar_mov
st.session_state["_engine"]          = get_engine()
st.session_state["_buscar_func"]     = buscar_movimentacoes_nuvem
st.session_state["_buscar_doc_func"] = buscar_movimentacoes_por_documento
st.session_state["_estilizar_func"]  = estilizar_tabela
st.session_state["_to_float_func"]   = to_float_br
st.session_state["_tratar_df"]       = _tratar_mov

tab1, tab2, tab3, tab4 = st.tabs(["📋 Auditoria", "📊 Indicadores", "🔄 Inv. Cíclico", "📋 Ajustes"])

with tab1:
    auditoria.render(v_jlle_view, estilizar_tabela, para_excel)
with tab2:
    indicadores.render(dff_jlle, formatar_br)
with tab3:
    inventario_ciclico.render(dff_jlle, dff_outras, formatar_br)
with tab4:
    ajustes_inventario.render(empresa_sel, filial_sel, formatar_br)
