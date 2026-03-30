import streamlit as st
import pandas as pd
import numpy as np
import io
from sqlalchemy import create_engine
from processador_movs import tratar_notas_fiscais, buscar_movimentacoes_nuvem, buscar_ultima_movimentacao_geral, remover_acentos, limpar_id_produto, limpar_id_geral, get_df_empresas
from processador_auditoria import cruzar_wms_erp

# IMPORTANDO AS NOVAS ABAS
from tabs import joinville, filiais, indicadores, movimentacoes, inventario_ciclico

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
        styled = styled.applymap(lambda v: 'background-color: #722f1d; color: #ffffff; font-weight: bold; border: 1px solid #EC6E21;' if v == "Divergente" else ('background-color: #1a4a32; color: #b3ffcc; font-weight: bold;' if v == "OK" else ''), subset=["Status"])
    styled = styled.set_table_styles([
        {'selector': 'thead th', 'props': [('background-color', '#004550'), ('color', '#ffffff'), ('border-bottom', '2px solid #EC6E21'), ('text-transform', 'uppercase')]},
        {'selector': 'td', 'props': [('padding', '8px 12px'), ('border-bottom', '1px solid rgba(255,255,255,0.05)')]}
    ])
    if fmt_cols: styled = styled.format(fmt_cols, na_rep="-")
    return styled

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

# --- SIDEBAR ---
with st.sidebar:
    st.header("⚙️ Atualizar Bases")
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
                        # Diagnóstico por empresa
                        resumo = df_auditoria.groupby("Empresa")["Produto"].count().reset_index()
                        resumo.columns = ["Empresa", "Linhas"]
                        st.dataframe(resumo, use_container_width=True, hide_index=True)
                        engine = get_engine()
                        if engine:
                            df_auditoria.to_sql("auditoria", engine, if_exists="replace", index=False)
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

# --- CORPO PRINCIPAL ---
st.markdown('<div class="main-title">Gestão Integrada I9</div>', unsafe_allow_html=True)
df_base = carregar_do_banco("auditoria")

if df_base is not None:
    # DEBUG TEMPORÁRIO — remover após validar
    with st.expander("🔍 Debug: Filiais no banco"):
        filiais_banco = sorted(df_base["Filial"].dropna().unique().tolist())
        st.write("**Todas no banco:**", filiais_banco)
        st.write("**Na lista Joinville:**", [f for f in filiais_banco if any(j in f for j in ["Maquinas - Filial","Máquinas - Filial","Service - Matriz","Service - Filial","Tools - Filial"])])
        st.write("**Na lista Outras:**", [f for f in filiais_banco if any(j in f for j in ["Jundiai","Caxias","Jaragua","Robotica","Robótica"])])
        st.write("**Sem classificação:**", [f for f in filiais_banco if not any(j in f for j in ["Maquinas - Filial","Máquinas - Filial","Service - Matriz","Service - Filial","Tools - Filial","Jundiai","Caxias","Jaragua","Robotica","Robótica"])])
    # Filtros
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

    # Separação Joinville x Outras Filiais
    lista_joinville = [
        "Maquinas - Filial",   "Máquinas - Filial",
        "Service - Matriz",
        "Service - Filial",
        "Tools - Filial",
    ]
    lista_outras = [
        "Maquinas - Jundiai",  "Máquinas - Jundiai",
        "Service - Caxias",
        "Service - Jundiai",
        "Robotica - Matriz",   "Robótica - Matriz",
        "Robotica - Jaragua",  "Robótica - Jaragua",
    ]
    dff_jlle   = dff[ dff["Filial"].isin(lista_joinville)].copy()
    dff_outras = dff[ dff["Filial"].isin(lista_outras)].copy()
    # Mantém só o sufixo para exibição (ex: "Maquinas - Filial" → "Filial")
    dff_jlle["Filial"]   = dff_jlle["Filial"].str.split(" - ").str[-1]
    dff_outras["Filial"] = dff_outras["Filial"].str.split(" - ").str[-1]

    # Reordenar colunas — dados já vêm com rateio correto do processador_auditoria
    def preparar_view(df):
        if df.empty: return df
        df_v = df.copy()
        # Qtd Locais: usa coluna do banco se existir, senão calcula
        if "Qtd_Locais" in df_v.columns:
            df_v = df_v.rename(columns={"Qtd_Locais": "Qtd Locais"})
        elif "Produto" in df_v.columns:
            df_v["Qtd Locais"] = df_v.groupby("Produto")["Produto"].transform("count").astype(int)
        # Ordem de colunas amigável
        ordem = [
            "Status", "Empresa", "Filial", "Localização", "Armazem",
            "Produto", "Qtd Locais", "Descrição", "Vl Unit",
            "Saldo ERP (Total)", "Saldo ERP (Rateado)", "Saldo WMS",
            "Divergência", "Vl Divergência", "Vl Total ERP",
        ]
        colunas_ok = [c for c in ordem if c in df_v.columns]
        resto = [c for c in df_v.columns if c not in colunas_ok]
        return df_v[colunas_ok + resto]

    v_jlle_view = preparar_view(dff_jlle)
    v_outras_view = preparar_view(dff_outras)

    # CHAMADA DAS ABAS
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📍 Joinville", "🚛 Filiais", "📊 Indicadores", "🕒 Movimentações", "🔄 Inv. Cíclico"])

    with tab1:
        joinville.render(v_jlle_view, estilizar_tabela, para_excel)
    with tab2:
        filiais.render(v_outras_view, estilizar_tabela, para_excel)
    with tab3:
        indicadores.render(dff_jlle, formatar_br)
    with tab4:
        movimentacoes.render(f_code.zfill(6) if f_code else "", get_engine(), buscar_movimentacoes_nuvem, buscar_ultima_movimentacao_geral, estilizar_tabela, to_float_br)
    with tab5:
        inventario_ciclico.render(dff_jlle, dff_outras, formatar_br)
else:
    st.info("💡 Carregue os dados para começar.")