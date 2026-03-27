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
    for col in df.columns:
        if any(x in col for x in ["Saldo", "Divergência", "Qtd"]): fmt_cols[col] = "{:,.2f}"
        elif any(x in col for x in ["Vl Unit", "Vl Total", "Preço"]): fmt_cols[col] = "R$ {:,.2f}"

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

# --- INTERFACE ---
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
    dff_outras["Filial"] = dff_outras["Filial"].str.split(" - ").str[-1]

    tab1, tab2, tab3, tab4 = st.tabs(["📍 Joinville", "🚛 Filiais", "📊 Indicadores", "🕒 Movimentações"])

    def preparar_view(df):
        if df.empty: return df
        df_v = df.rename(columns={"C Unitario": "Vl Unit"})
        cols = [c for c in df_v.columns if c != "Vl Unit"]
        if "Descrição" in cols: cols.insert(cols.index("Descrição") + 1, "Vl Unit")
        return df_v[cols]

    with tab1:
        v_jlle = preparar_view(dff_jlle)
        if not v_jlle.empty: st.dataframe(estilizar_tabela(v_jlle), use_container_width=True, hide_index=True)
        st.download_button("📥 Excel Joinville", para_excel(v_jlle), "joinville.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    with tab2:
        v_out = preparar_view(dff_outras)
        if not v_out.empty: st.dataframe(estilizar_tabela(v_out), use_container_width=True, hide_index=True)
        st.download_button("📥 Excel Filiais", para_excel(v_out), "filiais.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    with tab3:
        if not dff_jlle.empty:
            v_total = dff_jlle["Vl Total ERP"].sum()
            v_err = dff_jlle["Vl Divergência"].abs().sum()
            ac_v = (1 - (v_err/v_total))*100 if v_total > 0 else 0
            df_unq = dff_jlle.drop_duplicates(subset=["Empresa", "Filial", "Armazem", "Produto"])
            k1, k2, k3 = st.columns(3)
            k1.metric("ESTOQUE TOTAL", f"R$ {formatar_br(v_total)}")
            k2.metric("VALOR DIVERGENTE", f"R$ {formatar_br(v_err)}")
            k3.metric("ACURACIDADE VALOR", f"{ac_v:.2f}%")
            k4, k5, k6 = st.columns(3)
            k4.metric("TOTAL ITENS", f"{len(df_unq):,}".replace(",", "."))
            k5.metric("ITENS DIVERGENTES", f"{len(df_unq[df_unq['Status'] == 'Divergente']):,}".replace(",", "."))
            k6.metric("ACURACIDADE ITENS", f"{(1 - (len(df_unq[df_unq['Status'] == 'Divergente'])/len(df_unq)))*100:.2f}%")

    with tab4:
        if f_code and len(f_code) >= 3:
            engine = get_engine()
            df_nf = buscar_movimentacoes_nuvem(engine, f_code)
            if not df_nf.empty:
                # --- RESTAURANDO FORMATAÇÃO DE NOTAS ---
                df_nf = df_nf.drop_duplicates()
                df_nf["DIGITACAO"] = pd.to_datetime(df_nf["DIGITACAO"]).dt.strftime("%d/%m/%Y")
                if "Empresa_Filial_Nome" in df_nf.columns:
                    split = df_nf["Empresa_Filial_Nome"].str.split(" - ", n=1, expand=True)
                    df_nf.insert(0, "Filial", split[1].fillna(""))
                    df_nf.insert(0, "Empresa", split[0].fillna(""))
                    df_nf = df_nf.drop(columns=["Empresa_Filial_Nome"])
                
                df_nf = df_nf.rename(columns={
                    "TIPOMOVIMENTO": "Tipo Movimento", "DOCUMENTO": "Documento", "DIGITACAO": "Digitação",
                    "NOTA_DEVOLUCAO": "Nota Devolução", "PRODUTO": "Produto", "DESCRICAO": "Descrição",
                    "CENTRO_CUSTO": "Centro Custo", "RAZAO_SOCIAL": "Razão Social",
                    "QUANTIDADE": "Qtd", "PRECO_UNITARIO": "Vl Unit", "TOTAL": "Vl Total"
                })

                # Limpeza de Nota Devolução e Centro de Custo
                for col in ["Nota Devolução", "Centro Custo"]:
                    if col in df_nf.columns:
                        df_nf[col] = df_nf[col].astype(str).str.replace(".0", "", regex=False).replace("nan", "")
                
                # Formatação de Moeda
                for col in ["Vl Unit", "Vl Total"]:
                    if col in df_nf.columns: df_nf[col] = to_float_br(df_nf[col])

                st.dataframe(estilizar_tabela(df_nf), use_container_width=True, hide_index=True)
            else:
                st.warning("Nenhuma movimentação encontrada.")
else:
    st.info("💡 Aguardando dados...")
