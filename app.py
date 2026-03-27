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

logger = logging.getLogger(__name__)

# CSS para Cards e Radios
st.markdown(
    """
    <style>
    div[data-testid="stMetric"] {
        border: 1px solid #464b5d; padding: 20px; border-radius: 12px;
        background-color: #0e1117; box-shadow: 2px 2px 10px rgba(0,0,0,0.2);
    }
    div[data-testid="stRadio"] > div {
        flex-direction: row; border: 1px solid #464b5d; padding: 5px 15px;
        border-radius: 15px; background-color: #0e1117; overflow-x: auto;
    }
    div[data-testid="stRadio"] label { margin-right: 15px; white-space: nowrap; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# BANCO DE DADOS
# ---------------------------------------------------------------------------

def get_engine():
    try:
        conn_url = st.secrets["connections"]["postgresql"]["url"]
        return create_engine(
            conn_url, connect_args={"options": "-c fts.prepare_threshold=0"}
        )
    except KeyError:
        st.error("⚙️ Configuração ausente: verifique o bloco [connections.postgresql] em st.secrets.")
        return None
    except Exception as exc:
        st.error(f"❌ Falha ao criar engine do banco: {exc}")
        return None

def carregar_do_banco(tabela):
    engine = get_engine()
    if engine is None: return None
    try:
        return pd.read_sql(f"SELECT * FROM {tabela}", engine)
    except Exception as exc:
        st.error(f"❌ Erro ao carregar tabela '{tabela}': {exc}")
        return None

# ---------------------------------------------------------------------------
# UTILITÁRIOS
# ---------------------------------------------------------------------------

def formatar_br(valor):
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def exportar_csv(df):
    return df.to_csv(index=False).encode('utf-8-sig')

# ---------------------------------------------------------------------------
# INTERFACE
# ---------------------------------------------------------------------------

st.title("📊 Gestão Integrada I9")

df_base = carregar_do_banco("auditoria")

with st.sidebar:
    st.header("⚙️ Atualizar Bases")
    st.info("Utilize os campos abaixo para carregar novos arquivos WMS ou ERP.")
    # (Os uploaders continuam aqui conforme sua necessidade)

# ---------------------------------------------------------------------------
# PAINEL PRINCIPAL
# ---------------------------------------------------------------------------

if df_base is not None:
    st.write("### 🛠️ Filtros de Seleção")
    c1, c2, c3 = st.columns(3)

    # 1. Filtro de Empresa
    with c1:
        f_emp = st.radio("🏢 Empresa", ["Todas"] + sorted(df_base["Empresa"].unique().tolist()), horizontal=True)
    df_t1 = df_base if f_emp == "Todas" else df_base[df_base["Empresa"] == f_emp]

    # 2. Filtro de Filial (Mostra nome curto no rádio)
    with c2:
        opcoes_filiais_completas = sorted(df_t1["Filial"].unique().tolist())
        dict_filiais = {"Todas": "Todas"}
        for f in opcoes_filiais_completas:
            nome_curto = f.split(" - ")[-1] if " - " in f else f
            dict_filiais[nome_curto] = f
        
        f_fil_curta = st.radio("📍 Filial", list(dict_filiais.keys()), horizontal=True)
        f_fil_longa = dict_filiais[f_fil_curta]

    df_t2 = df_t1 if f_fil_longa == "Todas" else df_t1[df_t1["Filial"] == f_fil_longa]

    # 3. Filtro de Status e Busca
    with c3:
        f_stat = st.radio("✔️ Status", ["Todos", "OK", "Divergente"], horizontal=True)

    dff_parcial = df_t2 if f_stat == "Todos" else df_t2[df_t2["Status"] == f_stat]
    f_code = st.text_input("🔍 CONSULTA POR CÓDIGO", placeholder="Ex: 001262")

    dff = dff_parcial.copy()
    if f_code:
        dff = dff[dff["Produto"].astype(str).str.contains(f_code, na=False)]

    # --- REGRA DE SEPARAÇÃO JOINVILLE VS FILIAIS ---
    lista_joinville = ["Maquinas - Filial", "Service - Matriz", "Service - Filial", "Tools - Filial"]
    
    dff_jlle = dff[dff["Filial"].isin(lista_joinville)].copy()
    dff_outras = dff[~dff["Filial"].isin(lista_joinville)].copy()

    # --- AJUSTE DA COLUNA FILIAL PARA EXIBIÇÃO (LIMPEZA DO NOME) ---
    # Isso transforma "Tools - Filial" em apenas "Filial" dentro da tabela
    dff_jlle["Filial"] = dff_jlle["Filial"].str.split(" - ").str[-1]
    dff_outras["Filial"] = dff_outras["Filial"].str.split(" - ").str[-1]

    # --- CRIAÇÃO DAS ABAS ---
    tab1, tab2, tab3, tab4 = st.tabs([
        "📍 Auditoria Joinville", 
        "🚛 Auditoria Filiais", 
        "📈 Indicadores (Joinville)", 
        "🚚 Entradas e Saídas"
    ])

    estilo_tabela = {
        "Saldo ERP (Total)"   : "{:,.2f}",
        "Saldo ERP (Rateado)" : "{:,.2f}",
        "C Unitario"          : "R$ {:,.2f}",
        "Saldo WMS"           : "{:,.2f}",
        "Divergência"         : "{:,.2f}",
        "Vl Divergência"      : "R$ {:,.2f}",
        "Vl Total ERP"        : "R$ {:,.2f}",
    }

    with tab1:
        st.subheader("Auditoria - Unidades Joinville")
        if dff_jlle.empty:
            st.warning("Nenhum dado encontrado para Joinville.")
        else:
            st.dataframe(dff_jlle.style.format(estilo_tabela, decimal=",", thousands="."), use_container_width=True, hide_index=True)
            st.download_button("📥 Exportar Joinville (Excel/CSV)", exportar_csv(dff_jlle), "auditoria_joinville.csv", "text/csv")

    with tab2:
        st.subheader("Auditoria - Outras Filiais")
        if dff_outras.empty:
            st.info("Nenhum dado de outras filiais.")
        else:
            st.dataframe(dff_outras.style.format(estilo_tabela, decimal=",", thousands="."), use_container_width=True, hide_index=True)
            st.download_button("📥 Exportar Filiais (Excel/CSV)", exportar_csv(dff_outras), "auditoria_filiais.csv", "text/csv")

    with tab3:
        st.subheader("KPIs - Somente Unidades Joinville")
        if dff_jlle.empty:
            st.error("Sem dados de Joinville para calcular indicadores.")
        else:
            # Cálculos de Indicadores
            v_total = dff_jlle["Vl Total ERP"].sum()
            v_err_abs = dff_jlle["Vl Divergência"].abs().sum()
            ac_v = (1 - (v_err_abs / v_total)) * 100 if v_total > 0 else 0

            # Itens Únicos (Removendo duplicatas de locais para contar o produto)
            df_unq = dff_jlle.drop_duplicates(subset=["Empresa", "Filial", "Armazem", "Produto"])
            total_it = len(df_unq)
            it_div = len(df_unq[df_unq["Status"] == "Divergente"])
            ac_it = (1 - (it_div / total_it)) * 100 if total_it > 0 else 0

            k1, k2, k3 = st.columns(3)
            k1.metric("Valor em Estoque", f"R$ {formatar_br(v_total)}")
            k2.metric("Impacto Divergente (Abs)", f"R$ {formatar_br(v_err_abs)}")
            k3.metric("Acuracidade Valor", f"{ac_v:.2f}%")

            k4, k5, k6 = st.columns(3)
            k4.metric("Total de Itens", f"{total_it:,}".replace(",", "."))
            k5.metric("Itens Divergentes", f"{it_div:,}".replace(",", "."))
            k6.metric("Acuracidade Itens", f"{ac_it:.2f}%")

    with tab4:
        if f_code and len(f_code) >= 3:
            try:
                engine = get_engine()
                df_nf_res = buscar_movimentacoes_nuvem(engine, f_code)
                if not df_nf_res.empty:
                    st.write(f"Últimas Movimentações: **{f_code}**")
                    st.dataframe(df_nf_res, use_container_width=True, hide_index=True)
                else:
                    st.warning("Nenhuma movimentação encontrada.")
            except Exception as exc:
                st.error(f"❌ Erro ao buscar movimentações: {exc}")
        else:
            st.info("Digite o código no campo de busca para ver o histórico.")
else:
    st.info("💡 Carregue os arquivos na lateral para iniciar.")
