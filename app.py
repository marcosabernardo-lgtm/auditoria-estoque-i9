import streamlit as st
import pandas as pd
import numpy as np
import io
import os
from sqlalchemy import create_engine
from processador_movs import tratar_notas_fiscais, buscar_movimentacoes_nuvem, remover_acentos, limpar_id_produto, limpar_id_geral, get_df_empresas

# Configuração
st.set_page_config(page_title="Gestão I9 - Premium", layout="wide")

# CSS para os Cards
st.markdown("""
    <style>
    div[data-testid="stMetric"] { border: 1px solid #464b5d; padding: 20px; border-radius: 12px; background-color: #0e1117; box-shadow: 2px 2px 10px rgba(0,0,0,0.2); }
    div[data-testid="stRadio"] > div { flex-direction: row; border: 1px solid #464b5d; padding: 5px 15px; border-radius: 15px; background-color: #0e1117; overflow-x: auto; }
    div[data-testid="stRadio"] label { margin-right: 15px; white-space: nowrap; }
    </style>
    """, unsafe_allow_html=True)

# --- CONEXÃO BANCO ---
def get_engine():
    try:
        conn_url = st.secrets["connections"]["postgresql"]["url"]
        return create_engine(conn_url, connect_args={"options": "-c fts.prepare_threshold=0"})
    except: return None

def salvar_no_banco(df, tabela):
    engine = get_engine()
    if engine is not None:
        df.to_sql(tabela, engine, if_exists='replace', index=False)
        return True
    return False

def carregar_do_banco(tabela):
    engine = get_engine()
    if engine is not None:
        try: return pd.read_sql(f'SELECT * FROM {tabela}', engine)
        except: return None
    return None

def formatar_br(valor):
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

@st.cache_data
def processar_auditoria(file_wms, file_estoque):
    df_ref = get_df_empresas().rename(columns={'Empresa_Cod_Filial':'ID_Empresa_Ref', 'Empresa_Filial_Nome':'Nome_Filial_Completo'})
    
    # Processar WMS
    df_loc = pd.read_excel(file_wms)
    df_loc.columns = [str(c).strip() for c in df_loc.columns]
    df_loc = df_loc[df_loc['Utilizado'] > 0].copy()
    col_wms_emp = [c for c in df_loc.columns if "Empresa" in c and "Filial" in c][0]
    df_loc['Aba_Ref'] = df_loc[col_wms_emp].str.extract(r'-(.*?) -', expand=False).str.strip().apply(remover_acentos)
    mask_tools_a02 = (df_loc['Aba_Ref'] == "Tools") & (df_loc['Localização'].str.startswith("A02", na=False))
    df_loc.loc[mask_tools_a02, 'Localização'] = df_loc.loc[mask_tools_a02, 'Localização'].str.replace("A02", "A20", 1)
    df_loc['Num_Filial_WMS'] = df_loc[col_wms_emp].str.extract(r'^(\d+)', expand=False).str.strip().str.slice(-2)
    df_loc['ID_Empresa_Ref'] = df_loc['Aba_Ref'] + " " + df_loc['Num_Filial_WMS']
    df_loc = pd.merge(df_loc, df_ref, on='ID_Empresa_Ref', how='inner')
    df_loc['Armazem_WMS'] = df_loc['Localização'].str.extract(r'A(.*?)\.', expand=False).fillna("").str.zfill(2)
    df_loc['Produto_WMS'] = limpar_id_produto(df_loc['Produto'])
    df_loc['ID_Cruzamento'] = df_loc['ID_Empresa_Ref'] + "-" + df_loc['Armazem_WMS'] + "-" + df_loc['Produto_WMS']
    df_loc_resumo = df_loc[['ID_Cruzamento', 'Localização', 'Utilizado']].rename(columns={'Utilizado': 'Saldo WMS'})

    # Processar ERP
    dict_abas = pd.read_excel(file_estoque, sheet_name=None)
    lista_dfs = []
    categorias_validas = ["Tools", "Service", "Maquinas", "Robotica"]
    for nome_aba in dict_abas.keys():
        aba_limpa = remover_acentos(nome_aba)
        if aba_limpa in categorias_validas:
            df_temp = dict_abas[nome_aba].copy()
            df_temp.columns = [str(c).strip() for c in df_temp.columns]
            df_temp['Aba_Ref'] = aba_limpa
            df_temp['Num_Filial_ERP'] = limpar_id_geral(df_temp['Filial'], 2)
            df_temp['ID_Empresa_Ref'] = df_temp['Aba_Ref'] + " " + df_temp['Num_Filial_ERP']
            df_temp = pd.merge(df_temp, df_ref, on='ID_Empresa_Ref', how='left')
            df_temp['Armazem_ERP'] = limpar_id_geral(df_temp['Armazem'], 2)
            df_temp['Produto_ERP'] = limpar_id_produto(df_temp['Produto'])
            df_temp['ID_Cruzamento'] = df_temp['ID_Empresa_Ref'] + "-" + df_temp['Armazem_ERP'] + "-" + df_temp['Produto_ERP']
            lista_dfs.append(df_temp)
    
    df_erp = pd.concat(lista_dfs, ignore_index=True)
    df_erp = df_erp[df_erp['Saldo Atual'] > 0].copy()
    df_final = pd.merge(df_erp, df_loc_resumo, on='ID_Cruzamento', how='left')
    df_final['Saldo WMS'] = df_final['Saldo WMS'].fillna(0)
    df_final['Localização'] = df_final['Localização'].fillna("Não Localizado")
    agrup = df_final.groupby('ID_Cruzamento').agg(Total_WMS=('Saldo WMS', 'sum'), Total_ERP=('Saldo Atual', 'max'), Qtd_Locais=('ID_Cruzamento', 'count')).reset_index()
    df_final = pd.merge(df_final, agrup, on='ID_Cruzamento')
    df_final['Status'] = np.where(np.abs(df_final['Total_WMS'] - df_final['Total_ERP']) < 0.01, "OK", "Divergente")
    df_final['Saldo ERP (Rateado)'] = np.where(df_final['Status'] == "OK", df_final['Saldo WMS'], df_final['Total_ERP'] / df_final['Qtd_Locais'])
    df_final['Divergência'] = np.where(df_final['Status'] == "OK", 0, df_final['Saldo WMS'] - df_final['Saldo ERP (Rateado)'])
    df_final['Vl Divergência'] = df_final['Divergência'] * df_final['C Unitario']
    df_final['Vl Total ERP'] = df_final['Saldo ERP (Rateado)'] * df_final['C Unitario']
    
    return df_final[['Status', 'Aba_Ref', 'Nome_Filial_Completo', 'Localização', 'Armazem_ERP', 'Produto_ERP', 'Descrição', 'Total_ERP', 'Saldo ERP (Rateado)', 'C Unitario', 'Saldo WMS', 'Divergência', 'Vl Divergência', 'Vl Total ERP']].rename(columns={'Aba_Ref': 'Empresa', 'Nome_Filial_Completo': 'Filial', 'Armazem_ERP': 'Armazem', 'Produto_ERP': 'Produto', 'Total_ERP': 'Saldo ERP (Total)'})

# --- INTERFACE ---
st.title("📊 Gestão Integrada de Estoque I9")

df_base = carregar_do_banco('auditoria')

with st.sidebar:
    st.header("⚙️ Atualizar Bases")
    with st.expander("1. Auditoria (WMS/ERP)"):
        u_wms = st.file_uploader("WMS (Localizações)", type=["xlsx"])
        u_erp = st.file_uploader("ERP (Estoque)", type=["xlsx"])
        if u_wms and u_erp and st.button("🚀 Enviar Auditoria"):
            df_aud = processar_auditoria(u_wms, u_erp)
            if salvar_no_banco(df_aud, 'auditoria'):
                st.success("Auditoria atualizada!")
                st.rerun()

    with st.expander("2. Movimentações (Notas Fiscais)"):
        u_movs = st.file_uploader("Arquivos da pasta bd_entradas", type=["xlsx"], accept_multiple_files=True)
        if u_movs and st.button("📦 Enviar Notas Fiscais"):
            df_nf = tratar_notas_fiscais(u_movs)
            if salvar_no_banco(df_nf, 'movimentacoes'):
                st.success(f"{len(df_nf)} notas enviadas!")

if df_base is not None:
    # FILTROS
    st.write("### 🛠️ Filtros Globais")
    c1, c2, c3 = st.columns(3)
    with c1: f_emp = st.radio("🏢 Empresa", ["Todas"] + sorted(df_base['Empresa'].unique().tolist()), horizontal=True)
    df_t1 = df_base if f_emp == "Todas" else df_base[df_base['Empresa'] == f_emp]
    with c2: f_fil = st.radio("📍 Filial", ["Todas"] + sorted(df_t1['Filial'].unique().tolist()), horizontal=True)
    df_t2 = df_t1 if f_fil == "Todas" else df_t1[df_t1['Filial'] == f_fil]
    with c3: f_stat = st.radio("✔️ Status", ["Todos", "OK", "Divergente"], horizontal=True)
    
    dff_parcial = df_t2 if f_stat == "Todos" else df_t2[df_t2['Status'] == f_stat]
    f_code = st.text_input("🔍 CONSULTA POR CÓDIGO", placeholder="Ex: 001262")
    
    dff = dff_parcial.copy()
    if f_code: dff = dff[dff['Produto'].astype(str).str.contains(f_code, na=False)]

    tab1, tab2, tab3 = st.tabs(["📄 Consulta Auditoria", "📈 Indicadores", "🚚 Entradas e Saídas"])

    with tab1:
        st.dataframe(dff.style.format({'Saldo ERP (Total)': '{:,.0f}', 'C Unitario': 'R$ {:,.4f}', 'Vl Divergência': 'R$ {:,.2f}', 'Vl Total ERP': 'R$ {:,.2f}'}, decimal=',', thousands='.'), use_container_width=True)

    with tab2:
        v_total = dff['Vl Total ERP'].sum()
        v_div = dff['Vl Divergência'].sum()
        ac_v = (1 - (dff['Vl Divergência'].abs().sum() / v_total)) * 100 if v_total > 0 else 0
        st.markdown("#### Indicadores Financeiros")
        k1, k2, k3 = st.columns(3)
        k1.metric("Valor em Estoque", f"R$ {formatar_br(v_total)}")
        k2.metric("Impacto Divergente", f"R$ {formatar_br(v_div)}")
        k3.metric("Acuracidade Valor", f"{ac_v:.2f}%")

    with tab3:
        if f_code and len(f_code) >= 3:
            df_nf_res = buscar_movimentacoes_nuvem(get_engine(), f_code)
            if not df_nf_res.empty:
                st.write(f"Notas Fiscais do Produto: {f_code}")
                st.dataframe(df_nf_res, use_container_width=True)
            else: st.warning("Nenhuma nota encontrada.")
        else: st.info("Digite o código acima para consultar as notas.")
else:
    st.info("💡 Carregue os arquivos na lateral.")
