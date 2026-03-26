import streamlit as st
import pandas as pd
import numpy as np
import io
import unicodedata
from sqlalchemy import create_engine
import os

# Configuração da página
st.set_page_config(page_title="Auditoria I9 - Online", layout="wide")

# --- CONEXÃO COM BANCO NA NUVEM ---
def get_engine():
    try:
        conn_url = st.secrets["connections"]["postgresql"]["url"]
        if conn_url.startswith("postgres://"):
            conn_url = conn_url.replace("postgres://", "postgresql://", 1)
        
        # Adicionamos esse argumento para evitar erros com o Pooler do Supabase
        return create_engine(conn_url, connect_args={"options": "-c fts.prepare_threshold=0"})
    except Exception as e:
        st.error(f"Erro na configuração: {e}")
        return None

def salvar_no_banco(df):
    engine = get_engine()
    if engine:
        try:
            df.to_sql('auditoria', engine, if_exists='replace', index=False)
            return True
        except Exception as e:
            st.error(f"Erro ao salvar na nuvem: {e}")
    return False

def carregar_do_banco():
    engine = get_engine()
    if engine:
        try:
            return pd.read_sql('SELECT * FROM auditoria', engine)
        except: return None
    return None

# --- FUNÇÕES DE APOIO ---
def remover_acentos(texto):
    if not isinstance(texto, str): return texto
    return "".join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def formatar_br(valor):
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def limpar_id_produto(serie):
    return serie.astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(6)

def limpar_id_geral(serie, digitos):
    return serie.astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(digitos)

def get_df_empresas():
    data = [
        ["Tools 00", "Tools", "Matriz"], ["Tools 01", "Tools", "Filial"],
        ["Maquinas 00", "Maquinas", "Matriz"], ["Maquinas 01", "Maquinas", "Filial"], ["Maquinas 02", "Maquinas", "Jundiai"],
        ["Robotica 00", "Robotica", "Matriz"], ["Robotica 01", "Robotica", "Jaragua"],
        ["Service 01", "Service", "Matriz"], ["Service 02", "Service", "Filial"], ["Service 03", "Service", "Caxias"], ["Service 04", "Service", "Jundiai"]
    ]
    return pd.DataFrame(data, columns=['ID_Empresa_Ref', 'Empresa_Cat', 'Filial_Nome'])

@st.cache_data
def processar_dados(file_wms, file_estoque):
    df_ref = get_df_empresas()
    df_loc = pd.read_excel(file_wms)
    df_loc.columns = [str(c).strip() for c in df_loc.columns]
    if 'Utilizado' in df_loc.columns:
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
    dict_abas = pd.read_excel(file_estoque, sheet_name=None)
    lista_dfs = []
    categorias_validas = df_ref['Empresa_Cat'].unique()
    for nome_aba in dict_abas.keys():
        nome_aba_limpo = remover_acentos(nome_aba)
        if nome_aba_limpo in categorias_validas:
            df_temp = dict_abas[nome_aba].copy()
            df_temp.columns = [str(c).strip() for c in df_temp.columns]
            df_temp['Aba_Ref'] = nome_aba_limpo
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
    return df_final[['Status', 'Empresa_Cat', 'Filial_Nome', 'Localização', 'Armazem_ERP', 'Produto_ERP', 'Descrição', 'Total_ERP', 'Saldo ERP (Rateado)', 'C Unitario', 'Saldo WMS', 'Divergência', 'Vl Divergência', 'Vl Total ERP']].rename(columns={'Empresa_Cat': 'Empresa', 'Filial_Nome': 'Filial', 'Armazem_ERP': 'Armazem', 'Produto_ERP': 'Produto', 'Total_ERP': 'Saldo ERP (Total)'})

# --- CSS PARA ESTILIZAÇÃO ---
st.markdown("""
    <style>
    div[data-testid="stMetric"] { border: 1px solid #464b5d; padding: 20px; border-radius: 12px; background-color: #0e1117; box-shadow: 2px 2px 10px rgba(0,0,0,0.2); }
    div[data-testid="stRadio"] > div { flex-direction: row; border: 1px solid #464b5d; padding: 5px 15px; border-radius: 15px; background-color: #0e1117; overflow-x: auto; }
    div[data-testid="stRadio"] label { margin-right: 15px; white-space: nowrap; }
    </style>
    """, unsafe_allow_html=True)

# --- INTERFACE PRINCIPAL ---
st.title("📊 Auditoria de Estoque I9 - Online")

df_base = carregar_do_banco()

st.sidebar.header("📁 Gestão de Dados")
u_wms = st.sidebar.file_uploader("Upload WMS", type=["xlsx"])
u_erp = st.sidebar.file_uploader("Upload ERP", type=["xlsx"])

if u_wms and u_erp:
    df_novo = processar_dados(u_wms, u_erp)
    if st.sidebar.button("🚀 ENVIAR PARA NUVEM"):
        if salvar_no_banco(df_novo):
            st.sidebar.success("Dados Online Atualizados!")
            st.rerun()
    df_base = df_novo

if df_base is not None:
    st.write("### 🛠️ Filtros")
    c1, c2, c3 = st.columns(3)
    with c1:
        f_emp = st.radio("🏢 Empresa", ["Todas"] + sorted(df_base['Empresa'].unique().tolist()), horizontal=True)
    df_f1 = df_base if f_emp == "Todas" else df_base[df_base['Empresa'] == f_emp]
    with c2:
        f_fil = st.radio("📍 Filial", ["Todas"] + sorted(df_f1['Filial'].unique().tolist()), horizontal=True)
    df_f2 = df_f1 if f_fil == "Todas" else df_f1[df_f1['Filial'] == f_fil]
    with c3:
        f_stat = st.radio("✔️ Status", ["Todos"] + sorted(df_f2['Status'].unique().tolist()), horizontal=True)
    
    dff_parcial = df_f2 if f_stat == "Todos" else df_f2[df_f2['Status'] == f_stat]
    f_code = st.text_input("🔍 CONSULTA POR CÓDIGO", placeholder="Digite o código...")
    
    dff = dff_parcial.copy()
    if f_code: dff = dff[dff['Produto'].astype(str).str.contains(f_code, na=False)]

    tab1, tab2 = st.tabs(["📄 Consulta Planilha", "📊 Indicadores"])

    with tab1:
        st.dataframe(dff.style.format({'Saldo ERP (Total)': '{:,.0f}', 'Saldo ERP (Rateado)': '{:,.2f}', 'Saldo WMS': '{:,.2f}', 'Divergência': '{:,.2f}', 'C Unitario': 'R$ {:,.4f}', 'Vl Divergência': 'R$ {:,.2f}', 'Vl Total ERP': 'R$ {:,.2f}'}, decimal=',', thousands='.'), use_container_width=True)
        def to_excel(df):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Auditoria')
            return output.getvalue()
        st.download_button("📥 Baixar Planilha (XLSX)", to_excel(dff), "auditoria_i9.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    with tab2:
        v_total = dff['Vl Total ERP'].sum()
        v_div = dff['Vl Divergência'].sum()
        ac_v = (1 - (dff['Vl Divergência'].abs().sum() / v_total)) * 100 if v_total > 0 else 0
        df_unq = dff.drop_duplicates(subset=['Empresa', 'Filial', 'Armazem', 'Produto'])
        total_it = len(df_unq)
        it_div = len(df_unq[df_unq['Status'] == "Divergente"])
        ac_it = (1 - (it_div / total_it)) * 100 if total_it > 0 else 0

        st.markdown("#### 💰 Financeiro")
        k1, k2, k3 = st.columns(3)
        k1.metric("Valor em Estoque", f"R$ {formatar_br(v_total)}")
        k2.metric("Valor Divergente", f"R$ {formatar_br(v_div)}")
        k3.metric("Acuracidade Valor", f"{ac_v:.2f}%")

        st.markdown("#### 📦 Itens")
        k4, k5, k6 = st.columns(3)
        k4.metric("Total Itens", total_it)
        k5.metric("Divergentes", it_div)
        k6.metric("Acuracidade Itens", f"{ac_it:.2f}%")
else:
    st.info("💡 Banco na nuvem vazio. Carregue os arquivos XLSX na lateral.")
