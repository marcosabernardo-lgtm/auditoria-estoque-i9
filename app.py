import streamlit as st
import pandas as pd
import io
import os
from sqlalchemy import create_engine
# IMPORTANDO O SEU NOVO ARQUIVO
from processador_movs import tratar_notas_fiscais, buscar_movimentacoes_nuvem

# Configuração da página
st.set_page_config(page_title="Auditoria I9 - Premium", layout="wide")

# (Aqui você mantém o seu CSS dos Cards e Filtros que já definimos...)

# --- CONEXÃO BANCO ---
def get_engine():
    try:
        conn_url = st.secrets["connections"]["postgresql"]["url"]
        return create_engine(conn_url, connect_args={"options": "-c fts.prepare_threshold=0"})
    except: return None

def salvar_no_banco(df, tabela):
    engine = get_engine()
    if engine:
        df.to_sql(tabela, engine, if_exists='replace', index=False)
        return True
    return False

# (A função carregar_auditoria() continua aqui normal...)

# --- INTERFACE ---
st.title("📊 Gestão de Estoque e Movimentações I9")

# Criando as abas
tab_auditoria, tab_indicadores, tab_movs = st.tabs(["📄 Consulta Auditoria", "📈 Indicadores", "🚚 Entradas e Saídas"])

# --- SIDEBAR (Uploads separados) ---
with st.sidebar:
    st.header("⚙️ Atualizar Bases")
    with st.expander("1. Auditoria (WMS/ERP)"):
        # Seus campos de upload da auditoria...
        pass
    
    with st.expander("2. Movimentações (Pasta bd_entradas)"):
        u_movs = st.file_uploader("Arraste os XLSX da pasta", type=["xlsx"], accept_multiple_files=True)
        if u_movs and st.button("🚀 Processar e Enviar Notas"):
            with st.spinner("Limpando dados e enviando para nuvem..."):
                df_nf = tratar_notas_fiscais(u_movs)
                if salvar_no_banco(df_nf, 'movimentacoes'):
                    st.success(f"Banco atualizado com {len(df_nf)} registros!")

# --- LÓGICA DE FILTROS E EXIBIÇÃO ---
# (Aqui você mantém os filtros de bolinha que já criamos para a Auditoria...)

# --- ABA 3: A NOVA ABA DE MOVIMENTAÇÕES ---
with tab_movs:
    st.markdown("### 🚚 Histórico de Entradas e Saídas")
    # Pega o código que você digitou na barra de busca global
    # (Supondo que a variável do campo de busca chame-se f_code)
    if 'f_code' in locals() and len(f_code) >= 3:
        df_historico = buscar_movimentacoes_nuvem(get_engine(), f_code)
        if not df_historico.empty:
            st.write(f"Movimentações para o código: **{f_code}**")
            st.dataframe(df_historico, use_container_width=True)
        else:
            st.warning("Nenhuma nota encontrada para este código.")
    else:
        st.info("Digite um código de produto na busca acima para ver o histórico de notas.")

# (O restante do código das outras abas permanece o mesmo)
