import streamlit as st

def render(df_jlle, formatar_br_func):
    if not df_jlle.empty:
        v_total = df_jlle["Vl Total ERP"].sum()
        v_err = df_jlle["Vl Divergência"].abs().sum()
        ac_v = (1 - (v_err/v_total))*100 if v_total > 0 else 0
        
        df_unq = df_jlle.drop_duplicates(subset=["Empresa", "Filial", "Armazem", "Produto"])
        
        st.markdown('### 💰 Financeiro (Joinville)')
        k1, k2, k3 = st.columns(3)
        k1.metric("ESTOQUE TOTAL", f"R$ {v_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        k2.metric("VALOR DIVERGENTE", f"R$ {v_err:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        k3.metric("ACURACIDADE VALOR", f"{ac_v:.2f}%")
        
        st.markdown('### 📦 Itens (Joinville)')
        k4, k5, k6 = st.columns(3)
        k4.metric("TOTAL DE ITENS", f"{len(df_unq):,}".replace(",", "."))
        k5.metric("ITENS DIVERGENTES", f"{len(df_unq[df_unq['Status'] == 'Divergente']):,}".replace(",", "."))
        
        div_it = len(df_unq[df_unq['Status'] == 'Divergente'])
        ac_it = (1 - (div_it/len(df_unq)))*100 if len(df_unq) > 0 else 100
        k6.metric("ACURACIDADE ITENS", f"{ac_it:.2f}%")
    else:
        st.warning("Sem dados de Joinville para calcular indicadores.")
