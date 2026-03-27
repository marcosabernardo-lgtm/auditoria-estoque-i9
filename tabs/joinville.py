import streamlit as st

def render(df, estilizar_func, excel_func):
    st.subheader("Auditoria - Unidades Joinville")
    if not df.empty:
        st.dataframe(estilizar_func(df), use_container_width=True, hide_index=True)
        st.download_button(
            label="📥 Baixar Excel Joinville",
            data=excel_func(df),
            file_name="auditoria_joinville.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("Produto não encontrado em Joinville.")
