import streamlit as st
import pandas as pd

def render(df, estilizar_func, excel_func):
    st.subheader("Auditoria - Unidades Joinville")
    if not df.empty:
        # Destaque informativo sobre produtos com múltiplas localizações
        if "Qtd Locais" in df.columns:
            multi = int((df["Qtd Locais"] > 1).sum())
            total_prod = df["Produto"].nunique()
            if multi > 0:
                st.caption(
                    f"ℹ️ **{total_prod}** produtos únicos · "
                    f"**{multi}** linhas com produto em múltiplos locais WMS — "
                    f"coluna **Saldo ERP** reflete o total do produto (mesmo valor em todas as localizações)."
                )

        st.dataframe(
            estilizar_func(df),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Qtd Locais": st.column_config.NumberColumn(
                    "Qtd Locais",
                    help="Número de localizações WMS onde este produto está armazenado",
                    format="%d",
                ),
                "Saldo ERP": st.column_config.NumberColumn(
                    "Saldo ERP",
                    help="Saldo total no ERP — igual em todas as linhas do mesmo produto",
                    format="%.2f",
                ),
            }
        )
        st.download_button(
            label="📥 Baixar Excel Joinville",
            data=excel_func(df),
            file_name="auditoria_joinville.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("Produto não encontrado em Joinville.")
