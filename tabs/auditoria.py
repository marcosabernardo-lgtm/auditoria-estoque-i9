import streamlit as st

def render(df, estilizar_func, excel_func, titulo="Unidades Joinville", excel_nome="auditoria_joinville"):
    st.subheader(f"Auditoria - {titulo}")
    if df.empty:
        st.info("Produto não encontrado.")
        return

    # ── Cabeçalho ─────────────────────────────────────────────────────────
    col_info, col_excel = st.columns([4, 1])
    with col_info:
        if "Qtd Locais" in df.columns:
            multi      = int((df["Qtd Locais"] > 1).sum())
            total_prod = df["Produto"].nunique()
            if multi > 0:
                st.caption(
                    f"ℹ️ **{total_prod}** produtos únicos · "
                    f"**{multi}** linhas em múltiplos locais WMS — "
                    f"**Saldo ERP (Total)** reflete o total do produto."
                )
    with col_excel:
        st.download_button(
            label="📥 Baixar Excel",
            data=excel_func(df),
            file_name=f"{excel_nome}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    # ── Tabela ────────────────────────────────────────────────────────────
    st.dataframe(
        estilizar_func(df),
        use_container_width=True,
        hide_index=False,
        column_config={
            "Qtd Locais": st.column_config.NumberColumn("Qtd Locais", format="%d"),
            "Saldo ERP (Total)": st.column_config.NumberColumn("Saldo ERP (Total)", format="%.2f"),
        }
    )
