import streamlit as st

def render(df, estilizar_func, excel_func, titulo="Outras Filiais", excel_nome="auditoria_filiais"):
    st.subheader(f"Auditoria - {titulo}")
    if df.empty:
        st.info("Produto não encontrado.")
        return

    col_info, col_excel = st.columns([4, 1])
    with col_info:
        if "Qtd Locais" in df.columns:
            multi      = int((df["Qtd Locais"] > 1).sum())
            total_prod = df["Produto"].nunique()
            if multi > 0:
                st.caption(
                    f"ℹ️ **{total_prod}** produtos únicos · "
                    f"**{multi}** linhas com produto em múltiplos locais WMS — "
                    f"coluna **Saldo ERP (Total)** reflete o total do produto."
                )
    with col_excel:
        st.download_button(
            label="📥 Baixar Excel",
            data=excel_func(df),
            file_name=f"{excel_nome}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    evento = st.dataframe(
        estilizar_func(df),
        use_container_width=True,
        hide_index=False,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "Qtd Locais": st.column_config.NumberColumn("Qtd Locais", help="Localizações WMS", format="%d"),
            "Saldo ERP (Total)": st.column_config.NumberColumn("Saldo ERP (Total)", help="Saldo total no ERP", format="%.2f"),
            "Últ. Movimento": st.column_config.TextColumn("Últ. Movimento"),
            "Data Últ. Mov.": st.column_config.TextColumn("Data Últ. Mov."),
            "Doc. Últ. Mov.": st.column_config.TextColumn("Doc. Últ. Mov.", help="Clique na linha para ver movimentações"),
        }
    )

    linhas_sel = evento.selection.get("rows", []) if evento and hasattr(evento, "selection") else []
    if linhas_sel:
        row     = df.iloc[linhas_sel[0]]
        produto = str(row.get("Produto", "")).strip().zfill(6)
        doc     = str(row.get("Doc. Últ. Mov.", "")).strip()

        col_sel, col_btn = st.columns([3, 1])
        with col_sel:
            st.markdown(
                f"**Selecionado:** Produto `{produto}` · "
                f"Últ. Doc: `{doc if doc not in ('', 'nan', '000000000') else '—'}`"
            )
        with col_btn:
            if st.button("🕒 Ver Movimentações", key="btn_mov_fil", use_container_width=True):
                st.session_state["f_code_global"] = produto
                st.session_state["nav_para_mov"]  = True
                st.rerun()
