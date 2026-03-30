import streamlit as st
import pandas as pd

def render(df, estilizar_func, excel_func, titulo="Unidades Joinville", excel_nome="auditoria_joinville"):
    st.subheader(f"Auditoria - {titulo}")
    if df.empty:
        st.info("Produto não encontrado.")
        return

    # ── Cabeçalho: aviso + Excel no topo direito ──────────────────────────
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

    # ── Tabela com seleção de linha ───────────────────────────────────────
    evento = st.dataframe(
        estilizar_func(df),
        use_container_width=True,
        hide_index=False,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "Qtd Locais": st.column_config.NumberColumn("Qtd Locais", help="Localizações WMS", format="%d"),
            "Saldo ERP (Total)": st.column_config.NumberColumn("Saldo ERP (Total)", help="Saldo total no ERP", format="%.2f"),
        }
    )

    # ── Linha selecionada → busca movimentos do produto ───────────────────
    linhas_sel = evento.selection.get("rows", []) if evento and hasattr(evento, "selection") else []
    if linhas_sel:
        row     = df.iloc[linhas_sel[0]]
        produto = str(row.get("Produto", "")).strip().zfill(6)

        st.markdown(f"#### 🕒 Movimentações — Produto `{produto}`")

        col_sel, col_btn = st.columns([3, 1])
        with col_sel:
            st.caption(f"Última entrada e saída por filial para o produto selecionado.")
        with col_btn:
            if st.button("🔎 Ver na aba Movimentações", key=f"btn_nav_{excel_nome}", use_container_width=True):
                st.session_state["f_code_global"] = produto
                st.session_state["nav_para_mov"]  = True
                st.rerun()

        # Busca movimentos diretamente via session_state de engine
        engine = st.session_state.get("_engine")
        buscar = st.session_state.get("_buscar_func")
        estilizar = st.session_state.get("_estilizar_func")

        if engine and buscar and estilizar:
            with st.spinner("Buscando movimentações..."):
                try:
                    from movimentacoes import _tratar_df
                    to_float = st.session_state.get("_to_float_func")
                    df_mov = buscar(engine, produto)
                    if not df_mov.empty:
                        df_mov_trat = _tratar_df(df_mov, to_float, deduplicar=True)
                        st.dataframe(estilizar(df_mov_trat), use_container_width=True, hide_index=True)
                    else:
                        st.info("Nenhuma movimentação encontrada para este produto.")
                except Exception as e:
                    st.warning(f"Não foi possível carregar movimentações: {e}")
