import streamlit as st

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

    # ── Linha selecionada → exibe movimentações inline ────────────────────
    linhas_sel = evento.selection.get("rows", []) if evento and hasattr(evento, "selection") else []
    if not linhas_sel:
        return

    row     = df.iloc[linhas_sel[0]]
    produto = str(row.get("Produto", "")).strip().zfill(6)

    # Recupera dependências injetadas pelo app.py
    engine   = st.session_state.get("_engine")
    buscar   = st.session_state.get("_buscar_func")
    buscar_doc = st.session_state.get("_buscar_doc_func")
    estilizar  = st.session_state.get("_estilizar_func")
    tratar     = st.session_state.get("_tratar_df")
    to_float   = st.session_state.get("_to_float_func")

    if not all([engine, buscar, buscar_doc, estilizar, tratar, to_float]):
        st.warning("Dependências não carregadas. Recarregue a página.")
        return

    with st.spinner("Buscando movimentações..."):
        try:
            df_mov = buscar(engine, produto)
        except Exception as e:
            st.error(f"Erro ao buscar movimentações: {e}")
            return

    if df_mov.empty:
        st.info(f"Nenhuma movimentação encontrada para o produto `{produto}`.")
        return

    df_mov_trat = tratar(df_mov, to_float, deduplicar=True)

    # Pega o documento da última movimentação
    doc = ""
    if "Documento" in df_mov_trat.columns:
        doc = str(df_mov_trat["Documento"].iloc[0]).strip()

    # Duas seções lado a lado: resumo do produto + botão buscar nota
    col_mov, col_nota = st.columns([3, 2])

    with col_mov:
        st.markdown(f"#### 🕒 Movimentações — Produto `{produto}`")
        st.caption("Última entrada e saída por filial.")
        st.dataframe(estilizar(df_mov_trat), use_container_width=True, hide_index=True)

    with col_nota:
        st.markdown(f"#### 📄 Nota Fiscal")
        if doc and doc not in ("", "nan", "000000000"):
            st.caption(f"Documento: **{doc}**")
            if st.button(f"🔎 Ver todas as linhas da Nota {doc}", key=f"btn_nota_{excel_nome}", use_container_width=True):
                st.session_state[f"nota_aberta_{excel_nome}"] = doc

    # Exibe todas as linhas da nota se solicitado
    nota_aberta = st.session_state.get(f"nota_aberta_{excel_nome}", "")
    if nota_aberta:
        st.markdown(f"---")
        st.markdown(f"#### 📋 Movimentações — Nota `{nota_aberta}`")
        with st.spinner("Buscando nota..."):
            try:
                df_nota = buscar_doc(engine, nota_aberta)
                if not df_nota.empty:
                    df_nota_trat = tratar(df_nota, to_float, deduplicar=False)
                    st.caption(f"{len(df_nota_trat)} linha(s) encontrada(s) nesta nota.")
                    st.dataframe(estilizar(df_nota_trat), use_container_width=True, hide_index=True)
                else:
                    st.info(f"Nenhuma linha encontrada para a nota `{nota_aberta}`.")
            except Exception as e:
                st.error(f"Erro ao buscar nota: {e}")

        if st.button("✖ Fechar nota", key=f"btn_fechar_{excel_nome}"):
            st.session_state[f"nota_aberta_{excel_nome}"] = ""
            st.rerun()
