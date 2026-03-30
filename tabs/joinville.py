import streamlit as st
import pandas as pd

def render(df, estilizar_func, excel_func, titulo="Unidades Joinville", excel_nome="auditoria_joinville"):
    st.subheader(f"Auditoria - {titulo}")
    if df.empty:
        st.info("Produto não encontrado.")
        return

    # Aviso de múltiplos locais
    if "Qtd Locais" in df.columns:
        multi = int((df["Qtd Locais"] > 1).sum())
        total_prod = df["Produto"].nunique()
        if multi > 0:
            st.caption(
                f"ℹ️ **{total_prod}** produtos únicos · "
                f"**{multi}** linhas com produto em múltiplos locais WMS — "
                f"coluna **Saldo ERP (Total)** reflete o total do produto."
            )

    # Tabela com seleção de linha
    evento = st.dataframe(
        estilizar_func(df),
        use_container_width=True,
        hide_index=False,
        on_select="rerun",
        selection_mode="single-row",
        column_config={
            "Qtd Locais": st.column_config.NumberColumn(
                "Qtd Locais",
                help="Número de localizações WMS onde este produto está armazenado",
                format="%d",
            ),
            "Saldo ERP (Total)": st.column_config.NumberColumn(
                "Saldo ERP (Total)",
                help="Saldo total no ERP — igual em todas as linhas do mesmo produto",
                format="%.2f",
            ),
            "Últ. Movimento": st.column_config.TextColumn("Últ. Movimento"),
            "Data Últ. Mov.": st.column_config.TextColumn("Data Últ. Mov."),
            "Doc. Últ. Mov.": st.column_config.TextColumn(
                "Doc. Últ. Mov.",
                help="Clique na linha para ver todas as movimentações deste documento",
            ),
        }
    )

    # Linha selecionada → oferece navegar para Movimentações
    linhas_sel = evento.selection.get("rows", []) if evento and hasattr(evento, "selection") else []
    if linhas_sel:
        idx = linhas_sel[0]
        row = df.iloc[idx]
        doc = str(row.get("Doc. Últ. Mov.", "")).strip()
        produto = str(row.get("Produto", "")).strip()

        col_info, col_btn = st.columns([3, 1])
        with col_info:
            st.markdown(
                f"**Linha selecionada:** Produto `{produto}` · "
                f"Últ. Doc: `{doc if doc else 'sem documento'}`"
            )
        with col_btn:
            if doc and doc not in ("", "nan", "000000000"):
                if st.button(f"🔎 Ver movimentações", key="btn_nav_mov_auditoria_joinville"):
                    st.session_state["mov_doc_busca_value"] = doc
                    st.session_state["nav_para_mov"] = True
                    st.rerun()

    # Exportação
    col_ex1, col_ex2 = st.columns([3, 1])
    with col_ex2:
        st.download_button(
            label=f"📥 Baixar Excel Unidades Joinville",
            data=excel_func(df),
            file_name="auditoria_joinville.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
