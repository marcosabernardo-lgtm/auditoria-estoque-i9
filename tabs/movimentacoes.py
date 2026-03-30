import streamlit as st
import pandas as pd

COLUNAS_EXIBIR = [
    "Empresa", "Filial", "Tipo Movimento", "Documento", "Digitação",
    "Nota Devolução", "Produto", "Descrição", "Centro Custo",
    "Razão Social", "Qtd", "Vl Unit", "Vl Total",
]


def _tratar_df(df_nf, to_float_func, deduplicar=True):
    """Limpeza, renomeação e opcionalmente deduplicação por filial+tipo."""
    df_nf = df_nf.drop_duplicates()

    if "DIGITACAO" in df_nf.columns:
        df_nf["DIGITACAO"] = pd.to_datetime(df_nf["DIGITACAO"], errors="coerce")
        df_nf = df_nf.sort_values("DIGITACAO", ascending=False)
        if deduplicar:
            df_nf = df_nf.drop_duplicates(
                subset=["Empresa_Filial_Nome", "TIPOMOVIMENTO"], keep="first"
            )
        df_nf["DIGITACAO"] = df_nf["DIGITACAO"].dt.strftime("%d/%m/%Y")

    if "Empresa_Filial_Nome" in df_nf.columns:
        split = df_nf["Empresa_Filial_Nome"].str.split(" - ", n=1, expand=True)
        df_nf.insert(0, "Filial",  split[1].fillna("") if split.shape[1] > 1 else "")
        df_nf.insert(0, "Empresa", split[0].fillna(""))
        df_nf = df_nf.drop(columns=["Empresa_Filial_Nome"])

    df_nf = df_nf.rename(columns={
        "TIPOMOVIMENTO":  "Tipo Movimento",
        "DOCUMENTO":      "Documento",
        "DIGITACAO":      "Digitação",
        "NOTA_DEVOLUCAO": "Nota Devolução",
        "PRODUTO":        "Produto",
        "DESCRICAO":      "Descrição",
        "CENTRO_CUSTO":   "Centro Custo",
        "RAZAO_SOCIAL":   "Razão Social",
        "QUANTIDADE":     "Qtd",
        "PRECO_UNITARIO": "Vl Unit",
        "TOTAL":          "Vl Total",
    })

    if "Nota Devolução" in df_nf.columns:
        df_nf["Nota Devolução"] = (
            df_nf["Nota Devolução"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
            .replace({"None": "", "nan": ""})
        )
        df_nf["Nota Devolução"] = df_nf["Nota Devolução"].apply(
            lambda x: x.zfill(9) if x not in ("", "0" * 9) else ""
        )

    if "Centro Custo" in df_nf.columns:
        df_nf["Centro Custo"] = (
            df_nf["Centro Custo"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
            .replace({"None": "", "nan": ""})
        )

    for col in ["Vl Unit", "Vl Total"]:
        if col in df_nf.columns:
            df_nf[col] = to_float_func(df_nf[col])

    colunas_ok = [c for c in COLUNAS_EXIBIR if c in df_nf.columns]
    return df_nf[colunas_ok]


def render(f_code_padded, engine, buscar_func, buscar_doc_func, estilizar_func, to_float_func, doc_inicial=None):
    st.markdown("### 🕒 Movimentações")

    # ── Dois campos de busca lado a lado ─────────────────────────────────────
    col_cod, col_doc = st.columns(2)
    with col_cod:
        st.caption("🔍 Busca por Código de Produto")
        st.caption(f"Produto atual: **{f_code_padded}**" if f_code_padded else "Nenhum produto selecionado")
    with col_doc:
        # doc_inicial vem do clique na tabela Joinville/Filiais
        valor_inicial = doc_inicial or st.session_state.get("mov_doc_busca", "")
        doc_busca = st.text_input(
            "🔎 Busca por Documento",
            value=valor_inicial,
            placeholder="Digite parte do número do documento...",
            key="mov_doc_busca"
        )

    # ── Prioridade: documento > código ───────────────────────────────────────
    if doc_busca:
        st.markdown(f"#### Movimentações do documento: `{doc_busca}`")
        try:
            df_nf = buscar_doc_func(engine, doc_busca)
        except Exception as e:
            st.error(f"Erro ao buscar por documento: {e}")
            return

        if df_nf.empty:
            st.warning(f"Nenhuma movimentação encontrada para o documento **{doc_busca}**.")
            return

        # Todas as linhas — sem deduplicar
        df_exibir = _tratar_df(df_nf, to_float_func, deduplicar=False)
        st.caption(f"{len(df_exibir)} linha(s) encontrada(s)")
        st.dataframe(estilizar_func(df_exibir), use_container_width=True, hide_index=True)

    elif f_code_padded:
        st.markdown(f"#### Última entrada e saída por filial — Produto `{f_code_padded}`")
        try:
            df_nf = buscar_func(engine, f_code_padded)
        except Exception as e:
            st.error(f"Erro ao consultar movimentações: {e}")
            return

        if df_nf.empty:
            st.warning("Nenhuma movimentação encontrada para o código informado.")
            return

        # Deduplicado: última entrada e última saída por filial
        df_exibir = _tratar_df(df_nf, to_float_func, deduplicar=True)
        st.dataframe(estilizar_func(df_exibir), use_container_width=True, hide_index=True)

    else:
        st.info("💡 Digite um código de produto no campo **Consulta por Código** acima, ou busque por número de documento no campo ao lado.")
