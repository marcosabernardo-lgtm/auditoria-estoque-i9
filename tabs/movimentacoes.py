import streamlit as st
import pandas as pd

COLUNAS_EXIBIR = [
    "Empresa", "Filial", "Tipo Movimento", "Documento", "Digitação",
    "Nota Devolução", "Produto", "Descrição", "Centro Custo",
    "Razão Social", "Qtd", "Vl Unit", "Vl Total",
]


def _tratar_df(df_nf, to_float_func):
    """Aplica todas as transformações de limpeza e renomeação."""
    df_nf = df_nf.drop_duplicates()

    if "DIGITACAO" in df_nf.columns:
        df_nf["DIGITACAO"] = pd.to_datetime(df_nf["DIGITACAO"], errors="coerce")
        df_nf = df_nf.sort_values("DIGITACAO", ascending=False)
        df_nf = df_nf.drop_duplicates(subset=["Empresa_Filial_Nome", "TIPOMOVIMENTO"], keep="first")
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


def render(f_code_padded, engine, buscar_func, buscar_geral_func, estilizar_func, to_float_func):
    st.markdown("### 🕒 Movimentações — Última Entrada e Saída por Filial")

    # ── Sem código: mostra última entrada e última saída geral ────────────────
    if not f_code_padded:
        st.caption("Exibindo a última entrada e a última saída registradas no banco.")
        try:
            df_geral = buscar_geral_func(engine)
            if df_geral.empty:
                st.info("💡 Nenhuma movimentação encontrada no banco.")
                return
            df_exibir = _tratar_df(df_geral, to_float_func)
            st.dataframe(estilizar_func(df_exibir), use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Erro ao buscar movimentações: {e}")
        return

    # ── Com código: busca movimentações do produto ────────────────────────────
    try:
        df_nf = buscar_func(engine, f_code_padded)
    except Exception as e:
        st.error(f"Erro ao consultar movimentações: {e}")
        return

    if df_nf.empty:
        st.warning("Nenhuma movimentação encontrada para o código informado.")
        return

    df_exibir = _tratar_df(df_nf, to_float_func)
    st.caption(f"Produto: **{f_code_padded}** — última entrada e saída por filial")
    st.dataframe(estilizar_func(df_exibir), use_container_width=True, hide_index=True)
