import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import date

# ── Helpers ──────────────────────────────────────────────────────────────────

def calcular_score(df):
    df = df.copy()

    # Garante tipos numéricos
    for col in ["Saldo ERP (Total)", "Saldo WMS", "Vl Unit", "Vl Total ERP", "Divergência"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ── Curva ABC por valor total (peso 30%) ──────────────────────────────────
    df = df.sort_values("Vl Total ERP", ascending=False).reset_index(drop=True)
    total_valor = df["Vl Total ERP"].sum()
    if total_valor > 0:
        df["pct_acum"] = df["Vl Total ERP"].cumsum() / total_valor
    else:
        df["pct_acum"] = 0
    df["Curva ABC"] = np.where(df["pct_acum"] <= 0.80, "A",
                      np.where(df["pct_acum"] <= 0.95, "B", "C"))
    df["score_abc"] = df["Curva ABC"].map({"A": 10, "B": 6, "C": 3})

    # ── Divergência WMS x ERP (peso 25%) ─────────────────────────────────────
    df["score_diverg"] = np.where(df["Divergência"] != 0, 10, 0)

    # ── Valor em estoque normalizado (peso 25%) ───────────────────────────────
    max_vl = df["Vl Total ERP"].max() or 1
    df["score_valor"] = (df["Vl Total ERP"] / max_vl * 10).round(2)

    # ── Dias sem contagem (peso 20%) ──────────────────────────────────────────
    # Começa zerado — será alimentado conforme uso
    df["Dias s/ Contagem"] = 0
    df["score_dias"] = 2  # todos partem do mesmo patamar no início

    # ── Score final normalizado 0–10 ──────────────────────────────────────────
    raw = (
        0.30 * df["score_abc"] +
        0.25 * df["score_diverg"] +
        0.25 * df["score_valor"] +
        0.20 * df["score_dias"]
    )
    max_raw = raw.max() or 1
    df["Score"] = (raw / max_raw * 10).round(2)

    # ── Motivo principal ──────────────────────────────────────────────────────
    def motivo(row):
        razoes = []
        if row["Curva ABC"] == "A":
            razoes.append("Curva A")
        if row["Divergência"] != 0:
            razoes.append("Divergência")
        if row["Vl Total ERP"] > 0:
            razoes.append(f"R$ {row['Vl Total ERP']:,.0f}")
        return " · ".join(razoes) if razoes else "Valor em estoque"

    df["Motivo"] = df.apply(motivo, axis=1)
    df = df.sort_values("Score", ascending=False).reset_index(drop=True)
    df.index = df.index + 1

    return df


def gerar_xlsx(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=True, index_label="Ranking", sheet_name="Inv. Cíclico")
        wb = writer.book
        ws = writer.sheets["Inv. Cíclico"]

        fmt_header = wb.add_format({
            "bold": True, "bg_color": "#004550", "font_color": "#FFFFFF",
            "border": 1, "border_color": "#EC6E21", "align": "center",
            "valign": "vcenter"
        })
        fmt_A = wb.add_format({"bg_color": "#FFF3CD", "border": 1, "border_color": "#dee2e6"})
        fmt_B = wb.add_format({"bg_color": "#D1ECF1", "border": 1, "border_color": "#dee2e6"})
        fmt_C = wb.add_format({"bg_color": "#F8F9FA", "border": 1, "border_color": "#dee2e6"})

        for col_num, col_name in enumerate(["Ranking"] + list(df.columns)):
            ws.write(0, col_num, col_name, fmt_header)
            ws.set_column(col_num, col_num, max(len(str(col_name)) + 4, 14))

        for row_num, (idx, row) in enumerate(df.iterrows(), start=1):
            fmt = fmt_A if row.get("Curva ABC") == "A" else \
                  fmt_B if row.get("Curva ABC") == "B" else fmt_C
            ws.write(row_num, 0, idx, fmt)
            for col_num, val in enumerate(row, start=1):
                ws.write(row_num, col_num, val, fmt)

    output.seek(0)
    return output.getvalue()


# ── Render principal ──────────────────────────────────────────────────────────

def render(df_jlle, df_outras, formatar_br):
    st.markdown("## 🔄 Inventário Cíclico")
    st.markdown("Geração inteligente de listas de contagem baseada em score de priorização.")

    # Junta Joinville + Outras filiais
    df_total = pd.concat([df_jlle, df_outras], ignore_index=True)

    if df_total.empty:
        st.warning("Nenhum dado de auditoria encontrado. Carregue os dados na sidebar.")
        return

    # Calcula score
    df_score = calcular_score(df_total)

    # ── Métricas resumo ───────────────────────────────────────────────────────
    total_skus = len(df_score)
    total_diverg = int((df_score["Divergência"] != 0).sum())
    valor_total = df_score["Vl Total ERP"].sum()
    skus_A = int((df_score["Curva ABC"] == "A").sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total de SKUs", f"{total_skus:,}")
    c2.metric("SKUs Divergentes", f"{total_diverg:,}")
    c3.metric("SKUs Curva A", f"{skus_A:,}")
    c4.metric("Valor Total Estoque", f"R$ {formatar_br(valor_total)}")

    st.markdown("---")

    # ── Filtros ───────────────────────────────────────────────────────────────
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        empresas = ["Todas"] + sorted(df_score["Empresa"].dropna().unique().tolist())
        empresa_sel = st.selectbox("🏢 Empresa", empresas, key="ic_empresa")
    with col_f2:
        if empresa_sel != "Todas":
            filiais_disp = ["Todas"] + sorted(
                df_score[df_score["Empresa"] == empresa_sel]["Filial"].dropna().unique().tolist()
            )
        else:
            filiais_disp = ["Todas"] + sorted(df_score["Filial"].dropna().unique().tolist())
        filial_sel = st.selectbox("📍 Filial", filiais_disp, key="ic_filial")
    with col_f3:
        curva_sel = st.multiselect("📊 Curva ABC", ["A", "B", "C"], default=["A", "B", "C"], key="ic_curva")

    df_filtrado = df_score.copy()
    if empresa_sel != "Todas":
        df_filtrado = df_filtrado[df_filtrado["Empresa"] == empresa_sel]
    if filial_sel != "Todas":
        df_filtrado = df_filtrado[df_filtrado["Filial"] == filial_sel]
    if curva_sel:
        df_filtrado = df_filtrado[df_filtrado["Curva ABC"].isin(curva_sel)]

    total_filtrado = len(df_filtrado)

    # ── Seleção de quantidade ─────────────────────────────────────────────────
    st.markdown("### Quantidade de itens para o ciclo")
    modo = st.radio(
        "Modo",
        ["Quantidade fixa", "Percentual do estoque"],
        horizontal=True,
        key="ic_modo"
    )

    if modo == "Quantidade fixa":
        col_b1, col_b2, col_b3, col_b4 = st.columns(4)
        qtd_opcoes = {col_b1: 30, col_b2: 50, col_b3: 80, col_b4: 100}
        if "ic_qtd" not in st.session_state:
            st.session_state.ic_qtd = 50
        for col, qtd in qtd_opcoes.items():
            with col:
                if st.button(f"{qtd} itens", key=f"ic_btn_{qtd}",
                             type="primary" if st.session_state.ic_qtd == qtd else "secondary"):
                    st.session_state.ic_qtd = qtd
        qtd_final = min(st.session_state.ic_qtd, total_filtrado)

    else:
        pct_opcoes = {
            "5% — Críticos": 0.05,
            "10% — Alta prioridade": 0.10,
            "20% — Rotina": 0.20,
            "30% — Controle amplo": 0.30,
        }
        pct_label = st.select_slider(
            "Faixa percentual",
            options=list(pct_opcoes.keys()),
            value="10% — Alta prioridade",
            key="ic_pct"
        )
        pct_val = pct_opcoes[pct_label]
        qtd_final = max(1, int(total_filtrado * pct_val))
        st.caption(f"→ {qtd_final} itens selecionados de {total_filtrado} disponíveis")

    # ── Tabela resultado ──────────────────────────────────────────────────────
    st.markdown(f"### Lista do ciclo — Top {qtd_final} itens por score")

    colunas_exibir = [
        "Produto", "Descrição", "Empresa", "Filial",
        "Curva ABC", "Score", "Saldo ERP (Total)", "Saldo WMS",
        "Divergência", "Vl Total ERP", "Motivo"
    ]
    colunas_ok = [c for c in colunas_exibir if c in df_filtrado.columns]
    df_exibir = df_filtrado[colunas_ok].head(qtd_final)

    st.dataframe(
        df_exibir.style.apply(
            lambda r: ["background-color: #005562; color: #ffffff; font-size: 0.84rem;"] * len(r), axis=1
        ).set_table_styles([
            {"selector": "thead th", "props": [
                ("background-color", "#004550"), ("color", "#ffffff"),
                ("border-bottom", "2px solid #EC6E21"), ("text-transform", "uppercase")
            ]},
            {"selector": "td", "props": [
                ("padding", "8px 12px"), ("border-bottom", "1px solid rgba(255,255,255,0.05)")
            ]}
        ]).format({
            "Saldo ERP (Total)": "{:,.2f}",
            "Saldo WMS": "{:,.2f}",
            "Divergência": "{:,.2f}",
            "Vl Total ERP": "R$ {:,.2f}",
            "Score": "{:.2f}",
        }, na_rep="-"),
        use_container_width=True,
        hide_index=False,
    )

    # ── Exportação ────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Exportar lista para contagem")
    col_ex1, col_ex2 = st.columns([2, 1])
    with col_ex1:
        st.caption(f"O arquivo gerado contém {qtd_final} itens ordenados por score de prioridade.")
    with col_ex2:
        nome_arquivo = f"inventario_ciclico_{date.today().strftime('%d%m%Y')}.xlsx"
        st.download_button(
            label="📥 Baixar Excel para Contagem",
            data=gerar_xlsx(df_exibir),
            file_name=nome_arquivo,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )