import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import date

# ── Constantes ────────────────────────────────────────────────────────────────

# Empresas presentes no df_jlle (coluna "Empresa")
EMPRESAS_JOINVILLE = ["Maquinas", "Service", "Tools"]

PERIODO_KPMG_DIAS = 365  # Cobertura total exigida em 1 ano


# ── Controle de cobertura (session_state) ─────────────────────────────────────

def _chave_sessao(empresa: str, filial: str) -> str:
    """Gera uma chave única no session_state para cada combinação empresa+filial."""
    raw = f"{empresa}_{filial}".replace(" ", "_").replace("-", "")
    return f"ic_contados_{raw}"


def inicializar_controle(empresa: str, filial: str):
    """Cria o registro de contagem se ainda não existir."""
    chave = _chave_sessao(empresa, filial)
    if chave not in st.session_state:
        st.session_state[chave] = {}  # {produto: data_contagem (str)}


def marcar_contados(empresa: str, filial: str, produtos: list):
    """Registra os produtos como contados hoje."""
    chave = _chave_sessao(empresa, filial)
    hoje = date.today().isoformat()
    for p in produtos:
        st.session_state[chave][p] = hoje


def obter_contados(empresa: str, filial: str) -> dict:
    """Retorna dict {produto: data_contagem}."""
    chave = _chave_sessao(empresa, filial)
    return st.session_state.get(chave, {})


def resetar_contagem(empresa: str, filial: str):
    """Limpa o histórico de contagem (novo período)."""
    chave = _chave_sessao(empresa, filial)
    st.session_state[chave] = {}


# ── Cálculo de score ──────────────────────────────────────────────────────────

def calcular_score(df: pd.DataFrame, contados: dict) -> pd.DataFrame:
    """
    Calcula o score de prioridade de cada SKU.

    Pesos:
      30% Curva ABC (por valor total ERP)
      25% Divergência WMS x ERP
      25% Valor em estoque normalizado
      20% Dias sem contagem (baseado no histórico da sessão)
    """
    df = df.copy()

    # Garante tipos numéricos
    for col in ["Saldo ERP (Total)", "Saldo WMS", "Vl Unit", "Vl Total ERP", "Divergência"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ── Curva ABC por valor total (peso 30%) ──────────────────────────────
    df = df.sort_values("Vl Total ERP", ascending=False).reset_index(drop=True)
    total_valor = df["Vl Total ERP"].sum()
    if total_valor > 0:
        df["pct_acum"] = df["Vl Total ERP"].cumsum() / total_valor
    else:
        df["pct_acum"] = 0
    df["Curva ABC"] = np.where(df["pct_acum"] <= 0.80, "A",
                      np.where(df["pct_acum"] <= 0.95, "B", "C"))
    df["score_abc"] = df["Curva ABC"].map({"A": 10, "B": 6, "C": 3})

    # ── Divergência WMS x ERP (peso 25%) ──────────────────────────────────
    df["score_diverg"] = np.where(df["Divergência"] != 0, 10, 0)

    # ── Valor em estoque normalizado (peso 25%) ───────────────────────────
    max_vl = df["Vl Total ERP"].max() or 1
    df["score_valor"] = (df["Vl Total ERP"] / max_vl * 10).round(2)

    # ── Dias sem contagem (peso 20%) ──────────────────────────────────────
    hoje = date.today()

    def dias_sem_contar(produto):
        if produto in contados:
            try:
                ultima = date.fromisoformat(contados[produto])
                return (hoje - ultima).days
            except Exception:
                return PERIODO_KPMG_DIAS
        return PERIODO_KPMG_DIAS  # nunca contado = pior caso

    df["Dias s/ Contagem"] = df["Produto"].astype(str).apply(dias_sem_contar)
    # Normaliza 0-10: quem tem mais dias sem contar recebe score maior
    max_dias = df["Dias s/ Contagem"].max() or 1
    df["score_dias"] = (df["Dias s/ Contagem"] / max_dias * 10).round(2)

    # ── Score final normalizado 0–10 ──────────────────────────────────────
    raw = (
        0.30 * df["score_abc"] +
        0.25 * df["score_diverg"] +
        0.25 * df["score_valor"] +
        0.20 * df["score_dias"]
    )
    max_raw = raw.max() or 1
    df["Score"] = (raw / max_raw * 10).round(2)

    # ── Já contado? ───────────────────────────────────────────────────────
    df["Já Contado"] = df["Produto"].astype(str).apply(
        lambda p: "✅ Sim" if p in contados else "⬜ Não"
    )

    # ── Motivo principal ──────────────────────────────────────────────────
    def motivo(row):
        razoes = []
        if row["Curva ABC"] == "A":
            razoes.append("Curva A")
        if row["Divergência"] != 0:
            razoes.append("Divergência")
        if row["Dias s/ Contagem"] >= PERIODO_KPMG_DIAS:
            razoes.append("Nunca contado")
        elif row["Dias s/ Contagem"] > 180:
            razoes.append(f"{row['Dias s/ Contagem']}d sem contar")
        if row["Vl Total ERP"] > 0:
            razoes.append(f"R$ {row['Vl Total ERP']:,.0f}")
        return " · ".join(razoes) if razoes else "Em estoque"

    df["Motivo"] = df.apply(motivo, axis=1)
    df = df.sort_values("Score", ascending=False).reset_index(drop=True)
    df.index = df.index + 1

    return df


# ── Montagem da lista com regra KPMG ─────────────────────────────────────────

def montar_lista_ciclica(df_score: pd.DataFrame, qtd_ciclo: int, contados: dict) -> pd.DataFrame:
    """
    Monta a lista do ciclo em duas camadas:
      1. Top N por score (alta prioridade)
      2. Produtos nunca contados que completam a cota (regra KPMG)

    Garante que ao longo do ano todos os SKUs sejam cobertos.
    """
    produtos_nao_contados = set(
        df_score[~df_score["Produto"].astype(str).isin(contados.keys())]["Produto"].astype(str).tolist()
    )

    # Camada 1: top N por score
    top_n = df_score.head(qtd_ciclo).copy()
    top_n["Origem"] = top_n["Produto"].astype(str).apply(
        lambda p: "⬜ Cobertura KPMG" if p in produtos_nao_contados else "🔴 Alta prioridade"
    )
    # Garante que todos do top N sejam incluídos
    lista_final = top_n.copy()

    # Camada 2: nunca contados que ainda não estão na lista
    ja_na_lista = set(lista_final["Produto"].astype(str).tolist())
    nunca_contados_fora = df_score[
        df_score["Produto"].astype(str).isin(produtos_nao_contados) &
        ~df_score["Produto"].astype(str).isin(ja_na_lista)
    ].copy()

    if not nunca_contados_fora.empty:
        nunca_contados_fora["Origem"] = "⬜ Cobertura KPMG"
        lista_final = pd.concat([lista_final, nunca_contados_fora], ignore_index=True)

    lista_final = lista_final.reset_index(drop=True)
    lista_final.index = lista_final.index + 1
    return lista_final


# ── Exportação Excel ──────────────────────────────────────────────────────────

def gerar_xlsx(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=True, index_label="Ranking", sheet_name="Inv. Cíclico")
        wb = writer.book
        ws = writer.sheets["Inv. Cíclico"]

        fmt_header = wb.add_format({
            "bold": True, "bg_color": "#004550", "font_color": "#FFFFFF",
            "border": 1, "border_color": "#EC6E21", "align": "center", "valign": "vcenter"
        })
        fmt_A      = wb.add_format({"bg_color": "#FFF3CD", "border": 1, "border_color": "#dee2e6"})
        fmt_B      = wb.add_format({"bg_color": "#D1ECF1", "border": 1, "border_color": "#dee2e6"})
        fmt_C      = wb.add_format({"bg_color": "#F8F9FA", "border": 1, "border_color": "#dee2e6"})
        fmt_kpmg   = wb.add_format({"bg_color": "#E8F5E9", "border": 1, "border_color": "#dee2e6"})

        for col_num, col_name in enumerate(["Ranking"] + list(df.columns)):
            ws.write(0, col_num, col_name, fmt_header)
            ws.set_column(col_num, col_num, max(len(str(col_name)) + 4, 14))

        for row_num, (idx, row) in enumerate(df.iterrows(), start=1):
            if str(row.get("Origem", "")).startswith("⬜"):
                fmt = fmt_kpmg
            elif row.get("Curva ABC") == "A":
                fmt = fmt_A
            elif row.get("Curva ABC") == "B":
                fmt = fmt_B
            else:
                fmt = fmt_C
            ws.write(row_num, 0, idx, fmt)
            for col_num, val in enumerate(row, start=1):
                ws.write(row_num, col_num, val, fmt)

    output.seek(0)
    return output.getvalue()


# ── Render principal ──────────────────────────────────────────────────────────

def render(df_jlle: pd.DataFrame, df_outras: pd.DataFrame, formatar_br):
    st.markdown("## 🔄 Inventário Cíclico")
    st.markdown(
        "Geração inteligente de listas de contagem com **regra KPMG**: "
        "todos os SKUs devem ser contados ao menos uma vez por ano."
    )

    if df_jlle is None or df_jlle.empty:
        st.warning("Nenhum dado de Joinville encontrado. Carregue os dados na sidebar.")
        return

    # ── Filtro próprio da aba (independente do filtro global) ────────────
    st.markdown("### 1. Selecione a unidade")
    st.caption("Este filtro é independente do filtro global e define a base do inventário cíclico.")

    empresas_disp = sorted(df_jlle["Empresa"].dropna().unique().tolist())
    if not empresas_disp:
        st.error("Nenhuma empresa encontrada nos dados de Joinville.")
        return

    col_emp, col_fil = st.columns(2)

    with col_emp:
        empresa_sel = st.selectbox("🏢 Empresa", empresas_disp, key="ic_empresa_sel")

    with col_fil:
        filiais_disp = sorted(
            df_jlle[df_jlle["Empresa"] == empresa_sel]["Filial"].dropna().unique().tolist()
        )
        filial_sel = st.selectbox("📍 Filial", filiais_disp, key="ic_filial_sel")

    label_unidade = f"{empresa_sel} — {filial_sel}"

    # Filtra pela seleção da aba (ignora filtro global propositalmente)
    df_filial = df_jlle[
        (df_jlle["Empresa"] == empresa_sel) &
        (df_jlle["Filial"]  == filial_sel)
    ].copy()

    if df_filial.empty:
        st.warning(f"Sem dados para **{label_unidade}**.")
        return

    # Inicializa controle de sessão para empresa+filial
    inicializar_controle(empresa_sel, filial_sel)
    contados = obter_contados(empresa_sel, filial_sel)

    # ── Calcula score ─────────────────────────────────────────────────────
    df_score     = calcular_score(df_filial, contados)
    total_skus   = len(df_score)
    total_contados = sum(1 for p in df_score["Produto"].astype(str) if p in contados)
    pct_cobertura  = (total_contados / total_skus * 100) if total_skus > 0 else 0
    total_diverg   = int((df_score["Divergência"] != 0).sum())
    skus_A         = int((df_score["Curva ABC"] == "A").sum())
    valor_total    = df_score["Vl Total ERP"].sum()

    # ── Métricas ──────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown(f"### 2. Visão geral — {label_unidade}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total de SKUs",       f"{total_skus:,}")
    c2.metric("SKUs Divergentes",    f"{total_diverg:,}")
    c3.metric("SKUs Curva A",        f"{skus_A:,}")
    c4.metric("Valor Total Estoque", f"R$ {formatar_br(valor_total)}")

    # ── Barra de cobertura KPMG ───────────────────────────────────────────
    st.markdown("#### Cobertura KPMG (ano vigente)")
    falta     = total_skus - total_contados
    cor_barra = "#27AE60" if pct_cobertura >= 100 else "#EC6E21"

    st.markdown(
        f"""
        <div style="background:#004550;border-radius:8px;padding:12px 16px;margin-bottom:8px;">
          <div style="display:flex;justify-content:space-between;margin-bottom:6px;">
            <span style="color:#fff;font-size:0.9rem;">
              ✅ <b>{total_contados}</b> contados &nbsp;|&nbsp;
              ⬜ <b>{falta}</b> pendentes
            </span>
            <span style="color:{cor_barra};font-weight:bold;font-size:0.95rem;">
              {pct_cobertura:.1f}%
            </span>
          </div>
          <div style="background:#003040;border-radius:4px;height:10px;">
            <div style="background:{cor_barra};width:{min(pct_cobertura,100):.1f}%;
                        height:10px;border-radius:4px;transition:width 0.4s;">
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if pct_cobertura >= 100:
        st.success("🎉 Todos os SKUs foram contados neste período. Exigência KPMG cumprida!")

    with st.expander("⚙️ Controles do período"):
        st.caption("Ao iniciar um novo período anual, clique abaixo para zerar o histórico desta unidade.")
        if st.button(f"🔄 Iniciar novo período — {label_unidade}", key="ic_reset"):
            resetar_contagem(empresa_sel, filial_sel)
            st.success("Histórico zerado. Novo período iniciado!")
            st.rerun()

    # ── Seleção de quantidade ─────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 3. Defina o tamanho do ciclo")

    modo = st.radio(
        "Modo de seleção",
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
                tipo = "primary" if st.session_state.ic_qtd == qtd else "secondary"
                if st.button(f"{qtd} itens", key=f"ic_btn_{qtd}", type=tipo):
                    st.session_state.ic_qtd = qtd
        qtd_ciclo = min(st.session_state.ic_qtd, total_skus)

    else:
        pct_opcoes = {
            "5% — Críticos":         0.05,
            "10% — Alta prioridade": 0.10,
            "20% — Rotina":          0.20,
            "30% — Controle amplo":  0.30,
        }
        pct_label = st.select_slider(
            "Faixa percentual",
            options=list(pct_opcoes.keys()),
            value="10% — Alta prioridade",
            key="ic_pct"
        )
        pct_val   = pct_opcoes[pct_label]
        qtd_ciclo = max(1, int(total_skus * pct_val))
        st.caption(f"→ {qtd_ciclo} itens selecionados de {total_skus} disponíveis")

    # ── Monta lista com regra KPMG ────────────────────────────────────────
    df_lista = montar_lista_ciclica(df_score, qtd_ciclo, contados)

    qtd_prioridade = int((df_lista["Origem"] == "🔴 Alta prioridade").sum())
    qtd_kpmg       = int((df_lista["Origem"] == "⬜ Cobertura KPMG").sum())

    st.markdown("---")
    st.markdown(
        f"### 4. Lista do ciclo — {len(df_lista)} itens  "
        f"<span style='font-size:0.8rem;color:#EC6E21;'>🔴 {qtd_prioridade} por prioridade</span>  "
        f"<span style='font-size:0.8rem;color:#27AE60;'> ⬜ {qtd_kpmg} cobertura KPMG</span>",
        unsafe_allow_html=True,
    )

    colunas_exibir = [
        "Produto", "Descrição", "Empresa", "Filial",
        "Curva ABC", "Score", "Já Contado", "Dias s/ Contagem",
        "Saldo ERP (Total)", "Saldo WMS", "Divergência",
        "Vl Total ERP", "Motivo", "Origem"
    ]
    colunas_ok = [c for c in colunas_exibir if c in df_lista.columns]
    df_exibir  = df_lista[colunas_ok]

    st.dataframe(
        df_exibir.style.apply(
            lambda r: ["background-color: #005562; color: #ffffff; font-size: 0.84rem;"] * len(r),
            axis=1
        ).set_table_styles([
            {"selector": "thead th", "props": [
                ("background-color", "#004550"), ("color", "#ffffff"),
                ("border-bottom", "2px solid #EC6E21"), ("text-transform", "uppercase")
            ]},
            {"selector": "td", "props": [
                ("padding", "8px 12px"),
                ("border-bottom", "1px solid rgba(255,255,255,0.05)")
            ]}
        ]).format({
            "Saldo ERP (Total)": "{:,.2f}",
            "Saldo WMS":         "{:,.2f}",
            "Divergência":       "{:,.2f}",
            "Vl Total ERP":      "R$ {:,.2f}",
            "Score":             "{:.2f}",
            "Dias s/ Contagem":  "{:.0f}d",
        }, na_rep="-"),
        use_container_width=True,
        hide_index=False,
    )

    # ── Marcar como contados ──────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 5. Registrar contagem realizada")
    st.caption(
        "Após realizar a contagem física, marque os produtos abaixo. "
        "Isso atualiza o progresso KPMG e ajusta o score dos próximos ciclos."
    )

    produtos_lista = df_exibir["Produto"].astype(str).tolist()
    produtos_sel   = st.multiselect(
        "Selecione os produtos contados neste ciclo",
        options=produtos_lista,
        key="ic_marcar"
    )

    if st.button("✅ Confirmar contagem dos produtos selecionados", key="ic_confirmar"):
        if produtos_sel:
            marcar_contados(empresa_sel, filial_sel, produtos_sel)
            st.success(f"{len(produtos_sel)} produto(s) marcado(s) como contado(s). Score atualizado!")
            st.rerun()
        else:
            st.warning("Selecione ao menos um produto antes de confirmar.")

    # ── Exportação ────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 6. Exportar lista para contagem")
    col_ex1, col_ex2 = st.columns([2, 1])
    with col_ex1:
        st.caption(
            f"O arquivo contém {len(df_exibir)} itens ordenados por score. "
            f"Unidade: {label_unidade}."
        )
    with col_ex2:
        slug = f"{empresa_sel}_{filial_sel}".replace(" ", "_").replace("-", "")
        nome_arquivo = f"inv_ciclico_{slug}_{date.today().strftime('%d%m%Y')}.xlsx"
        st.download_button(
            label="📥 Baixar Excel para Contagem",
            data=gerar_xlsx(df_exibir),
            file_name=nome_arquivo,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
