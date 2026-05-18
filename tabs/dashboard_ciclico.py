import io
import json

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st
from sqlalchemy import text


# ── CARREGAMENTO DE DADOS ─────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def _carregar_dados_dashboard(_engine):
    """Busca todos os ciclos históricos de todas as empresas e os dados de auditoria."""
    ciclos = []
    df_audit = pd.DataFrame()

    try:
        with _engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT empresa, filial, num_ciclo, data_geracao, data_fechamento,
                       responsavel, qtd_lista, produtos_lista, uploads_json
                FROM inventario_ciclos_historico
                ORDER BY data_fechamento DESC
            """)).fetchall()
        for r in rows:
            ciclos.append({
                "empresa":        r[0],
                "filial":         r[1],
                "num_ciclo":      r[2],
                "data_geracao":   r[3],
                "data_fechamento": r[4],
                "responsavel":    r[5],
                "qtd_lista":      r[6],
                "uploads":        json.loads(r[8] or "[]"),
            })
    except Exception as e:
        st.error(f"Erro ao carregar histórico de ciclos: {e}")

    try:
        df_audit = pd.read_sql(
            'SELECT DISTINCT "Empresa", "Filial", "Produto", "Descrição", "Vl Unit" FROM auditoria',
            _engine,
        )
        df_audit["Produto"] = df_audit["Produto"].astype(str).str.zfill(6)
    except Exception:
        pass

    return ciclos, df_audit


def _construir_df_itens(ciclos, df_audit):
    """
    Transforma os ciclos históricos em um DataFrame plano por produto/ciclo.
    Colunas: Data Inventario, Empresa, Filial, Produto, Descricao,
             Valor Unit, Qtd Invent, Valor Invent, Qtd Protheus,
             Valor Protheus, Qtd Divergente, Valor Divergente.
    """
    # Lookup: produto -> (descricao, vl_unit)
    lookup: dict[str, tuple[str, float]] = {}
    if not df_audit.empty:
        for _, row in df_audit.iterrows():
            p = str(row.get("Produto", "")).zfill(6)
            if p not in lookup:
                lookup[p] = (
                    str(row.get("Descrição", "") or ""),
                    float(row.get("Vl Unit", 0) or 0),
                )

    registros = []
    for ciclo in ciclos:
        emp       = ciclo["empresa"]
        fil       = ciclo["filial"]
        data_fech = ciclo["data_fechamento"]

        for upload in ciclo.get("uploads", []):
            dados = upload.get("dados", [])
            if not isinstance(dados, list):
                continue

            for item in dados:
                if not isinstance(item, dict):
                    continue

                cod = str(item.get("Codigo", item.get("Código", ""))).strip().zfill(6)
                if not cod or cod == "000000":
                    continue

                qtd_inv = float(
                    item.get("Saldo WMS",
                    item.get("Qtd WMS",
                    item.get("Qtd Antes", 0))) or 0
                )
                qtd_prot = float(
                    item.get("Saldo ERP (Total)",
                    item.get("Qtd ERP",
                    item.get("Qtd Depois", 0))) or 0
                )
                vl_unit  = float(item.get("Vl Unit", 0) or 0)
                descricao = str(item.get("Descricao", item.get("Descrição", ""))).strip()

                if vl_unit == 0 and cod in lookup:
                    vl_unit = lookup[cod][1]
                if not descricao and cod in lookup:
                    descricao = lookup[cod][0]

                qtd_div = qtd_inv - qtd_prot

                registros.append({
                    "Data Inventario":  data_fech,
                    "Empresa":          emp,
                    "Filial":           fil,
                    "Empresa / Filial": f"{emp} / {fil}",
                    "Produto":          cod,
                    "Descricao":        descricao,
                    "Valor Unit":       vl_unit,
                    "Qtd Invent":       qtd_inv,
                    "Valor Invent":     qtd_inv * vl_unit,
                    "Qtd Protheus":     qtd_prot,
                    "Valor Protheus":   qtd_prot * vl_unit,
                    "Qtd Divergente":   qtd_div,
                    "Valor Divergente": qtd_div * vl_unit,
                    "num_ciclo":        ciclo["num_ciclo"],
                })

    if not registros:
        return pd.DataFrame()

    df = pd.DataFrame(registros)
    df["Data Inventario"] = pd.to_datetime(df["Data Inventario"], errors="coerce")
    return df


# ── COMPONENTES VISUAIS ───────────────────────────────────────────────────────

def _kpi_card(col, label: str, value, is_currency: bool = False, color: str = "#ffffff"):
    if is_currency:
        formatted = f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    elif isinstance(value, float):
        formatted = f"{value:.2f}%"
    else:
        formatted = f"{value:,}".replace(",", ".")

    with col:
        st.markdown(
            f"""
            <div style="border:2px solid #EC6E21; background:#004550; border-radius:10px;
                        padding:15px; text-align:center; height:90px; display:flex;
                        flex-direction:column; justify-content:center;">
                <div style="color:#a0c4cc; font-size:0.72rem; font-weight:600;
                            text-transform:uppercase; margin-bottom:6px; letter-spacing:0.5px;">
                    {label}
                </div>
                <div style="color:{color}; font-size:1.4rem; font-weight:800; line-height:1.1;">
                    {formatted}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _gauge_svg(pct: float) -> str:
    """Gauge semicircular em SVG puro (sem dependência de plotly)."""
    pct = max(0.0, min(100.0, pct))
    # Arco: raio 80, centro (110, 100)
    # Comprimento total semicírculo = π × 80 ≈ 251.3
    arc_len   = np.pi * 80
    progress  = (pct / 100) * arc_len
    color_arc = "#EC6E21" if pct >= 95 else "#e74c3c"

    # Ângulo da agulha: -90° (esquerda) a +90° (direita)
    needle_angle = (pct / 100) * 180 - 90

    # Posição do marcador Meta 95%
    meta_angle = (95 / 100) * 180 - 90

    return f"""
    <div style="display:flex; flex-direction:column; align-items:center; margin-top:10px;">
      <svg width="220" height="130" viewBox="0 0 220 130" xmlns="http://www.w3.org/2000/svg">
        <!-- Fundo do arco -->
        <path d="M 30 100 A 80 80 0 0 1 190 100"
              fill="none" stroke="#1a5562" stroke-width="16" stroke-linecap="round"/>
        <!-- Arco de progresso -->
        <path d="M 30 100 A 80 80 0 0 1 190 100"
              fill="none" stroke="{color_arc}" stroke-width="16" stroke-linecap="round"
              stroke-dasharray="{progress:.1f} {arc_len:.1f}"/>
        <!-- Marcador Meta 95% -->
        <line x1="110" y1="22" x2="110" y2="36"
              stroke="#EC6E21" stroke-width="1.5" stroke-dasharray="3,2"
              transform="rotate({(95/100)*180-90:.1f} 110 100)"/>
        <!-- Agulha -->
        <line x1="110" y1="100" x2="110" y2="28"
              stroke="#ffffff" stroke-width="2.5" stroke-linecap="round"
              transform="rotate({needle_angle:.1f} 110 100)"/>
        <circle cx="110" cy="100" r="5" fill="#EC6E21"/>
        <!-- Labels dos extremos -->
        <text x="22" y="118" fill="#a0c4cc" font-size="9" text-anchor="middle" font-family="sans-serif">0%</text>
        <text x="110" y="16" fill="#a0c4cc" font-size="9" text-anchor="middle" font-family="sans-serif">50%</text>
        <text x="198" y="118" fill="#a0c4cc" font-size="9" text-anchor="middle" font-family="sans-serif">100%</text>
        <!-- Label Meta -->
        <text x="168" y="42" fill="#EC6E21" font-size="8" text-anchor="middle" font-family="sans-serif">Meta</text>
        <text x="168" y="51" fill="#EC6E21" font-size="8" text-anchor="middle" font-family="sans-serif">95%</text>
      </svg>
      <div style="color:#EC6E21; font-size:1.8rem; font-weight:800; margin-top:-8px;">
        {pct:.2f}%
      </div>
      <div style="color:#a0c4cc; font-size:0.75rem; margin-top:2px;">% Acuracidade Itens</div>
    </div>
    """


# ── SUB-ABAS ──────────────────────────────────────────────────────────────────

def _tab_base_historica(dff: pd.DataFrame):
    if dff.empty:
        st.info("Nenhum dado para os filtros selecionados.")
        return

    r1, r2, r3, r4 = st.columns([1.8, 3.5, 2.5, 1.5])
    with r1:
        tipo = st.radio("", ["Geral", "Divergente"], horizontal=True, key="dash_tipo")
    with r2:
        st.markdown(
            '<div style="color:#a0c4cc;font-size:0.72rem;font-weight:600;text-transform:uppercase;'
            'letter-spacing:0.5px;margin-bottom:4px;">🔍 PESQUISAR</div>',
            unsafe_allow_html=True,
        )
        busca = st.text_input(
            "", placeholder="Produto, empresa, descrição...",
            key="dash_busca", label_visibility="collapsed",
        )
    with r3:
        ordem = st.selectbox(
            "CLASSIFICAR POR",
            ["Data Inventario", "Empresa / Filial", "Produto",
             "Valor Invent", "Qtd Divergente", "Valor Divergente"],
            key="dash_ordem",
        )
    with r4:
        direcao = st.selectbox("DIREÇÃO", ["↓ Desc", "↑ Asc"], key="dash_dir")

    df_view = dff.copy()
    if tipo == "Divergente":
        df_view = df_view[df_view["Qtd Divergente"] != 0]
    if busca:
        mask = (
            df_view["Produto"].str.contains(busca, case=False, na=False)
            | df_view["Descricao"].str.contains(busca, case=False, na=False)
            | df_view["Empresa"].str.contains(busca, case=False, na=False)
        )
        df_view = df_view[mask]

    asc = direcao == "↑ Asc"
    if ordem in df_view.columns:
        df_view = df_view.sort_values(ordem, ascending=asc)

    st.caption(f"{len(df_view):,} registros".replace(",", "."))

    cols_exib = [
        "Data Inventario", "Empresa / Filial", "Produto", "Descricao",
        "Valor Unit", "Qtd Invent", "Valor Invent", "Qtd Protheus",
        "Valor Protheus", "Qtd Divergente", "Valor Divergente",
    ]
    df_exib = df_view[[c for c in cols_exib if c in df_view.columns]].copy()
    df_exib["Data Inventario"] = df_exib["Data Inventario"].dt.strftime("%Y-%m-%d")

    def _style_row(row):
        base = "background-color:#005562; color:#ffffff; font-size:0.82rem;"
        if row.get("Qtd Divergente", 0) != 0:
            return [base + "border-left:3px solid #e74c3c;" if i == 0 else base
                    for i in range(len(row))]
        return [base] * len(row)

    fmt = {
        "Valor Unit":       "R$ {:,.2f}",
        "Valor Invent":     "R$ {:,.2f}",
        "Valor Protheus":   "R$ {:,.2f}",
        "Valor Divergente": "R$ {:,.2f}",
        "Qtd Invent":       "{:,.0f}",
        "Qtd Protheus":     "{:,.0f}",
        "Qtd Divergente":   "{:,.0f}",
    }
    styled = (
        df_exib.style
        .apply(_style_row, axis=1)
        .format({k: v for k, v in fmt.items() if k in df_exib.columns}, na_rep="-")
        .hide(axis="index")
        .set_table_styles([
            {"selector": "thead th", "props": [
                ("background-color", "#004550"), ("color", "#ffffff"),
                ("border-bottom", "2px solid #EC6E21"),
                ("text-transform", "uppercase"), ("font-size", "0.75rem"),
            ]},
            {"selector": "td", "props": [
                ("padding", "6px 10px"),
                ("border-bottom", "1px solid rgba(255,255,255,0.05)"),
            ]},
        ])
    )

    # Botão exportar ACIMA da tabela
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df_exib.to_excel(writer, index=False)
    col_cap, col_btn = st.columns([5, 1])
    with col_btn:
        st.download_button(
            "📥 Exportar",
            data=output.getvalue(),
            file_name="inventario_ciclico_dashboard.xlsx",
            mime="application/vnd.ms-excel",
            use_container_width=True,
        )

    st.dataframe(styled, use_container_width=True, height=420)


def _tab_analise(df_all: pd.DataFrame, emp_sel: str, fil_sel: str):
    if df_all.empty:
        st.info("Nenhum dado disponível para análise.")
        return

    tipo = st.radio(
        "", ["Acuracidade Quantidade", "Acuracidade Valor"],
        horizontal=True, key="dash_analise_tipo",
    )

    # A análise usa TODO o histórico (ignora filtro Fechamento), mas respeita Empresa/Filial
    df_analise = df_all.copy()
    if emp_sel != "Todas as empresas":
        df_analise = df_analise[df_analise["Empresa"] == emp_sel]
    if fil_sel != "Todas as filiais":
        df_analise = df_analise[df_analise["Filial"] == fil_sel]

    df_analise = df_analise.dropna(subset=["Data Inventario"])
    # Agrupa por "YYYY-MM" (string) para garantir um ponto por mês
    df_analise["Mes_key"] = df_analise["Data Inventario"].dt.strftime("%Y-%m")

    grp = (
        df_analise.groupby("Mes_key", sort=True)
        .agg(
            total    =("Produto", "count"),
            div_qtd  =("Qtd Divergente",   lambda s: int((s != 0).sum())),
            div_val  =("Valor Divergente",  lambda s: int((s != 0).sum())),
        )
        .reset_index()
    )
    grp["Acuracidade Quantidade"] = (
        (grp["total"] - grp["div_qtd"]) / grp["total"] * 100
    ).round(2)
    grp["Acuracidade Valor"] = (
        (grp["total"] - grp["div_val"]) / grp["total"] * 100
    ).round(2)
    # Converte para datetime só para o eixo X do Altair
    grp["Mes"] = pd.to_datetime(grp["Mes_key"] + "-01")

    if grp.empty:
        st.info("Sem dados suficientes para o gráfico.")
        return

    col_y = tipo

    y_scale = alt.Scale(domain=[93.0, 101.0])
    y_axis  = alt.Axis(
        values=[93, 94, 95, 96, 97, 98, 99, 100, 101],
        labelExpr="datum.value + '%'",
        labelColor="#a0c4cc",
        gridColor="#1a6672",
        domainColor="#007687",
        tickColor="#007687",
        title="",
    )
    x_axis = alt.Axis(
        format="%Y-%m",
        tickCount="month",
        labelColor="#a0c4cc",
        gridColor="#1a6672",
        domainColor="#007687",
        tickColor="#007687",
        title="",
    )

    area = (
        alt.Chart(grp)
        .mark_area(color="#2a7a5a", opacity=0.45)
        .encode(
            x=alt.X("Mes:T", axis=x_axis),
            y=alt.Y(f"{col_y}:Q", scale=y_scale, axis=y_axis),
        )
    )
    line = (
        alt.Chart(grp)
        .mark_line(color="#EC6E21", strokeWidth=2.5)
        .encode(
            x=alt.X("Mes:T"),
            y=alt.Y(f"{col_y}:Q", scale=y_scale),
            tooltip=[
                alt.Tooltip("Mes_key:N", title="Mês"),
                alt.Tooltip(f"{col_y}:Q", title="Acuracidade", format=".2f"),
            ],
        )
    )
    points = (
        alt.Chart(grp)
        .mark_point(color="#EC6E21", size=70, filled=True)
        .encode(
            x=alt.X("Mes:T"),
            y=alt.Y(f"{col_y}:Q", scale=y_scale),
        )
    )
    meta_df   = pd.DataFrame({"y": [95.0]})
    meta_line = (
        alt.Chart(meta_df)
        .mark_rule(strokeDash=[6, 4], color="#EC6E21", opacity=0.7, strokeWidth=1.5)
        .encode(y=alt.Y("y:Q", scale=y_scale))
    )
    meta_text = (
        alt.Chart(meta_df)
        .mark_text(align="left", color="#EC6E21", opacity=0.9,
                   fontSize=10, dx=4, dy=-4)
        .encode(
            y=alt.Y("y:Q", scale=y_scale),
            x=alt.value(0),
            text=alt.value("Meta 95%"),
        )
    )

    chart = (
        alt.layer(area, line, points, meta_line, meta_text)
        .resolve_scale(y="shared")
        .properties(
            height=320,
            title=alt.TitleParams(
                text=col_y,
                color="#ffffff",
                fontSize=13,
                fontWeight="bold",
                anchor="start",
            ),
        )
        .configure_view(fill="#004550", stroke=None)
        .configure_axis(labelColor="#a0c4cc", gridColor="#1a6672",
                        domainColor="#007687", titleColor="#a0c4cc")
    )
    st.altair_chart(chart, use_container_width=True)


def _tab_resumo(dff: pd.DataFrame):
    if dff.empty:
        st.info("Nenhum dado para os filtros selecionados.")
        return

    tipo = st.radio(
        "", ["Acuracidade Quantidade", "Acuracidade Valor"],
        horizontal=True, key="dash_resumo_tipo",
    )

    div_col = "Qtd Divergente" if tipo == "Acuracidade Quantidade" else "Valor Divergente"

    grp = (
        dff.groupby("Empresa / Filial")
        .apply(lambda g: pd.Series({
            "total": len(g),
            "div":   int((g[div_col] != 0).sum()),
        }))
        .reset_index()
    )
    grp["% Acuracidade"]  = ((grp["total"] - grp["div"]) / grp["total"] * 100).round(2)
    grp["SKUs Divergentes"] = grp["div"].astype(int)

    total_items = len(dff)
    total_div   = int((dff[div_col] != 0).sum())
    total_acur  = ((total_items - total_div) / total_items * 100) if total_items > 0 else 100.0

    df_resumo = grp[["Empresa / Filial", "% Acuracidade", "SKUs Divergentes"]].copy()
    df_resumo = pd.concat([
        df_resumo,
        pd.DataFrame([{
            "Empresa / Filial": "Total",
            "% Acuracidade":    round(total_acur, 2),
            "SKUs Divergentes": total_div,
        }]),
    ], ignore_index=True)

    col_tab, col_gauge = st.columns([3, 2])

    with col_tab:
        def _style_resumo(row):
            base = "background-color:#004550; color:#ffffff; font-size:0.85rem;"
            if row["Empresa / Filial"] == "Total":
                return [base + "font-weight:800; border-top:2px solid #EC6E21;"] * len(row)
            acur  = float(row["% Acuracidade"])
            bg    = "#1a4a32" if acur >= 95 else "#4a1a1a"
            fg    = "#b3ffcc" if acur >= 95 else "#ffb3b3"
            cells = [base]
            cells.append(f"background-color:{bg}; color:{fg}; font-weight:600;")
            cells.append(base)
            return cells

        styled = (
            df_resumo.style
            .apply(_style_resumo, axis=1)
            .format({"% Acuracidade": "{:.2f}%", "SKUs Divergentes": "{:,}"})
            .set_table_styles([
                {"selector": "thead th", "props": [
                    ("background-color", "#004550"), ("color", "#ffffff"),
                    ("border-bottom", "2px solid #EC6E21"),
                    ("text-transform", "uppercase"), ("font-size", "0.8rem"),
                ]},
                {"selector": "td", "props": [
                    ("padding", "10px 14px"),
                    ("border-bottom", "1px solid rgba(255,255,255,0.07)"),
                ]},
            ])
        )
        st.dataframe(styled, use_container_width=True, hide_index=True)

    with col_gauge:
        st.markdown(_gauge_svg(total_acur), unsafe_allow_html=True)


# ── RENDER PRINCIPAL ──────────────────────────────────────────────────────────

def render(engine, formatar_br):
    if engine is None:
        st.warning("⚠️ Banco de dados não disponível.")
        return

    with st.spinner("Carregando histórico de inventários..."):
        ciclos, df_audit = _carregar_dados_dashboard(engine)

    if not ciclos:
        st.info("📊 Nenhum ciclo de inventário cíclico fechado encontrado. "
                "Encerre um ciclo na aba **Inv. Cíclico** para ver dados aqui.")
        return

    df_all = _construir_df_itens(ciclos, df_audit)

    if df_all.empty:
        st.info("📊 Ciclos encontrados, mas sem dados de itens nos uploads ERP.")
        return

    # ── FILTROS ──────────────────────────────────────────────────────────────
    datas_disp = sorted(
        df_all["Data Inventario"].dropna().dt.strftime("%Y-%m-%d").unique(),
        reverse=True,
    )
    datas_labels = ["Todos"] + datas_disp
    empresas_disp = sorted(df_all["Empresa"].dropna().unique().tolist())

    fc1, fc2, fc3 = st.columns([2, 3, 3])
    with fc1:
        st.markdown(
            '<div style="color:#a0c4cc;font-size:0.72rem;font-weight:600;'
            'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">FECHAMENTO</div>',
            unsafe_allow_html=True,
        )
        data_sel = st.selectbox("", datas_labels, key="dash_data",
                                label_visibility="collapsed")
    with fc2:
        st.markdown(
            '<div style="color:#a0c4cc;font-size:0.72rem;font-weight:600;'
            'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">EMPRESA</div>',
            unsafe_allow_html=True,
        )
        emp_sel = st.selectbox(
            "", ["Todas as empresas"] + empresas_disp,
            key="dash_emp", label_visibility="collapsed",
        )
    with fc3:
        if emp_sel != "Todas as empresas":
            filiais_disp = sorted(
                df_all[df_all["Empresa"] == emp_sel]["Filial"].dropna().unique().tolist()
            )
        else:
            filiais_disp = sorted(df_all["Filial"].dropna().unique().tolist())
        st.markdown(
            '<div style="color:#a0c4cc;font-size:0.72rem;font-weight:600;'
            'text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;">FILIAL</div>',
            unsafe_allow_html=True,
        )
        fil_sel = st.selectbox(
            "", ["Todas as filiais"] + filiais_disp,
            key="dash_fil", label_visibility="collapsed",
        )

    # Aplicar filtros
    dff = df_all.copy()
    if data_sel != "Todos":
        dff = dff[dff["Data Inventario"].dt.strftime("%Y-%m-%d") == data_sel]
    if emp_sel != "Todas as empresas":
        dff = dff[dff["Empresa"] == emp_sel]
    if fil_sel != "Todas as filiais":
        dff = dff[dff["Filial"] == fil_sel]

    # ── KPIs ─────────────────────────────────────────────────────────────────
    st.markdown("<div style='margin:12px 0 4px;'></div>", unsafe_allow_html=True)

    total_rows = len(dff)
    qtd_inv    = int(dff["Qtd Invent"].sum()) if not dff.empty else 0
    n_div_qtd  = int((dff["Qtd Divergente"] != 0).sum()) if not dff.empty else 0
    n_div_val  = int((dff["Valor Divergente"] != 0).sum()) if not dff.empty else 0
    acur_qtd   = ((total_rows - n_div_qtd) / total_rows * 100) if total_rows > 0 else 100.0
    vl_inv     = dff["Valor Invent"].sum() if not dff.empty else 0.0
    vl_div     = dff["Valor Divergente"].sum() if not dff.empty else 0.0
    acur_val   = ((total_rows - n_div_val) / total_rows * 100) if total_rows > 0 else 100.0

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    _kpi_card(k1, "Qtd Inventariada",  total_rows)
    _kpi_card(k2, "Qtd Divergentes",   n_div_qtd)
    _kpi_card(k3, "Acuracidade Qtd",   acur_qtd,
              color="#4CAF50" if acur_qtd >= 95 else "#e74c3c")
    _kpi_card(k4, "Valor Inventariado", vl_inv, is_currency=True)
    _kpi_card(k5, "Valor Divergente",   vl_div, is_currency=True)
    _kpi_card(k6, "Acuracidade Valor",  acur_val,
              color="#4CAF50" if acur_val >= 95 else "#e74c3c")

    st.markdown("---")

    # ── SUB-ABAS ─────────────────────────────────────────────────────────────
    stab1, stab2, stab3 = st.tabs([
        "📋 Base Histórica",
        "📈 Análise de Inventário",
        "📊 Resumo",
    ])

    with stab1:
        _tab_base_historica(dff)
    with stab2:
        _tab_analise(df_all, emp_sel, fil_sel)
    with stab3:
        _tab_resumo(dff)
