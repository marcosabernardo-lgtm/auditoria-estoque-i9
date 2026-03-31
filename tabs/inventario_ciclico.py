import streamlit as st
import pandas as pd
import numpy as np
import io
import json
from datetime import date, datetime
from sqlalchemy import text

PERIODO_KPMG_DIAS = 365

try:
    from inventario_db import (
        db_obter_contados, db_marcar_contados, db_resetar_contados,
        db_obter_ciclos, db_gravar_ciclo, db_resetar_ciclos,
        db_obter_ciclo_ativo, db_salvar_ciclo_ativo,
        db_acumular_upload, db_fechar_ciclo_ativo, db_resetar_tudo,
    )
    DB_DISPONIVEL = True
except ImportError:
    DB_DISPONIVEL = False


def processar_resultado_wms(arquivo):
    xls     = pd.ExcelFile(arquivo)
    df_meta = pd.read_excel(xls, sheet_name=xls.sheet_names[0], header=None, nrows=7)
    meta    = {}
    for _, row in df_meta.iterrows():
        chave = str(row.iloc[0]).strip().replace(":", "")
        valor = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
        if "mero" in chave or "Numero" in chave: meta["num_inv"]      = valor
        elif "Data" in chave:                     meta["data"]         = valor
        elif "onsav" in chave or "espons" in chave: meta["responsavel"] = valor
        elif "curac" in chave:                    meta["acuracidade"]  = valor
    df = pd.read_excel(xls, sheet_name=xls.sheet_names[0], skiprows=8, header=0)
    df.columns = ["Produto","Qtd Antes","Vl Antes","Qtd Depois","Vl Depois","Qtd Diferença","Vl Diferença","Acuracidade"]
    df = df.dropna(subset=["Produto"])
    df = df[df["Produto"].astype(str).str.strip() != ""]
    df["Codigo"]    = df["Produto"].astype(str).str.split(" - ", n=1).str[0].str.strip().str.zfill(6)
    df["Descricao"] = df["Produto"].astype(str).str.split(" - ", n=1).str[1].str.strip().fillna("")
    for col in ["Qtd Antes","Qtd Depois","Qtd Diferença"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    try:    meta["data_iso"] = datetime.strptime(meta.get("data",""), "%d/%m/%Y").date().isoformat()
    except: meta["data_iso"] = date.today().isoformat()
    meta["df"] = df; meta["produtos"] = df["Codigo"].tolist()
    return meta


@st.cache_data(ttl=300, show_spinner=False)
def calcular_score(df, contados_tuple):
    contados = dict(contados_tuple)
    df = df.copy()
    for col in ["Saldo ERP (Total)","Saldo WMS","Vl Unit","Vl Total ERP","Divergência"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    chave_arm = [c for c in ["Produto","Armazem"] if c in df.columns]
    if "Produto" in df.columns and len(df) > df["Produto"].nunique():
        cols_soma_wms  = [c for c in ["Saldo WMS"] if c in df.columns]
        cols_fixos_arm = [c for c in ["Produto","Armazem","Descricao","Descricão","Descrição","Empresa","Filial","Saldo ERP (Total)","Vl Unit","Vl Total ERP"] if c in df.columns]
        df_w = df.groupby(chave_arm, as_index=False)[cols_soma_wms].sum() if cols_soma_wms else df[chave_arm].drop_duplicates()
        df_f = df[cols_fixos_arm].drop_duplicates(subset=chave_arm, keep="first")
        df_a = df_f.merge(df_w, on=chave_arm, how="left")
        df_a["Divergência"] = df_a["Saldo ERP (Total)"] - df_a["Saldo WMS"]
        cols_sp = [c for c in ["Saldo WMS","Saldo ERP (Total)","Divergência","Vl Total ERP"] if c in df_a.columns]
        cols_fp = [c for c in ["Produto","Descrição","Empresa","Filial","Vl Unit"] if c in df_a.columns]
        df = df_a[cols_fp].drop_duplicates(subset=["Produto"], keep="first").merge(
             df_a.groupby("Produto", as_index=False)[cols_sp].sum(), on="Produto", how="left")
    df = df.sort_values("Vl Total ERP", ascending=False).reset_index(drop=True)
    tv = df["Vl Total ERP"].sum()
    df["pct_acum"]  = df["Vl Total ERP"].cumsum() / tv if tv > 0 else 0
    df["Curva ABC"] = np.where(df["pct_acum"]<=0.80,"A", np.where(df["pct_acum"]<=0.95,"B","C"))
    df["score_abc"] = df["Curva ABC"].map({"A":10,"B":6,"C":3})
    df["score_diverg"] = np.where(df["Divergência"]!=0, 10, 0)
    mv = df["Vl Total ERP"].max() or 1
    df["score_valor"] = (df["Vl Total ERP"]/mv*10).round(2)
    hoje = date.today()
    def dias(p):
        if str(p) in contados:
            try: return (hoje - date.fromisoformat(contados[str(p)])).days
            except: return PERIODO_KPMG_DIAS
        return PERIODO_KPMG_DIAS
    df["Dias s/ Contagem"] = df["Produto"].astype(str).apply(dias)
    md = df["Dias s/ Contagem"].max() or 1
    df["score_dias"] = (df["Dias s/ Contagem"]/md*10).round(2)
    raw = 0.30*df["score_abc"] + 0.25*df["score_diverg"] + 0.25*df["score_valor"] + 0.20*df["score_dias"]
    df["Score"] = (raw/(raw.max() or 1)*10).round(2)
    df["Já Contado"] = df["Produto"].astype(str).apply(lambda p: f"✅ {contados[p]}" if p in contados else "⬜ Não")
    def motivo(r):
        rs = []
        if r["Curva ABC"]=="A": rs.append("Curva A")
        if r["Divergência"]!=0: rs.append("Divergência")
        if r["Dias s/ Contagem"]>=PERIODO_KPMG_DIAS: rs.append("Nunca contado")
        elif r["Dias s/ Contagem"]>180: rs.append(f"{r['Dias s/ Contagem']}d sem contar")
        if r["Vl Total ERP"]>0: rs.append(f"R$ {r['Vl Total ERP']:,.0f}")
        return " · ".join(rs) if rs else "Em estoque"
    df["Motivo"] = df.apply(motivo, axis=1)
    df = df.sort_values("Score", ascending=False).reset_index(drop=True)
    df.index = df.index + 1
    return df


def montar_lista(df_score, qtd, contados):
    nao_cont = set(df_score[~df_score["Produto"].astype(str).isin(contados)]["Produto"].astype(str))
    top = df_score.head(qtd).copy()
    top["Origem"] = top["Produto"].astype(str).apply(
        lambda p: "⬜ Cobertura KPMG" if p in nao_cont else "🔴 Alta prioridade")
    ja    = set(top["Produto"].astype(str))
    vagas = qtd - len(top)
    if vagas > 0:
        extras = df_score[df_score["Produto"].astype(str).isin(nao_cont) &
                          ~df_score["Produto"].astype(str).isin(ja)].head(vagas).copy()
        if not extras.empty:
            extras["Origem"] = "⬜ Cobertura KPMG"
            top = pd.concat([top, extras], ignore_index=True)
    top = top.reset_index(drop=True); top.index = top.index + 1
    return top


def gerar_xlsx_lista(df, label=""):
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as w:
        df.to_excel(w, index=True, index_label="Ranking", sheet_name="Lista")
        wb, ws = w.book, w.sheets["Lista"]
        fh = wb.add_format({"bold":True,"bg_color":"#004550","font_color":"#FFFFFF","border":1})
        for i,c in enumerate(["Ranking"]+list(df.columns)):
            ws.write(0,i,c,fh); ws.set_column(i,i,max(len(str(c))+4,14))
    out.seek(0); return out.getvalue()


def gerar_xlsx_historico(ciclos, label):
    if not ciclos: return b""
    df  = pd.DataFrame([{"Nº Ciclo":c.get("num_ciclo","—"),"Data Geração":c.get("data_geracao","—"),
             "Data Contagem":c.get("data","—"),"Responsável":c.get("responsavel","—"),
             "Acuracidade":c.get("acuracidade","—"),"SKUs Lista":c.get("qtd_lista",0),
             "SKUs Contados":c.get("qtd_contados",0),"Cobertura %":f"{c.get('cobertura_pct',0):.1f}%",
             "Status":c.get("status","—")} for c in ciclos])
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="Histórico KPMG")
    out.seek(0); return out.getvalue()


def montar_df_relatorio(uploads, df_filial):
    """Cruza todos os uploads do ciclo com o ERP e retorna o df do relatório."""
    if not uploads or df_filial is None or df_filial.empty:
        return pd.DataFrame()

    # Base ERP: saldo por produto (já deduplicado)
    erp_cols = [c for c in ["Produto","Descrição","Saldo ERP (Total)","Vl Unit","Vl Total ERP"] if c in df_filial.columns]
    df_erp = df_filial[erp_cols].copy()
    for col in ["Saldo ERP (Total)","Vl Unit","Vl Total ERP"]:
        if col in df_erp.columns:
            df_erp[col] = pd.to_numeric(df_erp[col], errors="coerce").fillna(0)
    df_erp["Produto"] = df_erp["Produto"].astype(str).str.zfill(6)
    df_erp = df_erp.groupby("Produto", as_index=False).agg(
        Descrição=("Descrição","first") if "Descrição" in df_erp.columns else ("Produto","first"),
        **{"Saldo ERP (Total)": ("Saldo ERP (Total)","sum")} if "Saldo ERP (Total)" in df_erp.columns else {},
        **{"Vl Unit": ("Vl Unit","first")} if "Vl Unit" in df_erp.columns else {},
        **{"Vl Total ERP": ("Vl Total ERP","sum")} if "Vl Total ERP" in df_erp.columns else {},
    )

    # Consolida todos os uploads: pega a última contagem por produto
    rows = []
    for u in uploads:
        df_u = u.get("df_rows")
        if df_u:
            for r in df_u:
                rows.append(r)
    if not rows:
        return pd.DataFrame()

    df_wms_all = pd.DataFrame(rows)
    df_wms_all["Codigo"] = df_wms_all["Codigo"].astype(str).str.zfill(6)
    # Suporta tanto nomes antigos (Qtd Antes/Depois) quanto novos (Saldo WMS/Invent WMS)
    if "Saldo WMS" not in df_wms_all.columns and "Qtd Antes" in df_wms_all.columns:
        df_wms_all = df_wms_all.rename(columns={"Qtd Antes":"Saldo WMS","Qtd Depois":"Invent WMS"})
    for col in ["Saldo WMS","Invent WMS"]:
        if col in df_wms_all.columns:
            df_wms_all[col] = pd.to_numeric(df_wms_all[col], errors="coerce").fillna(0)
    # Última ocorrência por produto
    df_wms_ult = df_wms_all.drop_duplicates(subset=["Codigo"], keep="last")

    # Join ERP × WMS
    merge_cols = ["Codigo"] + [c for c in ["Saldo WMS","Invent WMS","Acuracidade"] if c in df_wms_ult.columns]
    df_rel = df_erp.merge(
        df_wms_ult[merge_cols].rename(columns={"Codigo":"Produto"}),
        on="Produto", how="left"
    )
    df_rel["Saldo WMS"]   = df_rel.get("Saldo WMS",  pd.Series(0, index=df_rel.index)).fillna(0)
    df_rel["Invent WMS"]  = df_rel.get("Invent WMS", pd.Series(0, index=df_rel.index)).fillna(0)
    df_rel["Acuracidade"] = df_rel.get("Acuracidade", pd.Series("—", index=df_rel.index)).fillna("—")

    saldo_erp = pd.to_numeric(df_rel["Saldo ERP (Total)"], errors="coerce").fillna(0) \
                if "Saldo ERP (Total)" in df_rel.columns else pd.Series(0, index=df_rel.index)
    vl_unit   = pd.to_numeric(df_rel["Vl Unit"], errors="coerce").fillna(0) \
                if "Vl Unit" in df_rel.columns else pd.Series(0, index=df_rel.index)

    df_rel["Diferença Invent"]   = saldo_erp - df_rel["Invent WMS"]
    if "Vl Total ERP" not in df_rel.columns:
        df_rel["Vl Total ERP"]   = saldo_erp * vl_unit
    df_rel["Vl Total Diferença"] = df_rel["Diferença Invent"] * vl_unit

    cols_saida = [c for c in [
        "Produto","Descrição",
        "Saldo ERP (Total)","Saldo WMS","Invent WMS",
        "Diferença Invent","Acuracidade",
        "Vl Total ERP","Vl Total Diferença"
    ] if c in df_rel.columns]
    return df_rel[cols_saida].sort_values("Vl Total ERP", ascending=False).reset_index(drop=True)


def gerar_pdf_kpmg(ciclo, df_rel, empresa, filial):
    """Compatibilidade: gera PDF de um único ciclo usando a nova função consolidada."""
    return gerar_pdf_kpmg_consolidado([ciclo], {ciclo.get("num_ciclo",""): df_rel}, empresa, filial)


def gerar_pdf_kpmg_consolidado(ciclos_sel, dfs_rel, empresa, filial):
    """
    Gera PDF consolidado para um ou mais ciclos selecionados.
    ciclos_sel : lista de dicts de ciclo
    dfs_rel    : dict {num_ciclo: df_relatorio}
    """
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.lib.units import cm
        from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                        Paragraph, Spacer, HRFlowable, PageBreak)
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
    except ImportError:
        return None

    buf    = io.BytesIO()
    doc    = SimpleDocTemplate(buf, pagesize=A4,
                               leftMargin=1.5*cm, rightMargin=1.5*cm,
                               topMargin=2*cm, bottomMargin=2*cm)

    C_TEAL   = colors.HexColor("#005562")
    C_ORANGE = colors.HexColor("#EC6E21")
    C_LIGHT  = colors.HexColor("#E8F5F9")
    C_DARK   = colors.HexColor("#003040")
    C_WHITE  = colors.white
    C_GRAY   = colors.HexColor("#CCCCCC")
    C_LGRAY  = colors.HexColor("#F5F5F5")
    C_RED    = colors.HexColor("#C0392B")
    C_GREEN  = colors.HexColor("#27AE60")

    def sty(name, **kw):
        base = dict(fontName="Helvetica", fontSize=9, textColor=colors.black, leading=12)
        base.update(kw)
        return ParagraphStyle(name, **base)

    s_capa_title = sty("ct", fontSize=22, textColor=C_TEAL,   fontName="Helvetica-Bold", spaceAfter=6)
    s_capa_sub   = sty("cs", fontSize=14, textColor=C_ORANGE, fontName="Helvetica-Bold", spaceAfter=4)
    s_capa_meta  = sty("cm", fontSize=9,  textColor=colors.HexColor("#555555"), spaceAfter=3)
    s_sec        = sty("sec", fontSize=12, textColor=C_TEAL, fontName="Helvetica-Bold", spaceBefore=12, spaceAfter=4)
    s_cellh      = sty("ch", fontSize=8,  textColor=C_WHITE, fontName="Helvetica-Bold", alignment=TA_CENTER, leading=10)
    s_cell       = sty("cc", fontSize=8,  textColor=colors.black, leading=10)
    s_cell_c     = sty("ccc",fontSize=8,  textColor=colors.black, alignment=TA_CENTER, leading=10)
    s_num        = sty("cn", fontSize=8,  textColor=colors.black, alignment=TA_RIGHT, leading=10)
    s_footer     = sty("ft", fontSize=7,  textColor=colors.gray, alignment=TA_CENTER)
    s_kpi_lbl    = sty("kl", fontSize=8,  textColor=C_WHITE, fontName="Helvetica-Bold", alignment=TA_CENTER, leading=10)
    s_kpi_val    = sty("kv", fontSize=16, textColor=C_WHITE, fontName="Helvetica-Bold", alignment=TA_CENTER, leading=18)
    s_det_label  = sty("dl", fontSize=8,  textColor=colors.HexColor("#555555"), leading=10)
    s_det_val    = sty("dv", fontSize=9,  textColor=colors.black, fontName="Helvetica-Bold", leading=11)

    elems = []
    hoje  = date.today().strftime("%d/%m/%Y")
    label_unidade = f"{empresa} — {filial}"

    # Datas do período
    datas = [c.get("data","") for c in ciclos_sel if c.get("data","") not in ("","—")]
    data_ini = min(datas) if datas else "—"
    data_fim = max(datas) if datas else "—"

    # KPIs consolidados
    total_skus_cont = sum(
        len(dfs_rel.get(c.get("num_ciclo",""), pd.DataFrame()))
        for c in ciclos_sel
    )
    acur_vals = []
    for c in ciclos_sel:
        a = c.get("acuracidade","0%")
        try: acur_vals.append(float(str(a).replace("%","").replace(",",".")))
        except: pass
    acur_media = f"{sum(acur_vals)/len(acur_vals):.1f}%" if acur_vals else "—"
    cobertura_max = max((c.get("cobertura_pct",0) for c in ciclos_sel), default=0)
    n_ciclos = len(ciclos_sel)

    # ── PÁGINA 1: CAPA ────────────────────────────────────────────────────
    elems.append(Spacer(1, 1*cm))
    elems.append(Paragraph("Gestão Integrada I9", sty("gi", fontSize=11, textColor=C_ORANGE, fontName="Helvetica-Bold")))
    elems.append(Spacer(1, 0.3*cm))
    elems.append(Paragraph("Relatório de", sty("r1", fontSize=18, textColor=C_TEAL)))
    elems.append(Paragraph("Inventário Cíclico", s_capa_title))
    elems.append(HRFlowable(width="100%", thickness=2, color=C_ORANGE))
    elems.append(Spacer(1, 0.5*cm))
    elems.append(Paragraph(f"Unidade: {label_unidade}", s_capa_sub))
    elems.append(Paragraph(f"Período: {data_ini} a {data_fim}", s_capa_meta))
    elems.append(Paragraph(f"Gerado em: {hoje}", s_capa_meta))
    elems.append(Paragraph(f"Ciclos: {n_ciclos}", s_capa_meta))
    elems.append(Spacer(1, 0.8*cm))

    # Resumo Executivo
    elems.append(Paragraph("Resumo Executivo", s_sec))
    elems.append(HRFlowable(width="100%", thickness=1, color=C_ORANGE))
    elems.append(Spacer(1, 0.3*cm))

    kpi_labels = ["SKUs Contados", "Cobertura KPMG", "Acuracidade Média", "Ciclos Realizados"]
    kpi_values = [str(total_skus_cont), f"{cobertura_max:.1f}%", acur_media, str(n_ciclos)]
    kpi_row_l  = [Paragraph(l, s_kpi_lbl) for l in kpi_labels]
    kpi_row_v  = [Paragraph(v, s_kpi_val) for v in kpi_values]
    kpi_t = Table([kpi_row_l, kpi_row_v], colWidths=[4*cm]*4)
    kpi_t.setStyle(TableStyle([
        ("BACKGROUND",     (0,0), (-1,-1), C_TEAL),
        ("BOX",            (0,0), (-1,-1), 0.5, C_DARK),
        ("INNERGRID",      (0,0), (-1,-1), 0.3, C_DARK),
        ("TOPPADDING",     (0,0), (-1,-1), 6),
        ("BOTTOMPADDING",  (0,0), (-1,-1), 6),
        ("ROUNDEDCORNERS", [4]),
    ]))
    elems.append(kpi_t)
    elems.append(Spacer(1, 0.6*cm))

    # Parágrafo de contexto
    status_cobertura = "CUMPRIDA ✓" if cobertura_max >= 100 else "EM ANDAMENTO"
    elems.append(Paragraph(
        f"A unidade {label_unidade} realizou {n_ciclos} ciclo(s) de inventário no período de "
        f"{data_ini} a {data_fim}. A cobertura acumulada atingiu {cobertura_max:.1f}% dos SKUs "
        f"cadastrados, com acuracidade média de {acur_media}. "
        f"Exigência KPMG de cobertura anual: <b>{status_cobertura}</b> ({cobertura_max:.1f}%).",
        sty("ctx", fontSize=9, textColor=colors.black, leading=13)
    ))

    # ── PÁGINA 2: LISTA DE CICLOS ─────────────────────────────────────────
    elems.append(PageBreak())
    elems.append(Paragraph("Lista de Ciclos Realizados", s_sec))
    elems.append(HRFlowable(width="100%", thickness=1, color=C_ORANGE))
    elems.append(Spacer(1, 0.3*cm))

    h_ciclos = [Paragraph(h, s_cellh) for h in
                ["#","Nº Ciclo","Data Contagem","Responsável","Nº Inv.","SKUs","Cobertura","Acuracidade"]]
    rows_ciclos = [h_ciclos]
    for i, c in enumerate(ciclos_sel, 1):
        df_c  = dfs_rel.get(c.get("num_ciclo",""), pd.DataFrame())
        n_sku = len(df_c) if not df_c.empty else c.get("qtd_contados", len(c.get("produtos_contados",[])))
        rows_ciclos.append([
            Paragraph(str(i),                         s_cell_c),
            Paragraph(c.get("num_ciclo","—"),         s_cell),
            Paragraph(c.get("data","—"),              s_cell_c),
            Paragraph(c.get("responsavel","—"),       s_cell),
            Paragraph(c.get("num_inv","—"),           s_cell_c),
            Paragraph(str(n_sku),                     s_cell_c),
            Paragraph(f"{c.get('cobertura_pct',0):.1f}%", s_cell_c),
            Paragraph(str(c.get("acuracidade","—")), s_cell_c),
        ])
    tbl_ciclos = Table(rows_ciclos,
                       colWidths=[0.8*cm, 5.5*cm, 2.5*cm, 4*cm, 1.5*cm, 1.5*cm, 2*cm, 2.2*cm],
                       repeatRows=1)
    tbl_ciclos.setStyle(TableStyle([
        ("BACKGROUND",     (0,0), (-1,0),  C_TEAL),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [C_WHITE, C_LGRAY]),
        ("BOX",            (0,0), (-1,-1), 0.5, C_GRAY),
        ("INNERGRID",      (0,0), (-1,-1), 0.3, C_GRAY),
        ("TOPPADDING",     (0,0), (-1,-1), 4),
        ("BOTTOMPADDING",  (0,0), (-1,-1), 4),
        ("VALIGN",         (0,0), (-1,-1), "MIDDLE"),
    ]))
    elems.append(tbl_ciclos)

    # ── PÁGINAS 3+: DETALHE POR CICLO ─────────────────────────────────────
    for idx, c in enumerate(ciclos_sel, 1):
        elems.append(PageBreak())
        num_c  = c.get("num_ciclo","—")
        df_rel = dfs_rel.get(num_c, pd.DataFrame())
        n_sku  = len(df_rel) if not df_rel.empty else c.get("qtd_contados", len(c.get("produtos_contados",[])))

        elems.append(Paragraph(f"Ciclo {idx} — {num_c}", s_sec))
        elems.append(HRFlowable(width="100%", thickness=1, color=C_ORANGE))
        elems.append(Spacer(1, 0.3*cm))

        # Metadados do ciclo em grid 3x2
        meta_data = [
            [Paragraph("Data da contagem", s_det_label), Paragraph(c.get("data","—"), s_det_val),
             Paragraph("Nº Inventário",    s_det_label), Paragraph(c.get("num_inv","—"), s_det_val),
             Paragraph("Status",           s_det_label), Paragraph(c.get("status","—"), s_det_val)],
            [Paragraph("Responsável",      s_det_label), Paragraph(c.get("responsavel","—"), s_det_val),
             Paragraph("Acuracidade",      s_det_label), Paragraph(str(c.get("acuracidade","—")), s_det_val),
             Paragraph("SKUs contados",    s_det_label), Paragraph(str(n_sku), s_det_val)],
            [Paragraph("SKUs na lista",    s_det_label), Paragraph(str(c.get("qtd_lista","—")), s_det_val),
             Paragraph("Cobertura",        s_det_label), Paragraph(f"{c.get('cobertura_pct',0):.1f}%", s_det_val),
             Paragraph("", s_det_label), Paragraph("", s_det_val)],
        ]
        tbl_meta = Table(meta_data, colWidths=[3*cm, 4.5*cm, 2.5*cm, 3*cm, 2.5*cm, 3*cm])
        tbl_meta.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,-1), C_LGRAY),
            ("BOX",           (0,0), (-1,-1), 0.5, C_GRAY),
            ("INNERGRID",     (0,0), (-1,-1), 0.3, C_GRAY),
            ("TOPPADDING",    (0,0), (-1,-1), 4),
            ("BOTTOMPADDING", (0,0), (-1,-1), 4),
        ]))
        elems.append(tbl_meta)
        elems.append(Spacer(1, 0.4*cm))

        # Tabela de produtos contados com dados do inventário
        if not df_rel.empty:
            elems.append(Paragraph(f"Produtos inventariados ({n_sku})", sty("pi", fontSize=9, textColor=C_TEAL, fontName="Helvetica-Bold", spaceBefore=4, spaceAfter=4)))
            headers  = ["Código","Descrição","Saldo ERP","Saldo WMS","Inventariado","Diferença","Acuracidade","Vl Total ERP","Vl Total Dif."]
            col_keys = ["Produto","Descrição","Saldo ERP (Total)","Saldo WMS","Invent WMS","Diferença Invent","Acuracidade","Vl Total ERP","Vl Total Diferença"]
            col_w    = [1.6*cm, 5.5*cm, 1.8*cm, 1.8*cm, 2*cm, 1.8*cm, 2.2*cm, 2.8*cm, 2.8*cm]

            tbl_data = [[Paragraph(h, s_cellh) for h in headers]]
            for _, row in df_rel.iterrows():
                r = []
                for k in col_keys:
                    v = row.get(k, "—")
                    if k in ["Saldo ERP (Total)","Saldo WMS","Invent WMS"]:
                        r.append(Paragraph(f"{float(v):,.2f}" if v not in ("—", None) else "—", s_num))
                    elif k == "Diferença Invent":
                        try:
                            fv = float(v)
                            txt = f"-{abs(fv):,.2f}" if fv > 0 else (f"+{abs(fv):,.2f}" if fv < 0 else "0,00")
                            r.append(Paragraph(txt, sty("dif", fontSize=8, alignment=TA_RIGHT,
                                               textColor=C_RED if fv > 0 else (C_GREEN if fv < 0 else colors.black), leading=10)))
                        except:
                            r.append(Paragraph("—", s_num))
                    elif k in ["Vl Total ERP","Vl Total Diferença"]:
                        try:
                            fv = float(v)
                            if k == "Vl Total Diferença" and fv != 0:
                                txt = f"R$ -{abs(fv):,.2f}" if fv > 0 else f"R$ +{abs(fv):,.2f}"
                                r.append(Paragraph(txt, sty("vdif", fontSize=8, alignment=TA_RIGHT,
                                                   textColor=C_RED if fv > 0 else C_GREEN, leading=10)))
                            else:
                                r.append(Paragraph(f"R$ {fv:,.2f}", s_num))
                        except:
                            r.append(Paragraph("—", s_num))
                    else:
                        r.append(Paragraph(str(v)[:55], s_cell))
                tbl_data.append(r)

            tbl_prod = Table(tbl_data, colWidths=col_w, repeatRows=1)
            tbl_prod.setStyle(TableStyle([
                ("BACKGROUND",     (0,0), (-1,0),  C_TEAL),
                ("ROWBACKGROUNDS", (0,1), (-1,-1), [C_WHITE, C_LGRAY]),
                ("BOX",            (0,0), (-1,-1), 0.5, C_GRAY),
                ("INNERGRID",      (0,0), (-1,-1), 0.3, C_GRAY),
                ("TOPPADDING",     (0,0), (-1,-1), 3),
                ("BOTTOMPADDING",  (0,0), (-1,-1), 3),
                ("VALIGN",         (0,0), (-1,-1), "MIDDLE"),
            ]))
            elems.append(tbl_prod)
        else:
            # Fallback: lista só com os códigos contados
            prods = [str(p).zfill(6) for p in c.get("produtos_contados",[])]
            if prods:
                elems.append(Paragraph(f"Produtos contados ({len(prods)})",
                    sty("pi2", fontSize=9, textColor=C_TEAL, fontName="Helvetica-Bold", spaceBefore=4, spaceAfter=4)))
                cols_por_linha = 5
                rows_p = []
                for i in range(0, len(prods), cols_por_linha):
                    chunk = prods[i:i+cols_por_linha]
                    chunk += [""] * (cols_por_linha - len(chunk))
                    rows_p.append([Paragraph(p, s_cell_c) for p in chunk])
                tbl_p = Table(rows_p, colWidths=[3.5*cm]*cols_por_linha)
                tbl_p.setStyle(TableStyle([
                    ("ROWBACKGROUNDS", (0,0), (-1,-1), [C_WHITE, C_LGRAY]),
                    ("BOX",           (0,0), (-1,-1), 0.5, C_GRAY),
                    ("INNERGRID",     (0,0), (-1,-1), 0.3, C_GRAY),
                    ("TOPPADDING",    (0,0), (-1,-1), 3),
                    ("BOTTOMPADDING", (0,0), (-1,-1), 3),
                ]))
                elems.append(tbl_p)

    # Rodapé final
    elems.append(Spacer(1, 0.6*cm))
    elems.append(HRFlowable(width="100%", thickness=0.5, color=C_GRAY))
    elems.append(Paragraph(
        f"Documento gerado pelo Sistema de Gestão Integrada I9 em {hoje}. "
        f"Este relatório é destinado à auditoria KPMG e representa o inventário cíclico realizado na unidade {label_unidade}.",
        s_footer))

    doc.build(elems)
    buf.seek(0)
    return buf.getvalue()


def _card(col, num, titulo, desc, ativo, concluido, chave):
    if concluido:
        brd="#27AE60"; bg="#E8F5E9"; ctxt="#27500A"; icon="✓"; badge="Concluído"; bbg="#27AE60"
    elif ativo:
        brd="#005562"; bg="#E1F5EE"; ctxt="#085041"; icon=str(num); badge="Ativo"; bbg="#005562"
    else:
        brd="#cccccc"; bg="var(--color-background-secondary)"; ctxt="var(--color-text-secondary)"; icon=str(num); badge="Pendente"; bbg="#888"
    with col:
        st.markdown(
            f"""<div style="border:2px solid {brd};border-radius:12px;padding:16px;
                background:{bg};min-height:110px;">
              <div style="display:flex;justify-content:space-between;align-items:flex-start;">
                <div style="width:36px;height:36px;border-radius:50%;background:{brd};
                            display:flex;align-items:center;justify-content:center;
                            color:#fff;font-weight:600;font-size:16px;">{icon}</div>
                <span style="background:{bbg};color:#fff;font-size:11px;
                             padding:2px 10px;border-radius:20px;">{badge}</span>
              </div>
              <div style="margin-top:10px;font-weight:600;font-size:15px;color:{ctxt};">{titulo}</div>
              <div style="font-size:12px;color:var(--color-text-secondary);margin-top:4px;">{desc}</div>
            </div>""", unsafe_allow_html=True)
        return st.button("Abrir", key=chave, use_container_width=True,
                         type="primary" if ativo else "secondary")


def render(df_jlle, df_outras, formatar_br):
    st.markdown("## 🔄 Inventário Cíclico")
    st.caption("Geração de listas com **regra KPMG**: todos os SKUs contados ao menos uma vez por ano.")

    if df_jlle is None or df_jlle.empty:
        st.warning("Nenhum dado encontrado. Carregue os dados na sidebar."); return

    # Empresa/Filial vivem no session_state — só definidos após "Gerar lista"
    empresa_sel = st.session_state.get("ic_empresa_sel")
    filial_sel  = st.session_state.get("ic_filial_sel")

    engine_db = st.session_state.get("_engine")

    # Cache no session_state — só recarrega do banco quando empresa/filial muda ou ic_force_reload
    if empresa_sel and filial_sel:
        _cache_key = f"ic_cache_{empresa_sel}_{filial_sel}"
        _deve_recarregar = (
            _cache_key not in st.session_state or
            st.session_state.pop("ic_force_reload", False)
        )
        if _deve_recarregar:
            st.session_state[f"{_cache_key}_contados"]    = db_obter_contados(engine_db, empresa_sel, filial_sel)
            st.session_state[f"{_cache_key}_ciclos"]      = db_obter_ciclos(engine_db, empresa_sel, filial_sel)
            st.session_state[f"{_cache_key}_ciclo_ativo"] = db_obter_ciclo_ativo(engine_db, empresa_sel, filial_sel)
            st.session_state[_cache_key] = True  # marca como carregado
        contados    = st.session_state.get(f"{_cache_key}_contados", {})
        ciclos      = st.session_state.get(f"{_cache_key}_ciclos", [])
        ciclo_ativo = st.session_state.get(f"{_cache_key}_ciclo_ativo")
        label       = f"{empresa_sel} — {filial_sel}"
        df_filial   = df_jlle[(df_jlle["Empresa"]==empresa_sel)&(df_jlle["Filial"]==filial_sel)].copy()
        # df_filial vazio = empresa/filial sem sufixo — usa df_jlle inteiro
        if df_filial.empty:
            df_filial = df_jlle.copy()
    else:
        contados = {}; ciclos = []; ciclo_ativo = None
        label = ""; df_filial = pd.DataFrame()

    if st.session_state.pop("ic_fechado_msg", False):
        st.success("✅ Inventário fechado e registrado no histórico KPMG!")

    if not df_filial.empty:
        df_score   = calcular_score(df_filial, tuple(sorted(contados.items())))
        total_skus = len(df_score)
        total_cont = sum(1 for p in df_score["Produto"].astype(str) if p in contados)
        pct_cob    = (total_cont/total_skus*100) if total_skus>0 else 0
    else:
        df_score = pd.DataFrame(); total_skus = 0; total_cont = 0; pct_cob = 0.0

    _uploads = ciclo_ativo.get("uploads", []) if ciclo_ativo else []
    ja_cont_ciclo = set()
    for u in _uploads:
        ja_cont_ciclo.update(str(p).strip().zfill(6) for p in u.get("produtos", []))
    pl_ciclo  = {str(p).strip().zfill(6) for p in (ciclo_ativo.get("produtos_lista",[]) if ciclo_ativo else [])}
    faltam    = pl_ciclo - ja_cont_ciclo
    pct_ciclo = len(ja_cont_ciclo & pl_ciclo) / len(pl_ciclo) * 100 if pl_ciclo else 0

    etapa_nav = st.session_state.get("ic_etapa_nav", 1)

    # Cards
    st.markdown("---")
    c1,c2,c3,c4,c5 = st.columns(5)
    b1 = _card(c1,1,"Gerar lista","Define o ciclo e gera a lista",
               ativo=(etapa_nav==1), concluido=(ciclo_ativo is not None), chave="ic_n1")
    b2 = _card(c2,2,"Upload WMS","Importa o resultado do WMS",
               ativo=(etapa_nav==2), concluido=(len(_uploads)>0), chave="ic_n2")
    b3 = _card(c3,3,"Adicionar etapa","Confirma e acumula uploads",
               ativo=(etapa_nav==3), concluido=(pct_ciclo>=100), chave="ic_n3")
    b4 = _card(c4,4,"Fechar inventário","Registra no histórico KPMG",
               ativo=(etapa_nav==4), concluido=(len(ciclos)>0), chave="ic_n4")
    b5 = _card(c5,5,"Histórico","Relatórios PDF dos ciclos fechados",
               ativo=(etapa_nav==5), concluido=False, chave="ic_n5")

    if b1: st.session_state["ic_etapa_nav"]=1; st.rerun()
    if b2: st.session_state["ic_etapa_nav"]=2; st.rerun()
    if b3: st.session_state["ic_etapa_nav"]=3; st.rerun()
    if b4: st.session_state["ic_etapa_nav"]=4; st.rerun()
    if b5: st.session_state["ic_etapa_nav"]=5; st.rerun()

    st.markdown("---")

    # ── ETAPA 1 ───────────────────────────────────────────────────────────
    if etapa_nav == 1:
        st.markdown("### 1. Gerar lista do ciclo")

        # ── Filtros internos ──────────────────────────────────────────────
        col_e, col_f, col_btn = st.columns([2, 2, 1])
        with col_e:
            empresas_disp = sorted(df_jlle["Empresa"].dropna().unique())
            emp_idx = empresas_disp.index(empresa_sel) if empresa_sel in empresas_disp else None
            emp_novo = st.selectbox("🏢 Empresa", [""] + empresas_disp,
                                    index=0 if emp_idx is None else emp_idx + 1,
                                    key="ic_emp_input")
        with col_f:
            if emp_novo:
                filiais_disp = sorted(df_jlle[df_jlle["Empresa"]==emp_novo]["Filial"].dropna().unique())
                fil_idx = filiais_disp.index(filial_sel) if filial_sel in filiais_disp else None
                fil_novo = st.selectbox("📍 Filial", [""] + filiais_disp,
                                        index=0 if fil_idx is None else fil_idx + 1,
                                        key="ic_fil_input")
            else:
                st.selectbox("📍 Filial", [""], key="ic_fil_input", disabled=True)
                fil_novo = ""
        with col_btn:
            st.markdown("<div style='margin-top:28px'>", unsafe_allow_html=True)
            btn_gerar = st.button("🔍 Gerar lista", type="primary", use_container_width=True, key="ic_btn_gerar")
            st.markdown("</div>", unsafe_allow_html=True)

        if btn_gerar:
            if not emp_novo or not fil_novo:
                st.warning("⚠️ Selecione Empresa e Filial antes de gerar a lista.")
                st.stop()
            st.session_state["ic_empresa_sel"] = emp_novo
            st.session_state["ic_filial_sel"]  = fil_novo
            # Invalida cache para forçar releitura
            _ck = f"ic_cache_{emp_novo}_{fil_novo}"
            st.session_state.pop(f"{_ck}_ts", None)
            st.rerun()

        # Só mostra métricas e lista se empresa/filial já estiverem selecionadas
        if not empresa_sel or not filial_sel or df_filial.empty:
            st.info("👆 Selecione a Empresa e a Filial acima e clique em **Gerar lista** para começar.")
            return

        data_aud = st.session_state.get("_data_auditoria")
        col_a,col_b = st.columns([3,2])
        col_a.caption(f"📅 Dados carregados em: **{data_aud or 'esta sessão'}**  |  🏢 **{label}**")
        col_b.caption("⚠️ Itens divergentes reaparecem mesmo após contados.")

        c1m,c2m,c3m,c4m = st.columns(4)
        c1m.metric("Total SKUs",  f"{total_skus:,}")
        c2m.metric("Divergentes", f"{int((df_score['Divergência']!=0).sum()):,}")
        c3m.metric("Curva A",     f"{int((df_score['Curva ABC']=='A').sum()):,}")
        c4m.metric("Valor Total", f"R$ {formatar_br(df_score['Vl Total ERP'].sum())}")

        cor_b = "#27AE60" if pct_cob>=100 else "#EC6E21"
        st.markdown(
            f"""<div style="background:#004550;border-radius:8px;padding:12px 16px;margin:8px 0;">
              <div style="display:flex;justify-content:space-between;margin-bottom:6px;">
                <span style="color:#fff;">✅ <b>{total_cont}</b> contados &nbsp;|&nbsp; ⬜ <b>{total_skus-total_cont}</b> pendentes</span>
                <span style="color:{cor_b};font-weight:bold;">{pct_cob:.1f}%</span>
              </div>
              <div style="background:#003040;border-radius:4px;height:10px;">
                <div style="background:{cor_b};width:{min(pct_cob,100):.1f}%;height:10px;border-radius:4px;"></div>
              </div></div>""", unsafe_allow_html=True)
        if pct_cob>=100: st.success("🎉 Todos os SKUs contados! Exigência KPMG cumprida.")

        modo = st.radio("Modo", ["Quantidade fixa","Percentual"], horizontal=True, key="ic_modo")
        if modo == "Quantidade fixa":
            st.caption("📌 Conta um número fixo de produtos por ciclo.")
            cols_b = st.columns(4)
            if "ic_qtd" not in st.session_state: st.session_state.ic_qtd = 2
            for cb,qtd in zip(cols_b,[2,30,50,80]):
                with cb:
                    if st.button(f"{qtd}",key=f"ic_q{qtd}",type="primary" if st.session_state.ic_qtd==qtd else "secondary"):
                        st.session_state.ic_qtd=qtd
            qtd_ciclo = min(st.session_state.ic_qtd, total_skus)
        else:
            st.caption("📊 **5%** = 20 ciclos/ano · **10%** = 10 ciclos/ano · **20%** = 5 ciclos/ano")
            pmap = {"5%":0.05,"10%":0.10,"20%":0.20,"30%":0.30}
            pl   = st.select_slider("Faixa",list(pmap.keys()),value="10%",key="ic_pct")
            qtd_ciclo = max(1,int(total_skus*pmap[pl]))
            st.caption(f"→ {qtd_ciclo} itens de {total_skus}")

        df_lista = montar_lista(df_score, qtd_ciclo, contados)
        qp = int((df_lista["Origem"]=="🔴 Alta prioridade").sum())
        qk = int((df_lista["Origem"]=="⬜ Cobertura KPMG").sum())
        st.markdown(f"**{len(df_lista)} itens** — <span style='color:#EC6E21'>🔴 {qp} prioridade</span> · <span style='color:#27AE60'>⬜ {qk} KPMG</span>",
                    unsafe_allow_html=True)

        cols_ex = [c for c in ["Produto","Descrição","Empresa","Filial","Curva ABC","Score","Já Contado",
                                "Dias s/ Contagem","Saldo ERP (Total)","Saldo WMS","Divergência","Vl Total ERP","Motivo","Origem"]
                   if c in df_lista.columns]
        df_exib = df_lista[cols_ex]
        st.dataframe(
            df_exib.style
            .apply(lambda r: ["background-color:#005562;color:#fff;font-size:0.84rem;"]*len(r), axis=1)
            .set_table_styles([{"selector":"thead th","props":[("background-color","#004550"),("color","#fff"),("border-bottom","2px solid #EC6E21")]}])
            .format({"Saldo ERP (Total)":"{:,.2f}","Saldo WMS":"{:,.2f}","Divergência":"{:,.2f}",
                     "Vl Total ERP":"R$ {:,.2f}","Score":"{:.2f}","Dias s/ Contagem":"{:.0f}d"}, na_rep="-"),
            use_container_width=True, hide_index=False)

        num_ciclo = f"{date.today().strftime('%Y%m%d')}-{empresa_sel}-{filial_sel}".replace(" ","")
        col_dl,col_info = st.columns([2,2])
        with col_dl:
            st.download_button("📥 Baixar Excel para Contagem",
                data=gerar_xlsx_lista(df_exib,label),
                file_name=f"inv_{num_ciclo}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with col_info:
            st.info(f"Nº do ciclo: **{num_ciclo}**")

        db_salvar_ciclo_ativo(engine_db, empresa_sel, filial_sel, {
            "num_ciclo":      num_ciclo,
            "data_geracao":   date.today().strftime("%d/%m/%Y"),
            "label":          label,
            "qtd_lista":      len(df_exib),
            "produtos_lista": df_exib["Produto"].astype(str).tolist(),
            "uploads":        _uploads,
            "status":         "Em andamento",
        })

    # ── ETAPA 2 ───────────────────────────────────────────────────────────
    elif etapa_nav == 2:
        if not empresa_sel or not filial_sel:
            st.warning("⚠️ Gere a lista primeiro (Etapa 1) para definir Empresa e Filial."); return
        st.markdown("### 2. Upload do resultado WMS")
        st.caption("Faça o upload do Excel gerado pelo WMS após a contagem.")
        if not ciclo_ativo:
            st.warning("⚠️ Nenhum ciclo ativo. Vá para **Gerar lista** primeiro."); return

        arquivo = st.file_uploader("Selecione o arquivo Excel do WMS",type=["xlsx"],
                                    key=f"ic_upload_{len(_uploads)}")
        if arquivo:
            try:
                res    = processar_resultado_wms(arquivo)
                df_wms = res["df"]
                c1m,c2m,c3m,c4m = st.columns(4)
                for cm,lbl,val in [(c1m,"Nº Inventário",res.get("num_inv","—")),
                                    (c2m,"Data",res.get("data","—")),
                                    (c3m,"Responsável",res.get("responsavel","—")),
                                    (c4m,"Acuracidade",res.get("acuracidade","—"))]:
                    cm.markdown(
                        f"""<div style="border:2px solid #EC6E21;border-radius:10px;padding:12px;background:#004550;">
                          <div style="color:#aaa;font-size:0.8rem;">{lbl}</div>
                          <div style="color:#fff;font-size:1rem;font-weight:700;word-break:break-word;">{val}</div>
                        </div>""", unsafe_allow_html=True)

                prods_wms   = {str(p).strip().zfill(6) for p in res["produtos"]}
                novos       = (pl_ciclo & prods_wms) - ja_cont_ciclo
                ja_tnh      = pl_ciclo & prods_wms & ja_cont_ciclo
                fora        = prods_wms - pl_ciclo

                st.markdown("#### Resultado desta etapa")
                ca,cb,cc = st.columns(3)
                ca.metric("✅ Novos contados",    len(novos))
                cb.metric("🔁 Já contados antes", len(ja_tnh))
                cc.metric("➕ Fora da lista",      len(fora))

                df_wms["Status"] = df_wms["Codigo"].apply(
                    lambda p: "✅ Novo" if p in novos else "🔁 Já contado" if p in ja_tnh
                              else "➕ Fora da lista" if p in fora else "—")

                # ── Enriquece com ERP ──────────────────────────────────────
                erp_lookup = {}
                if not df_filial.empty and "Produto" in df_filial.columns:
                    _erp = df_filial[["Produto"] +
                        [c for c in ["Saldo ERP (Total)","Vl Unit","Vl Total ERP"] if c in df_filial.columns]
                    ].copy()
                    for col in ["Saldo ERP (Total)","Vl Unit","Vl Total ERP"]:
                        if col in _erp.columns:
                            _erp[col] = pd.to_numeric(_erp[col], errors="coerce").fillna(0)
                    _erp["Produto"] = _erp["Produto"].astype(str).str.zfill(6)
                    _erp = _erp.groupby("Produto", as_index=False).sum()
                    erp_lookup = _erp.set_index("Produto").to_dict("index")

                df_wms["Saldo ERP (Total)"] = df_wms["Codigo"].map(lambda p: erp_lookup.get(p, {}).get("Saldo ERP (Total)", 0))
                df_wms["Vl Unit"]           = df_wms["Codigo"].map(lambda p: erp_lookup.get(p, {}).get("Vl Unit", 0))
                df_wms["Saldo WMS"]         = pd.to_numeric(df_wms["Qtd Antes"],  errors="coerce").fillna(0)
                df_wms["Invent WMS"]        = pd.to_numeric(df_wms["Qtd Depois"], errors="coerce").fillna(0)
                df_wms["Diferença Invent"]  = df_wms["Saldo ERP (Total)"] - df_wms["Invent WMS"]
                df_wms["Vl Total ERP"]      = df_wms["Saldo ERP (Total)"] * df_wms["Vl Unit"]
                df_wms["Vl Total Dif."]     = df_wms["Diferença Invent"]  * df_wms["Vl Unit"]

                # Monta df de exibição com nomes e ordem exatos pedidos
                df_exib_wms = df_wms[["Codigo","Descricao",
                    "Saldo ERP (Total)","Saldo WMS","Invent WMS",
                    "Diferença Invent","Acuracidade",
                    "Vl Total ERP","Vl Total Dif.","Status"]].copy()
                df_exib_wms = df_exib_wms.rename(columns={
                    "Saldo ERP (Total)": "Saldo ERP",
                    "Vl Total Dif.":     "Vl Total Diferença"
                })

                def _style_dif(val):
                    try:
                        v = float(str(val).replace("R$","").replace(",","").replace(" ","").replace("+","").replace("−","").replace("-",""))
                        raw = str(val)
                        neg = raw.startswith("-") or raw.startswith("−")
                        if neg: return "color:#C0392B;font-weight:bold"
                        if v > 0: return "color:#C0392B;font-weight:bold"
                        if v < 0: return "color:#27AE60;font-weight:bold"
                    except: pass
                    return ""

                def _fmt_dif(v):
                    try:
                        f = float(v)
                        if f != 0:
                            return f"-{abs(f):,.2f}" if f > 0 else f"+{abs(f):,.2f}"
                        return f"{f:,.2f}"
                    except:
                        return v

                def _fmt_vl_dif(v):
                    try:
                        f = float(v)
                        if f != 0:
                            return f"R$ -{abs(f):,.2f}" if f > 0 else f"R$ +{abs(f):,.2f}"
                        return f"R$ {f:,.2f}"
                    except:
                        return v

                st.dataframe(
                    df_exib_wms.style
                    .applymap(_style_dif, subset=["Diferença Invent","Vl Total Diferença"])
                    .format({
                        "Saldo ERP":       "{:,.2f}",
                        "Saldo WMS":       "{:,.2f}",
                        "Invent WMS":      "{:,.2f}",
                        "Diferença Invent": _fmt_dif,
                        "Vl Total ERP":    "R$ {:,.2f}",
                        "Vl Total Diferença": _fmt_vl_dif
                    }, na_rep="—"),
                    use_container_width=True, hide_index=True)

                # Preview PDF do upload atual
                _up_prev = {
                    "num_ciclo": ciclo_ativo.get("num_ciclo","—"),
                    "data_geracao": ciclo_ativo.get("data_geracao","—"),
                    "data": res.get("data","—"),
                    "responsavel": res.get("responsavel","—"),
                    "acuracidade": res.get("acuracidade","—"),
                    "cobertura_pct": pct_ciclo,
                }
                _df_prev = df_exib_wms.rename(columns={
                    "Saldo ERP":        "Saldo ERP (Total)",
                    "Vl Total Diferença":"Vl Total Diferença",
                }).copy()
                _pdf_prev = gerar_pdf_kpmg(_up_prev, _df_prev, empresa_sel, filial_sel)
                if _pdf_prev:
                    st.download_button(
                        "📄 Baixar PDF desta etapa",
                        data=_pdf_prev,
                        file_name=f"kpmg_etapa_{res.get('num_inv','')}.pdf",
                        mime="application/pdf",
                        key="ic_pdf_etapa"
                    )

                # Salva linhas para o relatório PDF (com nomes padronizados)
                df_rows_save = df_wms[["Codigo","Descricao","Qtd Antes","Qtd Depois","Acuracidade"]].copy()
                df_rows_save = df_rows_save.rename(columns={"Qtd Antes":"Saldo WMS","Qtd Depois":"Invent WMS"})

                st.session_state["ic_upload_pendente"] = {
                    "num_inv":res.get("num_inv","—"),"data":res.get("data","—"),
                    "data_iso":res.get("data_iso",date.today().isoformat()),
                    "responsavel":res.get("responsavel","—"),"acuracidade":res.get("acuracidade","—"),
                    "produtos":list(pl_ciclo & prods_wms),
                    "df_rows": df_rows_save.to_dict("records"),
                }
                st.info("✔ Arquivo carregado. Vá para **Adicionar etapa** para confirmar.")
            except Exception as e:
                st.error(f"Erro ao processar arquivo: {e}")

    # ── ETAPA 3 ───────────────────────────────────────────────────────────
    elif etapa_nav == 3:
        if not empresa_sel or not filial_sel:
            st.warning("⚠️ Gere a lista primeiro (Etapa 1) para definir Empresa e Filial."); return
        st.markdown("### 3. Adicionar etapa ao ciclo")
        if not ciclo_ativo:
            st.warning("⚠️ Nenhum ciclo ativo. Vá para **Gerar lista** primeiro."); return

        cor_p = "#27AE60" if pct_ciclo>=100 else "#EC6E21"
        st.markdown(
            f"""<div style="background:#004550;border-radius:8px;padding:12px 16px;margin-bottom:12px;">
              <div style="display:flex;justify-content:space-between;margin-bottom:6px;">
                <span style="color:#fff;">✅ <b>{len(ja_cont_ciclo & pl_ciclo)}</b> contados &nbsp;|&nbsp;
                ⬜ <b>{len(faltam)}</b> pendentes &nbsp;|&nbsp; 📤 <b>{len(_uploads)}</b> upload(s)</span>
                <span style="color:{cor_p};font-weight:bold;">{pct_ciclo:.1f}%</span>
              </div>
              <div style="background:#003040;border-radius:4px;height:10px;">
                <div style="background:{cor_p};width:{min(pct_ciclo,100):.1f}%;height:10px;border-radius:4px;"></div>
              </div></div>""", unsafe_allow_html=True)

        if _uploads:
            with st.expander(f"📤 Uploads realizados ({len(_uploads)})"):
                st.dataframe(pd.DataFrame([{"Etapa":i+1,"Nº Inventário":u.get("num_inv","—"),
                    "Data":u.get("data","—"),"Responsável":u.get("responsavel","—"),
                    "Acuracidade":u.get("acuracidade","—"),"Produtos":len(u.get("produtos",[]))}
                    for i,u in enumerate(_uploads)]), use_container_width=True, hide_index=True)

        up = st.session_state.get("ic_upload_pendente")
        if up:
            st.markdown("#### Upload pronto para adicionar:")
            st.markdown(
                f"""<div style="border:1.5px solid #27AE60;border-radius:8px;padding:12px;background:#E8F5E9;color:#27500A;">
                  <b>Nº {up["num_inv"]}</b> · {up["data"]} · {up["responsavel"]} · {up["acuracidade"]} · <b>{len(up["produtos"])} produtos</b>
                </div>""", unsafe_allow_html=True)
            if st.button("📥 Confirmar e adicionar esta etapa", key="ic_add", type="primary"):
                try:
                    ciclo_f = db_obter_ciclo_ativo(engine_db, empresa_sel, filial_sel)
                    ups_at  = ciclo_f.get("uploads",[]) if ciclo_f else []
                    ups_at.append(up)
                    with engine_db.connect() as conn:
                        conn.execute(text("""
                            UPDATE inventario_ciclo_ativo
                            SET uploads_json=:v, atualizado_em=NOW()
                            WHERE empresa=:e AND filial=:f
                        """), {"v":json.dumps(ups_at),"e":empresa_sel,"f":filial_sel})
                        conn.commit()
                    del st.session_state["ic_upload_pendente"]
                    st.session_state["ic_force_reload"]=True
                    st.session_state.pop(f"ic_cache_{empresa_sel}_{filial_sel}", None)
                    st.success(f"✅ Etapa adicionada! {len(up['produtos'])} produtos contados.")
                    st.rerun()
                except Exception as err:
                    st.error(f"Erro ao adicionar: {err}")
        else:
            st.info("Faça o upload na etapa **Upload WMS** primeiro, depois volte aqui para confirmar.")

    # ── ETAPA 4 ───────────────────────────────────────────────────────────
    elif etapa_nav == 4:
        if not empresa_sel or not filial_sel:
            st.warning("⚠️ Gere a lista primeiro (Etapa 1) para definir Empresa e Filial."); return
        st.markdown("### 4. Fechar inventário")

        if ciclo_ativo:
            st.markdown(
                f"""<div style="background:#004550;border-radius:8px;padding:12px 16px;margin-bottom:12px;">
                  <div style="display:flex;justify-content:space-between;align-items:center;">
                    <div><span style="color:#EC6E21;font-weight:bold;">📋 {ciclo_ativo["num_ciclo"]}</span><br>
                    <span style="color:#fff;font-size:0.85rem;">Gerado em {ciclo_ativo["data_geracao"]} · {ciclo_ativo["qtd_lista"]} produtos · {len(_uploads)} upload(s)</span></div>
                    <span style="color:#EC6E21;font-weight:bold;font-size:1.2rem;">{pct_ciclo:.1f}%</span>
                  </div></div>""", unsafe_allow_html=True)

            if pct_ciclo >= 100:
                st.success("✅ 100% dos itens contados! Pronto para fechar.")
                # Preview do relatório antes de fechar
                _df_prev4 = montar_df_relatorio(_uploads, df_filial)
                if not _df_prev4.empty:
                    _pdf_prev4 = gerar_pdf_kpmg(
                        {**ciclo_ativo,
                         "data": _uploads[-1].get("data","—") if _uploads else "—",
                         "responsavel": _uploads[-1].get("responsavel","—") if _uploads else "—",
                         "acuracidade": _uploads[-1].get("acuracidade","—") if _uploads else "—",
                         "cobertura_pct": pct_ciclo},
                        _df_prev4, empresa_sel, filial_sel)
                    if _pdf_prev4:
                        st.download_button(
                            "📄 Pré-visualizar Relatório PDF",
                            data=_pdf_prev4,
                            file_name=f"kpmg_preview_{ciclo_ativo.get('num_ciclo','')}.pdf",
                            mime="application/pdf",
                            key="ic_pdf_preview4"
                        )
                if st.button("🏁 Fechar inventário", key="ic_fechar", type="primary"):
                    todos = set()
                    for u in _uploads: todos.update(str(p).strip().zfill(6) for p in u.get("produtos",[]))
                    data_iso = _uploads[-1].get("data_iso",date.today().isoformat()) if _uploads else date.today().isoformat()
                    pct_f    = len(todos & pl_ciclo)/len(pl_ciclo)*100 if pl_ciclo else 0
                    # Monta e serializa df_relatorio para guardar no ciclo
                    df_rel   = montar_df_relatorio(_uploads, df_filial)
                    rel_json = df_rel.to_json(orient="records", force_ascii=False) if not df_rel.empty else "[]"
                    cf = {**ciclo_ativo,"uploads":len(_uploads),"produtos_contados":list(todos),
                          "cobertura_pct":pct_f,"status":"Concluído",
                          "num_inv":_uploads[-1].get("num_inv","—") if _uploads else "—",
                          "data":_uploads[-1].get("data","—") if _uploads else "—",
                          "responsavel":_uploads[-1].get("responsavel","—") if _uploads else "—",
                          "acuracidade":_uploads[-1].get("acuracidade","—") if _uploads else "—",
                          "relatorio_json": rel_json}
                    db_gravar_ciclo(engine_db,empresa_sel,filial_sel,cf)
                    db_marcar_contados(engine_db,empresa_sel,filial_sel,list(todos),
                                       data=data_iso,num_ciclo=ciclo_ativo.get("num_ciclo",""))
                    db_fechar_ciclo_ativo(engine_db,empresa_sel,filial_sel)
                    st.session_state["ic_fechado_msg"]=True
                    st.session_state["ic_etapa_nav"]=1
                    st.session_state["ic_force_reload"]=True
                    st.session_state.pop(f"ic_cache_{empresa_sel}_{filial_sel}", None)
                    st.rerun()
            else:
                col_bt,col_ms = st.columns([1,3])
                with col_bt: st.button("🏁 Fechar inventário",key="ic_fbl",disabled=True)
                with col_ms:
                    fl = sorted(list(faltam))[:10]
                    ms = len(faltam)-10 if len(faltam)>10 else 0
                    ls = ", ".join(f"`{p}`" for p in fl) + (f" e mais {ms}" if ms else "")
                    st.warning(f"⚠️ Faltam **{len(faltam)}** produtos: {ls}")
        else:
            st.info("Nenhum ciclo ativo no momento.")

    # ── ETAPA 5 — HISTÓRICO ───────────────────────────────────────────────
    elif etapa_nav == 5:
        if not empresa_sel or not filial_sel:
            st.warning("⚠️ Gere a lista primeiro (Etapa 1) para definir Empresa e Filial."); return
        st.markdown("### 5. Histórico KPMG")

        if not ciclos:
            st.info("Nenhum ciclo fechado ainda. Feche um inventário para ver o histórico aqui.")
            return

        st.caption("☑️ Selecione um ou mais ciclos para gerar o PDF. Ao selecionar mais de um, o relatório será consolidado.")

        # Monta dfs_rel para cada ciclo (necessário para o PDF)
        dfs_rel_todos = {}
        for c in ciclos:
            num_c    = c.get("num_ciclo","")
            rel_json = c.get("relatorio_json","[]")
            try:
                df_c = pd.read_json(io.StringIO(rel_json), orient="records") \
                       if rel_json and rel_json != "[]" else pd.DataFrame()
            except Exception:
                df_c = pd.DataFrame()
            if df_c.empty and not df_filial.empty:
                ups_c = c.get("uploads_raw",[])
                if ups_c:
                    df_c = montar_df_relatorio(ups_c, df_filial)
            dfs_rel_todos[num_c] = df_c

        # Tabela com checkboxes inline
        sel_ciclos = []
        col_ck, col_ciclo, col_data, col_resp, col_acur, col_sku, col_cob, col_status, col_pdf = st.columns(
            [0.5, 2.5, 1.5, 2.5, 1.2, 1, 1.2, 1.2, 1.5])
        col_ck.markdown("**☑**")
        col_ciclo.markdown("**Nº Ciclo**")
        col_data.markdown("**Data**")
        col_resp.markdown("**Responsável**")
        col_acur.markdown("**Acuracidade**")
        col_sku.markdown("**SKUs**")
        col_cob.markdown("**Cobertura**")
        col_status.markdown("**Status**")
        col_pdf.markdown("**PDF**")
        st.markdown('<hr style="margin:4px 0;border-color:#EC6E21;">', unsafe_allow_html=True)

        for c in ciclos:
            num_c = c.get("num_ciclo","—")
            df_c  = dfs_rel_todos.get(num_c, pd.DataFrame())
            n_sku = len(df_c) if not df_c.empty else c.get("qtd_contados", len(c.get("produtos_contados",[])))

            ck, cc, cd, cr, ca, cs, ccob, cst, cpdf = st.columns(
                [0.5, 2.5, 1.5, 2.5, 1.2, 1, 1.2, 1.2, 1.5])
            checked = ck.checkbox("", key=f"ck5_{num_c}", label_visibility="collapsed")
            cc.caption(num_c)
            cd.caption(c.get("data","—"))
            cr.caption(c.get("responsavel","—"))
            ca.caption(str(c.get("acuracidade","—")))
            cs.caption(str(n_sku))
            ccob.caption(f"{c.get('cobertura_pct',0):.1f}%")
            cst.caption(c.get("status","—"))

            # Botão PDF individual (sempre disponível, independente do checkbox)
            if cpdf.button("📄", key=f"pdf5_ind_{num_c}", help=f"PDF — {num_c}"):
                pdf_b = gerar_pdf_kpmg_consolidado([c], dfs_rel_todos, empresa_sel, filial_sel)
                if pdf_b:
                    st.session_state[f"_pdf5_bytes_{num_c}"] = pdf_b
                    st.session_state[f"_pdf5_nome_{num_c}"]  = f"kpmg_{num_c}.pdf"

            # Download aparece logo abaixo se gerado
            if st.session_state.get(f"_pdf5_bytes_{num_c}"):
                st.download_button(
                    f"⬇ {num_c}",
                    data=st.session_state[f"_pdf5_bytes_{num_c}"],
                    file_name=st.session_state[f"_pdf5_nome_{num_c}"],
                    mime="application/pdf",
                    key=f"dl5_{num_c}")

            if checked:
                sel_ciclos.append(c)

        # Botão PDF consolidado (aparece quando 2+ selecionados)
        st.markdown("---")
        if len(sel_ciclos) >= 2:
            st.info(f"**{len(sel_ciclos)} ciclos selecionados** — o PDF será consolidado.")
            if st.button("📄 Gerar PDF Consolidado", type="primary", key="btn_pdf_consol"):
                pdf_b = gerar_pdf_kpmg_consolidado(sel_ciclos, dfs_rel_todos, empresa_sel, filial_sel)
                if pdf_b:
                    nomes = "_".join(c.get("num_ciclo","") for c in sel_ciclos[:2])
                    st.download_button(
                        "⬇ Baixar PDF Consolidado",
                        data=pdf_b,
                        file_name=f"kpmg_consolidado_{nomes}.pdf",
                        mime="application/pdf",
                        key="dl5_consol")
                else:
                    st.error("⚠️ ReportLab não disponível. Adicione `reportlab` ao requirements.txt.")
        elif len(sel_ciclos) == 1:
            st.info("1 ciclo selecionado — clique em 📄 na linha ou selecione mais para consolidar.")

        col_dl, col_rs = st.columns([3, 1])
        with col_dl:
            st.download_button(
                "📥 Exportar Histórico (Excel)",
                data=gerar_xlsx_historico(ciclos, label),
                file_name=f"historico_kpmg_{empresa_sel}_{filial_sel}_{date.today().strftime('%d%m%Y')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="ic_dl_hist5")
        with col_rs:
            if st.button("🔄 Novo período", key="ic_reset5"):
                db_resetar_tudo(engine_db, empresa_sel, filial_sel)
                st.session_state["ic_etapa_nav"] = 1
                st.success("Novo período iniciado!")
                st.rerun()
