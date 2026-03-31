import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import date, datetime

# ── Gerador de relatório PDF KPMG ─────────────────────────────────────────────
try:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                     TableStyle, PageBreak, HRFlowable)
    from reportlab.platypus import Image as RLImage
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    PDF_DISPONIVEL = True
except ImportError:
    PDF_DISPONIVEL = False

PERIODO_KPMG_DIAS = 365

# Importa módulo de persistência (Supabase)
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

# ── Helpers ───────────────────────────────────────────────────────────────────

def _slug(empresa, filial):
    return f"{empresa}_{filial}".replace(" ", "_").replace("-", "")

def produtos_contados_no_ciclo(engine, e, f) -> set:
    """Retorna união de todos os produtos contados nos uploads do ciclo ativo."""
    ciclo = db_obter_ciclo_ativo(engine, e, f)
    if not ciclo:
        return set()
    todos = set()
    for u in ciclo.get("uploads", []):
        todos.update(u.get("produtos", []))
    return todos

def fechar_ciclo(engine, e, f):
    """Fecha o ciclo, persiste no banco e marca produtos como contados."""
    ciclo   = db_obter_ciclo_ativo(engine, e, f)
    if not ciclo:
        return
    uploads = ciclo.get("uploads", [])
    todos_contados = set()
    for u in uploads:
        todos_contados.update(u.get("produtos", []))
    data_iso = uploads[-1].get("data_iso", date.today().isoformat()) if uploads else date.today().isoformat()

    produtos_lista = set(ciclo.get("produtos_lista", []))
    pct = len(todos_contados & produtos_lista) / len(produtos_lista) * 100 if produtos_lista else 0

    ciclo_fechado = {
        **ciclo,
        "uploads":           len(uploads),
        "produtos_contados": list(todos_contados),
        "cobertura_pct":     pct,
        "data_fechamento":   date.today().strftime("%d/%m/%Y"),
        "status":            "Concluído",
        "num_inv":           uploads[-1].get("num_inv","—")     if uploads else "—",
        "data":              uploads[-1].get("data","—")        if uploads else "—",
        "responsavel":       uploads[-1].get("responsavel","—") if uploads else "—",
        "acuracidade":       uploads[-1].get("acuracidade","—") if uploads else "—",
    }
    db_gravar_ciclo(engine, e, f, ciclo_fechado)
    db_marcar_contados(engine, e, f, list(todos_contados),
                       data=data_iso, num_ciclo=ciclo.get("num_ciclo",""))
    db_fechar_ciclo_ativo(engine, e, f)


# ── Processador resultado WMS ─────────────────────────────────────────────────

def processar_resultado_wms(arquivo) -> dict:
    xls     = pd.ExcelFile(arquivo)
    df_meta = pd.read_excel(xls, sheet_name=xls.sheet_names[0], header=None, nrows=7)
    meta    = {}
    for _, row in df_meta.iterrows():
        chave = str(row.iloc[0]).strip().replace(":", "")
        valor = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
        if "mero" in chave or "Numero" in chave:  meta["num_inv"]     = valor
        elif "Data" in chave:                      meta["data"]        = valor
        elif "onsav" in chave or "espons" in chave:meta["responsavel"] = valor
        elif "curac" in chave:                     meta["acuracidade"] = valor

    df = pd.read_excel(xls, sheet_name=xls.sheet_names[0], skiprows=8, header=0)
    df.columns = ["Produto","Qtd Antes","Vl Antes","Qtd Depois","Vl Depois","Qtd Diferença","Vl Diferença","Acuracidade"]
    df = df.dropna(subset=["Produto"])
    df = df[df["Produto"].astype(str).str.strip() != ""]
    df["Codigo"] = df["Produto"].astype(str).str.split(" - ", n=1).str[0].str.strip().str.zfill(6)
    df["Descricao"] = df["Produto"].astype(str).str.split(" - ", n=1).str[1].str.strip().fillna("")
    for col in ["Qtd Antes","Qtd Depois","Qtd Diferença"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    try:
        data_iso = datetime.strptime(meta.get("data",""), "%d/%m/%Y").date().isoformat()
    except Exception:
        data_iso = date.today().isoformat()

    meta["df"]       = df
    meta["produtos"] = df["Codigo"].tolist()
    meta["data_iso"] = data_iso
    return meta


# ── Score ─────────────────────────────────────────────────────────────────────

def calcular_score(df: pd.DataFrame, contados: dict) -> pd.DataFrame:
    df = df.copy()
    for col in ["Saldo ERP (Total)","Saldo WMS","Vl Unit","Vl Total ERP","Divergência"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Consolida localizações por Produto+Armazem
    # - Saldo WMS: soma todas as localizações do mesmo armazém
    # - Saldo ERP (Total): já é o saldo do armazém (não soma entre armazéns diferentes)
    # - Depois soma os armazéns para obter o total por produto
    chave_arm = [c for c in ["Produto", "Armazem"] if c in df.columns]
    if "Produto" in df.columns and len(df) > df["Produto"].nunique():
        cols_soma_wms = [c for c in ["Saldo WMS"] if c in df.columns]
        cols_fixos_arm = [c for c in ["Produto","Armazem","Descrição","Empresa","Filial",
                                       "Saldo ERP (Total)","Vl Unit","Vl Total ERP"] if c in df.columns]
        # Passo 1: consolida por Produto+Armazem (soma WMS, ERP já é único por armazem)
        df_wms_arm = df.groupby(chave_arm, as_index=False)[cols_soma_wms].sum() if cols_soma_wms else df[chave_arm].drop_duplicates()
        df_erp_arm = df[cols_fixos_arm].drop_duplicates(subset=chave_arm, keep="first")
        df_arm     = df_erp_arm.merge(df_wms_arm, on=chave_arm, how="left")
        df_arm["Divergência"] = df_arm["Saldo ERP (Total)"] - df_arm["Saldo WMS"]

        # Passo 2: consolida por Produto (soma todos os armazéns)
        cols_soma_prod = [c for c in ["Saldo WMS","Saldo ERP (Total)","Divergência","Vl Total ERP"] if c in df_arm.columns]
        cols_fixos_prod = [c for c in ["Produto","Descrição","Empresa","Filial","Vl Unit"] if c in df_arm.columns]
        df_soma  = df_arm.groupby("Produto", as_index=False)[cols_soma_prod].sum()
        df_fixo  = df_arm[cols_fixos_prod].drop_duplicates(subset=["Produto"], keep="first")
        df       = df_fixo.merge(df_soma, on="Produto", how="left")

    # Curva ABC
    df = df.sort_values("Vl Total ERP", ascending=False).reset_index(drop=True)
    tv = df["Vl Total ERP"].sum()
    df["pct_acum"] = df["Vl Total ERP"].cumsum() / tv if tv > 0 else 0
    df["Curva ABC"] = np.where(df["pct_acum"]<=0.80,"A", np.where(df["pct_acum"]<=0.95,"B","C"))
    df["score_abc"] = df["Curva ABC"].map({"A":10,"B":6,"C":3})
    df["score_diverg"] = np.where(df["Divergência"]!=0, 10, 0)
    mv = df["Vl Total ERP"].max() or 1
    df["score_valor"] = (df["Vl Total ERP"]/mv*10).round(2)

    hoje = date.today()
    def dias(p):
        p = str(p)
        if p in contados:
            try: return (hoje - date.fromisoformat(contados[p])).days
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
        if r["Curva ABC"]=="A":             rs.append("Curva A")
        if r["Divergência"]!=0:             rs.append("Divergência")
        if r["Dias s/ Contagem"]>=PERIODO_KPMG_DIAS: rs.append("Nunca contado")
        elif r["Dias s/ Contagem"]>180:     rs.append(f"{r['Dias s/ Contagem']}d sem contar")
        if r["Vl Total ERP"]>0:             rs.append(f"R$ {r['Vl Total ERP']:,.0f}")
        return " · ".join(rs) if rs else "Em estoque"

    df["Motivo"] = df.apply(motivo, axis=1)
    df = df.sort_values("Score", ascending=False).reset_index(drop=True)
    df.index = df.index + 1
    return df


def montar_lista(df_score, qtd, contados):
    nao_cont = set(df_score[~df_score["Produto"].astype(str).isin(contados)]["Produto"].astype(str))
    # Camada 1: Top N por score
    top = df_score.head(qtd).copy()
    top["Origem"] = top["Produto"].astype(str).apply(
        lambda p: "⬜ Cobertura KPMG" if p in nao_cont else "🔴 Alta prioridade"
    )
    ja = set(top["Produto"].astype(str))
    # Camada 2: nunca contados que faltam para completar a cota
    # Só adiciona se ainda há vagas (qtd - len(top que já é nunca contado))
    vagas = qtd - len(top)
    if vagas > 0:
        extras = df_score[
            df_score["Produto"].astype(str).isin(nao_cont) &
            ~df_score["Produto"].astype(str).isin(ja)
        ].head(vagas).copy()
        if not extras.empty:
            extras["Origem"] = "⬜ Cobertura KPMG"
            top = pd.concat([top, extras], ignore_index=True)
    top = top.reset_index(drop=True)
    top.index = top.index + 1
    return top


# ── Excel ─────────────────────────────────────────────────────────────────────

def _fmt_header(wb):
    return wb.add_format({"bold":True,"bg_color":"#004550","font_color":"#FFFFFF",
                           "border":1,"border_color":"#EC6E21","align":"center"})

def gerar_xlsx_lista(df, label):
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as w:
        df.to_excel(w, index=True, index_label="Ranking", sheet_name="Lista")
        wb, ws = w.book, w.sheets["Lista"]
        fh = _fmt_header(wb)
        fA = wb.add_format({"bg_color":"#FFF3CD","border":1,"border_color":"#dee2e6"})
        fB = wb.add_format({"bg_color":"#D1ECF1","border":1,"border_color":"#dee2e6"})
        fC = wb.add_format({"bg_color":"#F8F9FA","border":1,"border_color":"#dee2e6"})
        fK = wb.add_format({"bg_color":"#E8F5E9","border":1,"border_color":"#dee2e6"})
        for i,c in enumerate(["Ranking"]+list(df.columns)):
            ws.write(0,i,c,fh); ws.set_column(i,i,max(len(str(c))+4,14))
        for r,(idx,row) in enumerate(df.iterrows(),1):
            fmt = fK if str(row.get("Origem","")).startswith("⬜") else \
                  fA if row.get("Curva ABC")=="A" else fB if row.get("Curva ABC")=="B" else fC
            ws.write(r,0,idx,fmt)
            for c,v in enumerate(row,1): ws.write(r,c,v,fmt)
    out.seek(0); return out.getvalue()


def gerar_xlsx_historico(ciclos, label):
    if not ciclos: return b""
    rows = []
    for c in ciclos:
        rows.append({
            "Nº Ciclo":       c.get("num_ciclo","—"),
            "Data Geração":   c.get("data_geracao","—"),
            "Unidade":        c.get("label","—"),
            "SKUs na Lista":  c.get("qtd_lista",0),
            "Nº Inventário":  c.get("num_inv","—"),
            "Data Contagem":  c.get("data","—"),
            "Responsável":    c.get("responsavel","—"),
            "Acuracidade":    c.get("acuracidade","—"),
            "SKUs Contados":  len(c.get("produtos_contados",[])),
            "Cobertura %":    f"{c.get('cobertura_pct',0):.1f}%",
            "Status":         c.get("status","—"),
        })
    df = pd.DataFrame(rows)
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="Histórico KPMG")
        wb, ws = w.book, w.sheets["Histórico KPMG"]
        fh = _fmt_header(wb)
        fr = wb.add_format({"border":1,"border_color":"#dee2e6"})
        for i,c in enumerate(df.columns):
            ws.write(0,i,c,fh); ws.set_column(i,i,max(len(str(c))+4,18))
        for r,row in df.iterrows():
            for c,v in enumerate(row): ws.write(r+1,c,v,fr)
        ws.write(len(df)+2, 0,
                 f"Unidade: {label} | Gerado em: {date.today().strftime('%d/%m/%Y')}",
                 wb.add_format({"italic":True,"font_color":"#666666"}))
    out.seek(0); return out.getvalue()




def gerar_relatorio_kpmg_pdf(ciclos: list, label_unidade: str,
                              total_skus: int, contados_global: dict) -> bytes:
    """
    Gera o relatório PDF formal para entrega à KPMG.

    Seções:
      1. Capa
      2. Resumo executivo (KPIs)
      3. Gráfico de cobertura
      4. Gráfico de acuracidade
      5. Lista de ciclos
      6. Detalhe de cada ciclo
    """
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=1.5*cm, bottomMargin=1.5*cm,
        title=f"Relatório KPMG — {label_unidade}",
        author="Sistema de Gestão I9",
    )
    st    = _estilos()
    story = []
    W     = A4[0] - 4*cm  # largura útil

    # ── CAPA ─────────────────────────────────────────────────────────────────
    # Faixa de cabeçalho colorida
    story.append(Table(
        [[Paragraph("Gestão Integrada I9", st["titulo"]),
          Paragraph(f"Relatório de<br/>Inventário Cíclico", st["subtitulo"])]],
        colWidths=[W*0.6, W*0.4],
        style=TableStyle([
            ("BACKGROUND", (0,0), (-1,-1), AZUL),
            ("TOPPADDING",    (0,0), (-1,-1), 14),
            ("BOTTOMPADDING", (0,0), (-1,-1), 14),
            ("LEFTPADDING",   (0,0), (0,-1),  16),
            ("RIGHTPADDING",  (-1,0),(-1,-1), 16),
            ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
            ("ALIGN",         (1,0), (1,-1),  "RIGHT"),
        ])
    ))
    story.append(Spacer(1, 0.3*cm))

    # Linha de identificação
    periodo_ini = ciclos[0].get("data", "—") if ciclos else "—"
    periodo_fim = ciclos[-1].get("data", "—") if ciclos else "—"
    info_rows = [
        ["Unidade:",   label_unidade],
        ["Período:",   f"{periodo_ini} a {periodo_fim}"],
        ["Gerado em:", date.today().strftime("%d/%m/%Y")],
        ["Ciclos:",    str(len(ciclos))],
    ]
    story.append(Table(
        info_rows,
        colWidths=[3*cm, W-3*cm],
        style=TableStyle([
            ("FONTNAME",  (0,0), (0,-1), "Helvetica-Bold"),
            ("FONTNAME",  (1,0), (1,-1), "Helvetica"),
            ("FONTSIZE",  (0,0), (-1,-1), 9),
            ("TEXTCOLOR", (0,0), (0,-1), AZUL),
            ("TEXTCOLOR", (1,0), (1,-1), TEXTO),
            ("TOPPADDING",    (0,0), (-1,-1), 3),
            ("BOTTOMPADDING", (0,0), (-1,-1), 3),
            ("LINEBELOW", (0,-1), (-1,-1), 0.5, colors.HexColor("#cccccc")),
        ])
    ))
    story.append(Spacer(1, 0.5*cm))

    # ── RESUMO EXECUTIVO ─────────────────────────────────────────────────────
    story.append(Paragraph("Resumo Executivo", st["h1"]))
    story.append(HRFlowable(width=W, thickness=2, color=LARANJA, spaceAfter=8))

    total_contados  = len(contados_global)
    pct_cobertura   = (total_contados / total_skus * 100) if total_skus > 0 else 0
    acuracidades    = []
    for c in ciclos:
        ac = str(c.get("acuracidade","0")).replace(",",".").replace("%","").strip()
        try: acuracidades.append(float(ac))
        except: pass
    media_acuracidade = sum(acuracidades)/len(acuracidades) if acuracidades else 0

    kpis = [
        (f"{total_skus:,}",           "Total de SKUs"),
        (f"{total_contados:,}",        "SKUs Contados"),
        (f"{pct_cobertura:.1f}%",      "Cobertura KPMG"),
        (f"{media_acuracidade:.1f}%",  "Acuracidade Média"),
        (f"{len(ciclos)}",             "Ciclos Realizados"),
    ]
    kpi_data = [[Paragraph(v, st["kpi_val"]) for v, _ in kpis],
                [Paragraph(l, st["kpi_lbl"]) for _, l in kpis]]
    story.append(Table(
        kpi_data,
        colWidths=[W/5]*5,
        style=TableStyle([
            ("BACKGROUND",    (0,0), (-1,-1), CINZA),
            ("BOX",           (0,0), (-1,-1), 0.5, colors.HexColor("#dddddd")),
            ("LINEABOVE",     (0,0), (-1,0),  3, LARANJA),
            ("TOPPADDING",    (0,0), (-1,-1), 10),
            ("BOTTOMPADDING", (0,0), (-1,-1), 10),
            ("ALIGN",         (0,0), (-1,-1), "CENTER"),
        ])
    ))
    story.append(Spacer(1, 0.5*cm))

    # Texto resumo
    status_cob = "✓ CUMPRIDA" if pct_cobertura >= 100 else f"EM ANDAMENTO ({pct_cobertura:.1f}%)"
    story.append(Paragraph(
        f"A unidade <b>{label_unidade}</b> realizou <b>{len(ciclos)} ciclo(s)</b> de inventário "
        f"no período de <b>{periodo_ini}</b> a <b>{periodo_fim}</b>. "
        f"A cobertura acumulada atingiu <b>{pct_cobertura:.1f}%</b> dos {total_skus:,} SKUs cadastrados, "
        f"com acuracidade média de <b>{media_acuracidade:.1f}%</b>. "
        f"Exigência KPMG de cobertura anual: <b>{status_cob}</b>.",
        st["body"]
    ))

    # ── GRÁFICO COBERTURA ────────────────────────────────────────────────────
    story.append(PageBreak())
    story.append(Paragraph("Evolução da Cobertura", st["h1"]))
    story.append(HRFlowable(width=W, thickness=2, color=LARANJA, spaceAfter=8))
    story.append(Paragraph(
        "Percentual de SKUs contados por ciclo em relação ao total da unidade. "
        "A linha verde tracejada representa a meta de 100% exigida pela KPMG.",
        st["body"]
    ))
    buf_cob = _grafico_cobertura(ciclos)
    if buf_cob:
        story.append(RLImage(buf_cob, width=W, height=5*cm))
    story.append(Spacer(1, 0.5*cm))

    # ── GRÁFICO ACURACIDADE ──────────────────────────────────────────────────
    story.append(Paragraph("Acuracidade por Ciclo", st["h1"]))
    story.append(HRFlowable(width=W, thickness=2, color=LARANJA, spaceAfter=8))
    story.append(Paragraph(
        "Acuracidade registrada pelo WMS em cada ciclo de contagem. "
        "Verde: ≥ 95% · Laranja: 80–95% · Vermelho: < 80%.",
        st["body"]
    ))
    buf_ac = _grafico_acuracidade(ciclos)
    if buf_ac:
        story.append(RLImage(buf_ac, width=W, height=4.5*cm))

    # ── LISTA DE CICLOS ──────────────────────────────────────────────────────
    story.append(PageBreak())
    story.append(Paragraph("Lista de Ciclos Realizados", st["h1"]))
    story.append(HRFlowable(width=W, thickness=2, color=LARANJA, spaceAfter=8))

    header = ["#", "Nº Ciclo", "Data Contagem", "Responsável", "Nº Inv.", "SKUs", "Cobertura", "Acuracidade"]
    rows   = [header]
    for i, c in enumerate(ciclos, 1):
        rows.append([
            str(i),
            c.get("num_ciclo","—"),
            c.get("data","—"),
            c.get("responsavel","—"),
            c.get("num_inv","—"),
            str(len(c.get("produtos_contados",[]))),
            f"{c.get('cobertura_pct',0):.1f}%",
            c.get("acuracidade","—"),
        ])

    col_w = [0.6*cm, 4.5*cm, 2.5*cm, 3.5*cm, 1.5*cm, 1.2*cm, 2*cm, 2*cm]
    t_style = TableStyle([
        ("BACKGROUND",    (0,0), (-1,0),  AZUL),
        ("TEXTCOLOR",     (0,0), (-1,0),  BRANCO),
        ("FONTNAME",      (0,0), (-1,0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0,0), (-1,-1), 8),
        ("ALIGN",         (0,0), (-1,-1), "CENTER"),
        ("ALIGN",         (3,1), (3,-1),  "LEFT"),
        ("ROWBACKGROUNDS",(0,1), (-1,-1), [BRANCO, CINZA]),
        ("GRID",          (0,0), (-1,-1), 0.3, colors.HexColor("#cccccc")),
        ("TOPPADDING",    (0,0), (-1,-1), 5),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
        ("LINEBELOW",     (0,0), (-1,0),  1.5, LARANJA),
    ])
    story.append(Table(rows, colWidths=col_w, style=t_style, repeatRows=1))

    # ── DETALHE DE CADA CICLO ────────────────────────────────────────────────
    for i, c in enumerate(ciclos, 1):
        story.append(PageBreak())
        story.append(Paragraph(f"Ciclo {i} — {c.get('num_ciclo','—')}", st["h1"]))
        story.append(HRFlowable(width=W, thickness=2, color=LARANJA, spaceAfter=6))

        # Info do ciclo
        det = [
            ["Data da contagem:",  c.get("data","—"),
             "Nº Inventário:",     c.get("num_inv","—")],
            ["Responsável:",       c.get("responsavel","—"),
             "Acuracidade:",       c.get("acuracidade","—")],
            ["SKUs na lista:",     str(c.get("qtd_lista",0)),
             "SKUs contados:",     str(len(c.get("produtos_contados",[])))],
            ["Cobertura:",         f"{c.get('cobertura_pct',0):.1f}%",
             "Status:",            c.get("status","—")],
        ]
        story.append(Table(
            det, colWidths=[3*cm, 5*cm, 3*cm, W-11*cm],
            style=TableStyle([
                ("FONTNAME",  (0,0), (0,-1), "Helvetica-Bold"),
                ("FONTNAME",  (2,0), (2,-1), "Helvetica-Bold"),
                ("FONTSIZE",  (0,0), (-1,-1), 8),
                ("TEXTCOLOR", (0,0), (0,-1), AZUL),
                ("TEXTCOLOR", (2,0), (2,-1), AZUL),
                ("BACKGROUND",(0,0), (-1,-1), CINZA),
                ("GRID",      (0,0), (-1,-1), 0.3, colors.HexColor("#dddddd")),
                ("TOPPADDING",(0,0), (-1,-1), 4),
                ("BOTTOMPADDING",(0,0),(-1,-1),4),
            ])
        ))
        story.append(Spacer(1, 0.3*cm))

        # Produtos contados
        prods = c.get("produtos_contados", [])
        if prods:
            story.append(Paragraph(f"Produtos contados ({len(prods)})", st["h2"]))
            # Divide em 4 colunas
            cols_n = 4
            prod_rows = [["Código"]*cols_n]
            for j in range(0, len(prods), cols_n):
                row = prods[j:j+cols_n]
                while len(row) < cols_n:
                    row.append("")
                prod_rows.append(row)

            story.append(Table(
                prod_rows,
                colWidths=[W/cols_n]*cols_n,
                style=TableStyle([
                    ("BACKGROUND",    (0,0), (-1,0),  AZUL2),
                    ("TEXTCOLOR",     (0,0), (-1,0),  BRANCO),
                    ("FONTNAME",      (0,0), (-1,0),  "Helvetica-Bold"),
                    ("FONTSIZE",      (0,0), (-1,-1), 8),
                    ("ALIGN",         (0,0), (-1,-1), "CENTER"),
                    ("ROWBACKGROUNDS",(0,1), (-1,-1), [BRANCO, CINZA]),
                    ("GRID",          (0,0), (-1,-1), 0.3, colors.HexColor("#cccccc")),
                    ("TOPPADDING",    (0,0), (-1,-1), 3),
                    ("BOTTOMPADDING", (0,0), (-1,-1), 3),
                ]),
                repeatRows=1
            ))

    # ── RODAPÉ FINAL ─────────────────────────────────────────────────────────
    story.append(Spacer(1, 1*cm))
    story.append(HRFlowable(width=W, thickness=0.5, color=colors.HexColor("#cccccc")))
    story.append(Paragraph(
        f"Documento gerado pelo Sistema de Gestão Integrada I9 em {date.today().strftime('%d/%m/%Y')}. "
        f"Este relatório é destinado à auditoria KPMG e representa o inventário cíclico realizado "
        f"na unidade {label_unidade}.",
        st["footer"]
    ))

    doc.build(story)
    buf.seek(0)
    return buf.getvalue()

# ── Render ────────────────────────────────────────────────────────────────────

def render(df_jlle, df_outras, formatar_br):
    st.markdown("## 🔄 Inventário Cíclico")
    st.caption("Geração de listas com **regra KPMG**: todos os SKUs contados ao menos uma vez por ano.")

    if df_jlle is None or df_jlle.empty:
        st.warning("Nenhum dado de Joinville encontrado.")
        return

    # ── Seleciona unidade ─────────────────────────────────────────────────
    col_e, col_f = st.columns(2)
    with col_e:
        empresa_sel = st.selectbox("🏢 Empresa",
            sorted(df_jlle["Empresa"].dropna().unique()), key="ic_emp")
    with col_f:
        filial_sel = st.selectbox("📍 Filial",
            sorted(df_jlle[df_jlle["Empresa"]==empresa_sel]["Filial"].dropna().unique()), key="ic_fil")

    label = f"{empresa_sel} — {filial_sel}"
    df_filial = df_jlle[(df_jlle["Empresa"]==empresa_sel)&(df_jlle["Filial"]==filial_sel)].copy()
    if df_filial.empty:
        st.warning(f"Sem dados para **{label}**."); return

    # Mensagem de sucesso após fechar
    if st.session_state.pop("ic_fechado_msg", False):
        st.success("✅ Inventário fechado e registrado no histórico KPMG!")

    # Engine do banco
    engine_db = st.session_state.get("_engine")

    # Carrega dados do banco
    contados    = db_obter_contados(engine_db, empresa_sel, filial_sel)
    ciclos      = db_obter_ciclos(engine_db, empresa_sel, filial_sel)
    ciclo_ativo = db_obter_ciclo_ativo(engine_db, empresa_sel, filial_sel)

    df_score   = calcular_score(df_filial, contados)
    total_skus = len(df_score)
    total_cont = sum(1 for p in df_score["Produto"].astype(str) if p in contados)
    pct_cob    = (total_cont/total_skus*100) if total_skus>0 else 0
    cor_barra  = "#27AE60" if pct_cob>=100 else "#EC6E21"

    # ── Métricas ──────────────────────────────────────────────────────────
    st.markdown("---")
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Total SKUs",        f"{total_skus:,}")
    c2.metric("Divergentes",       f"{int((df_score['Divergência']!=0).sum()):,}")
    c3.metric("Curva A",           f"{int((df_score['Curva ABC']=='A').sum()):,}")
    c4.metric("Valor Total",       f"R$ {formatar_br(df_score['Vl Total ERP'].sum())}")

    # Barra KPMG
    st.markdown(
        f"""<div style="background:#004550;border-radius:8px;padding:12px 16px;margin:8px 0;">
          <div style="display:flex;justify-content:space-between;margin-bottom:6px;">
            <span style="color:#fff;font-size:0.9rem;">✅ <b>{total_cont}</b> contados &nbsp;|&nbsp; ⬜ <b>{total_skus-total_cont}</b> pendentes</span>
            <span style="color:{cor_barra};font-weight:bold;">{pct_cob:.1f}%</span>
          </div>
          <div style="background:#003040;border-radius:4px;height:10px;">
            <div style="background:{cor_barra};width:{min(pct_cob,100):.1f}%;height:10px;border-radius:4px;"></div>
          </div></div>""",
        unsafe_allow_html=True)
    if pct_cob>=100:
        st.success("🎉 Todos os SKUs contados! Exigência KPMG cumprida.")

    # ── Ciclo ativo ───────────────────────────────────────────────────────
    st.markdown("---")

    # ── Gerar lista do ciclo ─────────────────────────────────────────────
    st.markdown("### Gerar lista do ciclo")

    # Aviso de atualização dos dados
    data_aud = st.session_state.get("_data_auditoria", None)
    col_av1, col_av2 = st.columns([3, 2])
    with col_av1:
        if data_aud:
            st.caption(f"📅 Dados de auditoria carregados em: **{data_aud}** — itens novos adicionados após essa data só aparecem após recarregar.")
        else:
            st.caption("📅 Dados carregados nesta sessão. Recarregue a página para garantir que itens novos estejam incluídos.")
    with col_av2:
        st.caption(f"⚠️ Itens divergentes reaparecem na lista mesmo após contados.")
    modo = st.radio("Modo", ["Quantidade fixa","Percentual"], horizontal=True, key="ic_modo")

    if modo == "Quantidade fixa":
        st.caption(
            "📌 Conta um número fixo de produtos por ciclo. "
            "Ideal quando sua equipe tem capacidade definida por contagem (ex: 50 itens por semana)."
        )
    else:
        st.caption(
            "📊 Conta uma fração do estoque por ciclo — o sistema calcula quantos itens isso representa. "
            "Ajuste a faixa conforme a capacidade da equipe: "
            "**5%** = 20 ciclos/ano · **10%** = 10 ciclos/ano · **20%** = 5 ciclos/ano · **30%** = ~4 ciclos/ano."
        )

    if modo == "Quantidade fixa":
        cols = st.columns(4)
        if "ic_qtd" not in st.session_state: st.session_state.ic_qtd = 2
        for col, qtd in zip(cols, [2,30,50,80]):
            with col:
                if st.button(f"{qtd}", key=f"ic_q{qtd}",
                             type="primary" if st.session_state.ic_qtd==qtd else "secondary"):
                    st.session_state.ic_qtd = qtd
        qtd_ciclo = min(st.session_state.ic_qtd, total_skus)
    else:
        pmap = {"5%":0.05,"10%":0.10,"20%":0.20,"30%":0.30}
        pl = st.select_slider("Faixa", list(pmap.keys()), value="10%", key="ic_pct")
        qtd_ciclo = max(1, int(total_skus * pmap[pl]))
        st.caption(f"→ {qtd_ciclo} itens de {total_skus}")

    df_lista = montar_lista(df_score, qtd_ciclo, contados)
    qp = int((df_lista["Origem"]=="🔴 Alta prioridade").sum())
    qk = int((df_lista["Origem"]=="⬜ Cobertura KPMG").sum())

    st.markdown(
        f"**{len(df_lista)} itens** — "
        f"<span style='color:#EC6E21'>🔴 {qp} prioridade</span> · "
        f"<span style='color:#27AE60'>⬜ {qk} KPMG</span>",
        unsafe_allow_html=True)

    cols_exib = [c for c in ["Produto","Descrição","Empresa","Filial","Curva ABC","Score",
                              "Já Contado","Dias s/ Contagem","Saldo ERP (Total)","Saldo WMS",
                              "Divergência","Vl Total ERP","Motivo","Origem"] if c in df_lista.columns]
    df_exibir = df_lista[cols_exib]

    st.dataframe(
        df_exibir.style
        .apply(lambda r: ["background-color:#005562;color:#fff;font-size:0.84rem;"]*len(r), axis=1)
        .set_table_styles([
            {"selector":"thead th","props":[("background-color","#004550"),("color","#fff"),("border-bottom","2px solid #EC6E21")]},
            {"selector":"td","props":[("padding","8px 12px"),("border-bottom","1px solid rgba(255,255,255,0.05)")]}])
        .format({"Saldo ERP (Total)":"{:,.2f}","Saldo WMS":"{:,.2f}","Divergência":"{:,.2f}",
                 "Vl Total ERP":"R$ {:,.2f}","Score":"{:.2f}","Dias s/ Contagem":"{:.0f}d"}, na_rep="-"),
        use_container_width=True, hide_index=False)

    # Número do ciclo gerado automaticamente
    num_ciclo = f"{date.today().strftime('%Y%m%d')}-{empresa_sel}-{filial_sel}".replace(" ","")

    col_dl, col_info = st.columns([2,2])
    with col_dl:
        st.download_button(
            "📥 Baixar Excel para Contagem",
            data=gerar_xlsx_lista(df_exibir, label),
            file_name=f"inv_{num_ciclo}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    with col_info:
        st.info(f"Nº do ciclo: **{num_ciclo}**")

    # Salva ciclo ativo no banco
    db_salvar_ciclo_ativo(engine_db, empresa_sel, filial_sel, {
        "num_ciclo":      num_ciclo,
        "data_geracao":   date.today().strftime("%d/%m/%Y"),
        "label":          label,
        "qtd_lista":      len(df_exibir),
        "produtos_lista": df_exibir["Produto"].astype(str).tolist(),
        "uploads":        [],
        "status":         "Em andamento",
    })

    # Upload do resultado WMS direto aqui
    st.markdown("---")
    # Recarrega ciclo ativo e progresso
    ciclo_ativo = db_obter_ciclo_ativo(engine_db, empresa_sel, filial_sel)
    uploads_anteriores = ciclo_ativo.get("uploads", []) if ciclo_ativo else []
    ja_contados_ciclo  = produtos_contados_no_ciclo(engine_db, empresa_sel, filial_sel)
    lista_atual        = ciclo_ativo or {}
    produtos_lista     = set(lista_atual.get("produtos_lista", []))
    # Normaliza zeros à esquerda para garantir comparação correta
    produtos_lista    = {str(p).strip().zfill(6) for p in produtos_lista}
    ja_contados_ciclo = {str(p).strip().zfill(6) for p in ja_contados_ciclo}
    faltam            = produtos_lista - ja_contados_ciclo
    pct_ciclo         = len(ja_contados_ciclo & produtos_lista) / len(produtos_lista) * 100 if produtos_lista else 0

    # DEBUG temporário
    with st.expander("🔍 Debug — comparação de produtos"):
        st.write("**Produtos na lista:**", sorted(produtos_lista))
        st.write("**Produtos contados no ciclo:**", sorted(ja_contados_ciclo))
        st.write("**Faltam:**", sorted(faltam))
        st.write("**% cobertura:**", pct_ciclo)
        st.write("**ciclo_ativo:**", ciclo_ativo)
        st.write("**uploads_anteriores:**", uploads_anteriores)
    cor_prog           = "#27AE60" if pct_ciclo >= 100 else "#EC6E21"

    st.markdown("---")
    st.markdown("### Upload do resultado WMS")

    # Card de progresso do ciclo
    st.markdown(
        f"""<div style="background:#004550;border-radius:8px;padding:12px 16px;margin-bottom:12px;">
          <div style="display:flex;justify-content:space-between;margin-bottom:6px;">
            <span style="color:#fff;font-size:0.9rem;">
              ✅ <b>{len(ja_contados_ciclo & produtos_lista)}</b> contados &nbsp;|&nbsp;
              ⬜ <b>{len(faltam)}</b> pendentes &nbsp;|&nbsp;
              📤 <b>{len(uploads_anteriores)}</b> upload(s)
            </span>
            <span style="color:{cor_prog};font-weight:bold;">{pct_ciclo:.1f}%</span>
          </div>
          <div style="background:#003040;border-radius:4px;height:10px;">
            <div style="background:{cor_prog};width:{min(pct_ciclo,100):.1f}%;height:10px;border-radius:4px;"></div>
          </div></div>""",
        unsafe_allow_html=True)

    # Histórico de uploads do ciclo
    if uploads_anteriores:
        with st.expander(f"📤 Uploads realizados neste ciclo ({len(uploads_anteriores)})"):
            df_ups = pd.DataFrame([{
                "Etapa":        i+1,
                "Nº Inventário":u.get("num_inv","—"),
                "Data":         u.get("data","—"),
                "Responsável":  u.get("responsavel","—"),
                "Acuracidade":  u.get("acuracidade","—"),
                "Produtos":     len(u.get("produtos",[])),
            } for i,u in enumerate(uploads_anteriores)])
            st.dataframe(df_ups, use_container_width=True, hide_index=True)

    st.markdown(f"#### Etapa {len(uploads_anteriores)+1} — Adicionar resultado WMS")
    st.caption("Faça o upload do Excel gerado pelo WMS para esta etapa.")

    arquivo = st.file_uploader(
        "Selecione o arquivo Excel do WMS",
        type=["xlsx"],
        key=f"ic_upload_{len(uploads_anteriores)}"
    )

    if arquivo:
        try:
            res    = processar_resultado_wms(arquivo)
            df_wms = res["df"]

            c1,c2,c3,c4 = st.columns(4)
            for col, label, val in [
                (c1, "Nº Inventário", res.get("num_inv","—")),
                (c2, "Data",          res.get("data","—")),
                (c3, "Responsável",   res.get("responsavel","—")),
                (c4, "Acuracidade",   res.get("acuracidade","—")),
            ]:
                col.markdown(
                    f"""<div style="border:2px solid #EC6E21;border-radius:10px;padding:15px;background:#004550;">
                      <div style="color:#aaa;font-size:0.8rem;margin-bottom:4px;">{label}</div>
                      <div style="color:#fff;font-size:1.1rem;font-weight:700;word-break:break-word;">{val}</div>
                    </div>""",
                    unsafe_allow_html=True
                )

            produtos_wms      = set(res["produtos"])
            novos_desta_etapa = (produtos_lista & produtos_wms) - ja_contados_ciclo
            ja_tinham         = produtos_lista & produtos_wms & ja_contados_ciclo
            fora_da_lista     = produtos_wms - produtos_lista

            st.markdown("#### Resultado desta etapa")
            col_a, col_b, col_c = st.columns(3)
            col_a.metric("✅ Novos contados",    len(novos_desta_etapa))
            col_b.metric("🔁 Já contados antes", len(ja_tinham))
            col_c.metric("➕ Fora da lista",      len(fora_da_lista))

            df_wms["Status"] = df_wms["Codigo"].apply(
                lambda p: "✅ Novo" if p in novos_desta_etapa else
                          "🔁 Já contado" if p in ja_tinham else
                          "➕ Fora da lista" if p in fora_da_lista else "—"
            )
            st.dataframe(
                df_wms[["Codigo","Descricao","Qtd Antes","Qtd Depois","Qtd Diferença","Acuracidade","Status"]],
                use_container_width=True, hide_index=True)

            if st.button("📥 Adicionar esta etapa ao ciclo", key="ic_add_etapa", type="primary"):
                try:
                    upload_data = {
                        "num_inv":     res.get("num_inv","—"),
                        "data":        res.get("data","—"),
                        "data_iso":    res.get("data_iso", date.today().isoformat()),
                        "responsavel": res.get("responsavel","—"),
                        "acuracidade": res.get("acuracidade","—"),
                        "produtos":    list(produtos_lista & produtos_wms),
                    }
                    st.write("DEBUG — produtos a gravar:", upload_data["produtos"])
                    # Testa UPDATE direto
                    import json as _json
                    from sqlalchemy import text as _text
                    try:
                        with engine_db.connect() as _conn:
                            _result = _conn.execute(_text("""
                                UPDATE inventario_ciclo_ativo
                                SET uploads_json = :v
                                WHERE empresa = :e AND filial = :f
                            """), {"v": _json.dumps([upload_data]), "e": empresa_sel, "f": filial_sel})
                            _conn.commit()
                            st.write(f"DEBUG — rows afetadas: {_result.rowcount}")
                    except Exception as _ue:
                        st.error(f"DEBUG — UPDATE direto falhou: {_ue}")
                    # Verifica se gravou
                    ciclo_check = db_obter_ciclo_ativo(engine_db, empresa_sel, filial_sel)
                    st.write("DEBUG — uploads após UPDATE:", ciclo_check.get("uploads") if ciclo_check else "None")
                    st.success(f"Etapa adicionada! {len(novos_desta_etapa)} novos produtos contados.")
                    # st.rerun()  # TEMPORARIAMENTE DESABILITADO PARA DEBUG
                except Exception as _err:
                    st.error(f"ERRO ao adicionar etapa: {_err}")

        except Exception as e:
            st.error(f"Erro ao processar arquivo: {e}")

    # Botão fechar — só libera com 100%
    st.markdown("---")
    if pct_ciclo >= 100:
        if st.button("🏁 Fechar inventário", key="ic_fechar", type="primary"):
            fechar_ciclo(engine_db, empresa_sel, filial_sel)
            st.session_state["ic_fechado_msg"] = True
            st.rerun()


    else:
        col_btn, col_msg = st.columns([1,3])
        with col_btn:
            st.button("🏁 Fechar inventário", key="ic_fechar_bloq", disabled=True)
        with col_msg:
            produtos_faltam = sorted(list(faltam))[:10]
            mais = len(faltam)-10 if len(faltam)>10 else 0
            lista_str = ", ".join(f"`{p}`" for p in produtos_faltam)
            if mais > 0: lista_str += f" e mais {mais}"
            st.warning(f"⚠️ Faltam **{len(faltam)}** produtos: {lista_str}")
    # ── Histórico de ciclos ───────────────────────────────────────────────
    if ciclos:
        st.markdown("---")
        with st.expander(f"📋 Histórico KPMG — {len(ciclos)} ciclo(s) realizados"):
            df_hist = pd.DataFrame([{
                "Nº Ciclo":      c.get("num_ciclo","—"),
                "Data Geração":  c.get("data_geracao","—"),
                "Data Contagem": c.get("data","—"),
                "Responsável":   c.get("responsavel","—"),
                "Acuracidade":   c.get("acuracidade","—"),
                "SKUs Lista":    c.get("qtd_lista",0),
                "SKUs Contados": len(c.get("produtos_contados",[])),
                "Cobertura %":   f"{c.get('cobertura_pct',0):.1f}%",
                "Status":        c.get("status","—"),
            } for c in ciclos])
            st.dataframe(df_hist, use_container_width=True, hide_index=True)

            col_dl, col_pdf, col_reset = st.columns([2, 2, 1])
            with col_dl:
                st.download_button(
                    "📥 Exportar Histórico (Excel)",
                    data=gerar_xlsx_historico(ciclos, label),
                    file_name=f"historico_kpmg_{_slug(empresa_sel,filial_sel)}_{date.today().strftime('%d%m%Y')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            with col_pdf:
                if PDF_DISPONIVEL:
                    pdf_bytes = gerar_relatorio_kpmg_pdf(
                        ciclos, label, total_skus, contados
                    )
                    st.download_button(
                        "📄 Relatório KPMG (PDF)",
                        data=pdf_bytes,
                        file_name=f"relatorio_kpmg_{_slug(empresa_sel,filial_sel)}_{date.today().strftime('%d%m%Y')}.pdf",
                        mime="application/pdf",
                        type="primary",
                    )
                else:
                    st.caption("PDF indisponível — instale reportlab e matplotlib")
            with col_reset:
                if st.button("🔄 Novo período", key="ic_reset"):
                    db_resetar_tudo(engine_db, empresa_sel, filial_sel)
                    st.success("Novo período iniciado!")
                    st.rerun()
