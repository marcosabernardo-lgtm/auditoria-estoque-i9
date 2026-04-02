import streamlit as st
from collections import defaultdict
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
        db_obter_justificativas, db_salvar_justificativas,
        db_obter_erp_upload, db_obter_erp_uploads, db_salvar_erp_upload,
        db_obter_nf_ajustes, db_salvar_nf_ajuste,
        db_obter_documentos_conferidos, db_carregar_tudo,
    )
    DB_DISPONIVEL = True
except ImportError:
    DB_DISPONIVEL = False


def parsear_nf_danfe(arquivo_bytes):
    """Extrai dados da NF-e DANFE (formato Alltech/TOTVS Protheus) via pdfplumber."""
    import re, io as _io
    result = {"num_nf":"","data":"","natureza":"","itens":[]}
    try:
        import pdfplumber
        with pdfplumber.open(_io.BytesIO(arquivo_bytes)) as pdf:
            text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception as e:
        return result, str(e)

    # Nº da NF
    nums = re.findall(r'N\.\s*0*(\d+)', text)
    if nums: result["num_nf"] = nums[0].zfill(9)

    # Data de emissão
    m = re.search(r'DATA DE EMISS[ÃA]O\s*\n?\s*(\d{2}/\d{2}/\d{4})', text)
    if m:
        result["data"] = m.group(1)
    else:
        m = re.search(r'(\d{2}/\d{2}/\d{4})\s+\d{2}:\d{2}:\d{2}', text)
        if m: result["data"] = m.group(1)

    # Natureza da operação
    m = re.search(r'NATUREZA DA OPERA[ÇC][ÃA]O\s*\n\s*(.+?)(?:\s+PROTOCOLO|\n)', text)
    if m:
        result["natureza"] = m.group(1).strip()
    else:
        m = re.search(r'(BAIXA [A-Z]+|VENDA|TRANSFERENCIA|AJUSTE DE INVENTARIO)', text)
        if m: result["natureza"] = m.group(1)

    # Itens — padrão: CODPROD DESCRICAO NCM CST CFOP UN QUANT V.UNIT V.TOTAL
    itens = []
    padrao = re.compile(
        r'(\d{6})\s+(.+?)\s+\d{8}\s+\d{3}\s+\d{4}\s+\w+\s+([\d,]+)\s+([\d,]+(?:\d{3})?)\s+([\d,]+)',
        re.MULTILINE)
    for m in padrao.finditer(text):
        itens.append({
            "Codigo":   m.group(1),
            "Descricao":m.group(2).strip(),
            "Qtd":      float(m.group(3).replace(",",".")),
            "Vl Unit":  float(m.group(4).replace(",",".")),
            "Vl Total": float(m.group(5).replace(",",".")),
        })
    result["itens"] = itens
    return result, None


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
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib import colors
        from reportlab.lib.units import cm
        from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                        Paragraph, Spacer, HRFlowable, PageBreak)
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
    except ImportError:
        return None

    buf    = io.BytesIO()
    doc    = SimpleDocTemplate(buf, pagesize=landscape(A4),
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

    s_capa_title = sty("ct", fontSize=22, textColor=C_TEAL,   fontName="Helvetica-Bold", spaceAfter=16, spaceBefore=8)
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
    # Evita duplicar prefixo: "Service — Service - Matriz" → "Service — Matriz"
    _fil_display = filial.split(" - ")[-1] if " - " in filial else filial
    label_unidade = f"{empresa} — {_fil_display}"

    # Datas do período
    datas = [c.get("data","") for c in ciclos_sel if c.get("data","") not in ("","—")]
    data_ini = min(datas) if datas else "—"
    data_fim = max(datas) if datas else "—"

    # KPIs consolidados
    total_skus_cont = sum(
        len(dfs_rel.get(c.get("num_ciclo",""), pd.DataFrame()))
        for c in ciclos_sel
    )
    # Calcula acuracidade por ciclo: (sem divergência / total) * 100
    def _calc_acur(df_c):
        if df_c.empty: return None
        # Tenta a partir da coluna Diferença Invent ou Divergencia Qtd
        for col_div in ["Diferença Invent","Divergencia Qtd","Divergência"]:
            if col_div in df_c.columns:
                total = len(df_c)
                sem_div = int((pd.to_numeric(df_c[col_div], errors="coerce").fillna(0) == 0).sum())
                if total > 0: return sem_div / total * 100
        return None

    acur_por_ciclo = {}
    for c in ciclos_sel:
        num_c = c.get("num_ciclo","")
        df_c  = dfs_rel.get(num_c, pd.DataFrame())
        acur  = _calc_acur(df_c)
        if acur is None:
            # fallback: campo gravado
            try: acur = float(str(c.get("acuracidade","")).replace("%","").replace(",",".")) or None
            except: acur = None
        acur_por_ciclo[num_c] = acur

    vals_validos = [v for v in acur_por_ciclo.values() if v is not None]
    acur_media = f"{sum(vals_validos)/len(vals_validos):.1f}%" if vals_validos else "N/D"
    cobertura_max = max((c.get("cobertura_pct",0) for c in ciclos_sel), default=0)
    n_ciclos = len(ciclos_sel)

    # ── PÁGINA 1: CAPA ────────────────────────────────────────────────────
    elems.append(Spacer(1, 1*cm))
    elems.append(Paragraph("Gestão Integrada I9", sty("gi", fontSize=11, textColor=C_ORANGE, fontName="Helvetica-Bold")))
    elems.append(Spacer(1, 0.3*cm))
    elems.append(Paragraph("Relatório de Inventário Cíclico", s_capa_title))
    elems.append(Spacer(1, 0.1*cm))
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
                ["#","Nº Ciclo","Data Contagem","Responsável","Nº Inv.","SKUs","Div.","Cobertura","Acurácia"]]
    rows_ciclos = [h_ciclos]
    for i, c in enumerate(ciclos_sel, 1):
        num_c = c.get("num_ciclo","")
        df_c  = dfs_rel.get(num_c, pd.DataFrame())

        # Pega uploads ERP do ciclo via erp_json
        _erp_j = c.get("erp_json","[]")
        try:
            _erp_data = json.loads(_erp_j) if _erp_j and _erp_j != "[]" else []
        except:
            _erp_data = []

        # Agrupa por documento
        _docs_map = defaultdict(list)
        for r in _erp_data:
            _doc_num = str(r.get("Documento","—")).strip()
            _docs_map[_doc_num].append(r)

        if not _docs_map:
            # Fallback: uma linha só com totais do ciclo
            n_sku = len(df_c) if not df_c.empty else c.get("qtd_contados", len(c.get("produtos_contados",[])))
            acur_c = acur_por_ciclo.get(num_c)
            acur_str = f"{acur_c:.1f}%" if acur_c is not None else "—"
            rows_ciclos.append([
                Paragraph(str(i),         s_cell_c),
                Paragraph(num_c,          s_cell),
                Paragraph(c.get("data","—"), s_cell_c),
                Paragraph(c.get("responsavel","—"), s_cell),
                Paragraph("—",            s_cell_c),
                Paragraph(str(n_sku),     s_cell_c),
                Paragraph("—",            s_cell_c),
                Paragraph(f"{c.get('cobertura_pct',0):.1f}%", s_cell_c),
                Paragraph(acur_str,       s_cell_c),
            ])
        else:
            # Uma linha por documento/upload
            docs_list = list(_docs_map.keys())
            n_docs = len(docs_list)
            for j, _doc_num in enumerate(docs_list):
                itens_doc = _docs_map[_doc_num]
                n_sku_doc = len(set(str(r.get("Codigo","")).zfill(6) for r in itens_doc))
                # SKUs divergentes neste documento
                n_div_doc = sum(1 for r in itens_doc
                                if float(str(r.get("Divergencia Qtd",0)).replace(",",".") or 0) != 0)
                # Acuracidade deste documento
                acur_doc = f"{(n_sku_doc-n_div_doc)/n_sku_doc*100:.1f}%" if n_sku_doc > 0 else "—"
                # Cobertura acumulada até este doc
                cobertura_doc = f"{c.get('cobertura_pct',0):.1f}%" if j == n_docs-1 else "—"

                rows_ciclos.append([
                    Paragraph(str(i) if j == 0 else "", s_cell_c),
                    Paragraph(num_c  if j == 0 else "", s_cell),
                    Paragraph(c.get("data","—"),        s_cell_c),
                    Paragraph(c.get("responsavel","—") if j == 0 else "", s_cell),
                    Paragraph(_doc_num,                 s_cell_c),
                    Paragraph(str(n_sku_doc),           s_cell_c),
                    Paragraph(str(n_div_doc),           s_cell_c),
                    Paragraph(cobertura_doc,            s_cell_c),
                    Paragraph(acur_doc,                 s_cell_c),
                ])

    tbl_ciclos = Table(rows_ciclos,
                       colWidths=[0.7*cm, 5.5*cm, 2.2*cm, 3.5*cm, 2.0*cm, 1.3*cm, 1.3*cm, 1.8*cm, 2.0*cm],
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

        # Remove duplicação no num_ciclo ex: "20260331-Service-Service-Matriz" → "20260331-Service-Matriz"
        _num_c_display = num_c
        parts = num_c.split("-")
        if len(parts) > 2:
            seen = []
            for p in parts:
                if p not in seen:
                    seen.append(p)
            _num_c_display = "-".join(seen)
        elems.append(Paragraph(f"Ciclo {idx} — {_num_c_display}", s_sec))
        elems.append(HRFlowable(width="100%", thickness=1, color=C_ORANGE))
        elems.append(Spacer(1, 0.3*cm))

        # Calcula SKUs divergentes e acuracidade para este ciclo
        n_div_c = 0
        for col_div in ["Diferença Invent","Divergencia Qtd","Divergência"]:
            if not df_rel.empty and col_div in df_rel.columns:
                n_div_c = int((pd.to_numeric(df_rel[col_div], errors="coerce").fillna(0) != 0).sum())
                break
        acur_c_val = acur_por_ciclo.get(num_c)
        acur_c_str = f"{acur_c_val:.1f}%" if acur_c_val is not None else "—"

        # Nº Inventários (múltiplos)
        _erp_j2 = c.get("erp_json","[]")
        try:
            _erp_d2 = json.loads(_erp_j2) if _erp_j2 and _erp_j2 != "[]" else []
            _docs2 = list(dict.fromkeys([str(r.get("Documento","")).strip() for r in _erp_d2 if r.get("Documento","")]))
            num_inv_det = ", ".join(_docs2) if _docs2 else c.get("num_inv","—")
        except:
            num_inv_det = c.get("num_inv","—")

        # Metadados do ciclo em grid 3x2
        meta_data = [
            [Paragraph("Data da contagem", s_det_label), Paragraph(c.get("data","—"), s_det_val),
             Paragraph("Nº Inventário",    s_det_label), Paragraph(num_inv_det, s_det_val),
             Paragraph("Status",           s_det_label), Paragraph(c.get("status","—"), s_det_val)],
            [Paragraph("Responsável",      s_det_label), Paragraph(c.get("responsavel","—"), s_det_val),
             Paragraph("Acuracidade",      s_det_label), Paragraph(acur_c_str, s_det_val),
             Paragraph("SKUs contados",    s_det_label), Paragraph(str(n_sku), s_det_val)],
            [Paragraph("SKUs na lista",    s_det_label), Paragraph(str(c.get("qtd_lista","—")), s_det_val),
             Paragraph("SKUs divergentes", s_det_label), Paragraph(str(n_div_c), s_det_val),
             Paragraph("Cobertura",        s_det_label), Paragraph(f"{c.get('cobertura_pct',0):.1f}%", s_det_val)],
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
            headers  = ["Código","Descrição","Saldo ERP","Saldo WMS","Inventariado","Diferença","Vl Total ERP","Vl Total Dif.","Justificativa","NF Ajuste"]
            col_keys = ["Produto","Descrição","Saldo ERP (Total)","Saldo WMS","Invent WMS","Diferença Invent","Vl Total ERP","Vl Total Diferença","Justificativa","NF Ajuste"]
            col_w    = [1.6*cm, 5.5*cm, 2.0*cm, 2.0*cm, 2.2*cm, 2.0*cm, 3.2*cm, 3.2*cm, 3.5*cm, 2.2*cm]
            # Carrega justificativas e NFs para este ciclo
            _justs_pdf = c.get("_justs_pdf", {})
            _nfs_pdf   = c.get("_nfs_pdf", {})

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
                        # Garante zeros à esquerda para código do produto
                        if k == "Produto":
                            r.append(Paragraph(str(v).zfill(6), s_cell))
                        elif k == "Justificativa":
                            cod = str(row.get("Produto","")).zfill(6)
                            just = _justs_pdf.get(cod, "—")
                            r.append(Paragraph(str(just)[:40], s_cell))
                        elif k == "NF Ajuste":
                            cod = str(row.get("Produto","")).zfill(6)
                            nf  = _nfs_pdf.get(cod, "—")
                            r.append(Paragraph(str(nf), s_cell_c))
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

    # Empresa/Filial: vêm da tela de seleção do app.py (_app_empresa/_app_filial)
    # ic_empresa_sel/ic_filial_sel são mantidos como alias para compatibilidade interna
    _app_emp = st.session_state.get("_app_empresa")
    _app_fil = st.session_state.get("_app_filial")
    if _app_emp and _app_fil:
        st.session_state.setdefault("ic_empresa_sel", _app_emp)
        st.session_state.setdefault("ic_filial_sel",  _app_fil)
        # Se app mudou empresa/filial, sincroniza
        if st.session_state.get("ic_empresa_sel") != _app_emp or \
           st.session_state.get("ic_filial_sel")  != _app_fil:
            st.session_state["ic_empresa_sel"] = _app_emp
            st.session_state["ic_filial_sel"]  = _app_fil

    empresa_sel = st.session_state.get("ic_empresa_sel")
    filial_sel  = st.session_state.get("ic_filial_sel")

    engine_db = st.session_state.get("_engine")

    # Cache no session_state — só recarrega do banco quando empresa/filial muda ou ic_force_reload
    if empresa_sel and filial_sel:
        _cache_key = f"ic_cache_{empresa_sel}_{filial_sel}"
        _force = st.session_state.pop("ic_force_reload", False)
        _deve_recarregar = (_cache_key not in st.session_state) or _force
        if _deve_recarregar:
            _tudo = db_carregar_tudo(engine_db, empresa_sel, filial_sel)
            st.session_state[f"{_cache_key}_contados"]    = _tudo["contados"]
            st.session_state[f"{_cache_key}_ciclos"]      = _tudo["ciclos"]
            st.session_state[f"{_cache_key}_ciclo_ativo"] = _tudo["ciclo_ativo"]
            st.session_state[f"{_cache_key}_erp_uploads"] = _tudo["erp_uploads"]
            st.session_state[f"{_cache_key}_nf_ajustes"]  = _tudo["nf_ajustes"]
            st.session_state[f"{_cache_key}_docs_conf"]   = _tudo["docs_conf"]
            st.session_state[f"{_cache_key}_justs"]       = _tudo["justs"]
            st.session_state[_cache_key] = True
        contados    = st.session_state.get(f"{_cache_key}_contados", {})
        ciclos      = st.session_state.get(f"{_cache_key}_ciclos", [])
        ciclo_ativo = st.session_state.get(f"{_cache_key}_ciclo_ativo")
        label       = f"{empresa_sel} — {filial_sel}"
        # filial_sel pode ser "Service - Matriz" (nome completo do banco)
        # df_jlle já tem Filial como sufixo ("Matriz") e Empresa como prefixo ("Service")
        _fil_sufixo = filial_sel.split(" - ")[-1] if " - " in filial_sel else filial_sel
        _emp_prefixo = empresa_sel.split(" - ")[0] if " - " in empresa_sel else empresa_sel
        df_filial = df_jlle[
            (df_jlle["Filial"] == _fil_sufixo) |
            (df_jlle["Empresa"].str.contains(_emp_prefixo, case=False, na=False) &
             (df_jlle["Filial"] == _fil_sufixo))
        ].copy()
        # Fallback: usa df_jlle inteiro se ainda vazio
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
    if not isinstance(_uploads, list):
        _uploads = []  # ciclos antigos gravaram uploads como int — normaliza para lista vazia
    ja_cont_ciclo = set()
    for u in _uploads:
        ja_cont_ciclo.update(str(p).strip().zfill(6) for p in u.get("produtos", []))
    pl_ciclo  = {str(p).strip().zfill(6) for p in (ciclo_ativo.get("produtos_lista",[]) if ciclo_ativo else [])}
    faltam    = pl_ciclo - ja_cont_ciclo
    pct_ciclo = len(ja_cont_ciclo & pl_ciclo) / len(pl_ciclo) * 100 if pl_ciclo else 0

    # Estado do ERP upload para o ciclo ativo
    _num_ciclo_ativo = ciclo_ativo.get("num_ciclo","") if ciclo_ativo else ""
    erp_uploads_ativo = st.session_state.get(f"{_cache_key}_erp_uploads", []) if empresa_sel and filial_sel else []
    erp_upload = erp_uploads_ativo[0] if erp_uploads_ativo else None

    # NF de ajuste para o ciclo ativo
    nf_ajustes_ativo = st.session_state.get(f"{_cache_key}_nf_ajustes", []) if empresa_sel and filial_sel else []

    # Conferência concluída: todos os uploads ERP foram conferidos
    _conf_concluida = False
    _nf_concluida   = False
    if _num_ciclo_ativo and empresa_sel and filial_sel and erp_uploads_ativo:
        try:
            _docs_conf = st.session_state.get(f"{_cache_key}_docs_conf", set())
            _docs_erp  = {u.get("documento","") for u in erp_uploads_ativo}
            # Conferência concluída APENAS se todos os docs ERP foram conferidos
            _conf_concluida = bool(_docs_erp and _docs_erp.issubset(_docs_conf))

            # NF concluída se conferência ok E não há "Ajuste de inventário" pendente ou há NF salva
            if _conf_concluida:
                _justs = st.session_state.get(f"{_cache_key}_justs", {})
                _n_ajuste = sum(1 for p,j in _justs.items()
                               if j == "Ajuste de inventário" and not p.startswith("_"))
                _nf_concluida = (_n_ajuste == 0) or (len(nf_ajustes_ativo) > 0)
            else:
                _nf_concluida = False
        except:
            pass
    elif _num_ciclo_ativo and empresa_sel and filial_sel and not erp_uploads_ativo:
        _conf_concluida = False
        _nf_concluida   = False

    etapa_nav = st.session_state.get("ic_etapa_nav", 1)

    # Cards — 7 etapas (novo fluxo)
    st.markdown("---")
    # Verifica se upload atual tem divergências (para ativar card Conferência)
    _tem_div_atual = False
    if erp_uploads_ativo:
        _docs_conf_check = st.session_state.get(f"{_cache_key}_docs_conf", set()) if _num_ciclo_ativo else set()
        _uploads_pend = [u for u in erp_uploads_ativo if u.get("documento","") not in _docs_conf_check]
        if _uploads_pend:
            _dados_pend = _uploads_pend[-1].get("dados",[])
            _tem_div_atual = any(float(str(r.get("Divergencia Qtd",0)).replace(",",".") or 0) != 0 for r in _dados_pend)

    # Se o usuário está na etapa 2 adicionando upload, cards 3 e 4 ficam pendentes
    if etapa_nav == 2:
        _conf_concluida = False
        _nf_concluida   = False

    c1,c2,c3,c4,c5,c6 = st.columns(6)
    b1 = _card(c1,1,"Gerar lista",  "Define o ciclo e a lista",
               ativo=(etapa_nav==1), concluido=(ciclo_ativo is not None), chave="ic_n1")
    b2 = _card(c2,2,"Upload ERP",   "Importa relatório Protheus",
               ativo=(etapa_nav==2), concluido=(len(erp_uploads_ativo)>0), chave="ic_n2")
    b3 = _card(c3,3,"Conferência",  "Divergências e justificativas",
               ativo=(etapa_nav==3), concluido=_conf_concluida, chave="ic_n3")
    b4 = _card(c4,4,"NF de Ajuste", "Upload da NF de baixa/perda",
               ativo=(etapa_nav==4), concluido=_nf_concluida, chave="ic_n4")
    b5 = _card(c5,5,"Fechar",       "Relatório final e fechamento",
               ativo=(etapa_nav==5), concluido=(len(ciclos)>0), chave="ic_n5")
    b6 = _card(c6,6,"Histórico",    "PDFs dos ciclos fechados",
               ativo=(etapa_nav==6), concluido=False, chave="ic_n6")

    if b1: st.session_state["ic_etapa_nav"]=1; st.rerun()
    if b2: st.session_state["ic_etapa_nav"]=2; st.rerun()
    if b3: st.session_state["ic_etapa_nav"]=3; st.rerun()
    if b4: st.session_state["ic_etapa_nav"]=4; st.rerun()
    if b5: st.session_state["ic_etapa_nav"]=5; st.rerun()
    if b6: st.session_state["ic_etapa_nav"]=6; st.rerun()

    st.markdown("---")

    # ── ETAPA 1 ───────────────────────────────────────────────────────────
    if etapa_nav == 1:
        st.markdown("### 1. Gerar lista do ciclo")

        # Empresa/filial já definidos pelo app.py — apenas confirma com botão
        if not empresa_sel or not filial_sel:
            st.info("👆 Volte à tela inicial e selecione a Empresa e a Filial para começar.")
            return

        st.markdown(
            f"""<div style="background:#004550;border-radius:8px;padding:10px 16px;margin-bottom:8px;">
              <span style="color:#aac8cc;font-size:0.85rem;">🏢 Empresa</span>
              <span style="color:#fff;font-weight:700;margin:0 16px;">{empresa_sel}</span>
              <span style="color:#aac8cc;font-size:0.85rem;">📍 Filial</span>
              <span style="color:#fff;font-weight:700;margin-left:8px;">{filial_sel}</span>
            </div>""", unsafe_allow_html=True)

        if df_filial.empty:
            st.info("👆 Selecione modo e clique em **Gerar lista** para carregar os dados.")
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
            cols_b = st.columns([1,1,1,1,2])
            if "ic_qtd" not in st.session_state: st.session_state.ic_qtd = 4
            for cb,qtd in zip(cols_b[:4],[4,30,50,80]):
                with cb:
                    if st.button(f"{qtd}",key=f"ic_q{qtd}",type="primary" if st.session_state.ic_qtd==qtd else "secondary",use_container_width=True):
                        st.session_state.ic_qtd=qtd
            with cols_b[4]:
                btn_gerar_lista = st.button("🔍 Gerar lista", type="primary", use_container_width=True, key="ic_btn_gerar_lista")
            qtd_ciclo = min(st.session_state.ic_qtd, total_skus)
        else:
            st.caption("📊 **5%** = 20 ciclos/ano · **10%** = 10 ciclos/ano · **20%** = 5 ciclos/ano")
            col_sl, col_gb = st.columns([3,1])
            with col_sl:
                pmap = {"5%":0.05,"10%":0.10,"20%":0.20,"30%":0.30}
                pl   = st.select_slider("Faixa",list(pmap.keys()),value="10%",key="ic_pct")
                qtd_ciclo = max(1,int(total_skus*pmap[pl]))
                st.caption(f"→ {qtd_ciclo} itens de {total_skus}")
            with col_gb:
                st.markdown("<div style='margin-top:28px'>", unsafe_allow_html=True)
                btn_gerar_lista = st.button("🔍 Gerar lista", type="primary", use_container_width=True, key="ic_btn_gerar_lista")
                st.markdown("</div>", unsafe_allow_html=True)

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

        # Gera num_ciclo sequencial se já existe ciclo no mesmo dia
        _base_ciclo = f"{date.today().strftime('%Y%m%d')}-{empresa_sel}-{filial_sel}".replace(" ","")
        _ciclos_hoje = db_obter_ciclos(engine_db, empresa_sel, filial_sel)
        _nums_hoje = [c.get("num_ciclo","") for c in _ciclos_hoje if c.get("num_ciclo","").startswith(_base_ciclo)]
        # Verifica também ciclo ativo
        if ciclo_ativo and ciclo_ativo.get("num_ciclo","").startswith(_base_ciclo):
            _nums_hoje.append(ciclo_ativo.get("num_ciclo",""))
        if _nums_hoje:
            _seq = len(_nums_hoje) + 1
            num_ciclo = f"{_base_ciclo}-{_seq}"
        else:
            num_ciclo = _base_ciclo
        col_dl,col_info = st.columns([2,2])
        with col_dl:
            st.download_button("📥 Baixar Excel para Contagem",
                data=gerar_xlsx_lista(df_exib,label),
                file_name=f"inv_{num_ciclo}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with col_info:
            st.info(f"Nº do ciclo: **{num_ciclo}**")

        if btn_gerar_lista:
            db_salvar_ciclo_ativo(engine_db, empresa_sel, filial_sel, {
                "num_ciclo":      num_ciclo,
                "data_geracao":   date.today().strftime("%d/%m/%Y"),
                "label":          label,
                "qtd_lista":      len(df_exib),
                "produtos_lista": df_exib["Produto"].astype(str).tolist(),
                "uploads":        _uploads,
                "status":         "Em andamento",
            })
            # Injeta ciclo ativo diretamente no cache — não depende de releitura do banco
            _novo_ciclo = {
                "num_ciclo":      num_ciclo,
                "data_geracao":   date.today().strftime("%d/%m/%Y"),
                "label":          label,
                "qtd_lista":      len(df_exib),
                "produtos_lista": df_exib["Produto"].astype(str).tolist(),
                "uploads":        _uploads,
                "status":         "Em andamento",
            }
            for _k in list(st.session_state.keys()):
                if _k.startswith("ic_cache_"):
                    del st.session_state[_k]
            _ck = f"ic_cache_{empresa_sel}_{filial_sel}"
            st.session_state[f"{_ck}_ciclo_ativo"] = _novo_ciclo
            st.session_state[f"{_ck}_erp_uploads"] = []
            st.session_state[f"{_ck}_nf_ajustes"]  = []
            st.session_state[f"{_ck}_docs_conf"]   = set()
            st.session_state[f"{_ck}_justs"]       = {}
            st.session_state[f"{_ck}_contados"]    = contados
            st.session_state[f"{_ck}_ciclos"]      = ciclos
            st.session_state[_ck] = True
            st.session_state["ic_etapa_nav"] = 2
            st.rerun()

    # ── ETAPA 2 — UPLOAD ERP (PROTHEUS) ──────────────────────────────────
    elif etapa_nav == 2:
        if not empresa_sel or not filial_sel:
            st.warning("⚠️ Gere a lista primeiro (Etapa 1) para definir Empresa e Filial."); return
        st.markdown("### 2. Upload do relatório ERP (Protheus)")
        st.caption("Importe os Excels de inventário gerados pelo Protheus — um por etapa de contagem.")

        if not ciclo_ativo:
            st.warning("⚠️ Nenhum ciclo ativo. Gere a lista primeiro."); return

        num_ciclo_erp = ciclo_ativo.get("num_ciclo","")

        # Lista de uploads ERP já salvos
        if erp_uploads_ativo:
            st.success(f"✅ **{len(erp_uploads_ativo)} upload(s) ERP** acumulado(s) neste ciclo:")
            for i, u in enumerate(erp_uploads_ativo, 1):
                n_linhas = len(u.get("dados",[]))
                _n_div_u = sum(1 for r in u.get("dados",[])
                               if float(str(r.get("Divergencia Qtd",0)).replace(",",".") or 0) != 0)
                _label_div = f"· ⚠️ {_n_div_u} divergência(s)" if _n_div_u > 0 else "· ✅ Sem divergências"
                with st.expander(f"Etapa {i} — Documento {u.get('documento','—')} · {u.get('data_upload','—')} · {n_linhas} linha(s) {_label_div}"):
                    df_prev = pd.DataFrame(u["dados"])
                    if not df_prev.empty:
                        st.dataframe(df_prev, use_container_width=True, hide_index=True)

            # Verifica uploads não conferidos
            _docs_conf_2 = st.session_state.get(f"{_cache_key}_docs_conf", set())
            _pend_2 = [u for u in erp_uploads_ativo if u.get("documento","") not in _docs_conf_2]

            if _pend_2:
                _n_div_pend = sum(
                    1 for r in _pend_2[-1].get("dados",[])
                    if float(str(r.get("Divergencia Qtd",0)).replace(",",".") or 0) != 0)
                if _n_div_pend == 0:
                    st.info(f"ℹ️ O último upload **não tem divergências**. Você pode adicionar um novo upload abaixo ou avançar para **Fechar**.")
                else:
                    st.warning(f"⚠️ O último upload tem **{_n_div_pend} divergência(s)**. Avance para **Conferência** antes de fechar.")
        else:
            st.info("Nenhum upload ERP ainda. Faça o upload abaixo.")

        st.markdown("---")
        st.markdown("##### Adicionar novo upload ERP")
        arq_erp = st.file_uploader(
            "Selecione o Excel do Protheus",
            type=["xlsx"],
            key=f"up_erp2_{num_ciclo_erp}_{len(erp_uploads_ativo)}")

        if arq_erp:
            try:
                df_erp = pd.read_excel(arq_erp, sheet_name=0, header=0)
                df_erp.columns = [str(c).strip().upper() for c in df_erp.columns]
                col_map = {
                    "CODIGO":                   "Codigo",
                    "DESCRICAO":                "Descricao",
                    "TP":                       "Tipo",
                    "GRUPO":                    "Grupo",
                    "UM":                       "UM",
                    "AMZ":                      "Armazem",
                    "DOCUMENTO":                "Documento",
                    "QUANTIDADE INVENTARIADA":  "Qtd WMS",
                    "QTD NA DATA DO INVENTARIO":"Qtd ERP",
                    "DIFERENCA QUANTIDADE":     "Divergencia Qtd",
                    "DIFERENCA VALOR":          "Divergencia Vl",
                }
                df_erp = df_erp.rename(columns={k:v for k,v in col_map.items() if k in df_erp.columns})
                df_erp = df_erp.dropna(subset=["Codigo"])
                df_erp["Codigo"] = df_erp["Codigo"].astype(str).str.zfill(6)
                for col in ["Qtd WMS","Qtd ERP","Divergencia Qtd","Divergencia Vl"]:
                    if col in df_erp.columns:
                        df_erp[col] = pd.to_numeric(df_erp[col], errors="coerce").fillna(0)

                documento = str(df_erp["Documento"].iloc[0]).strip() if "Documento" in df_erp.columns else "—"
                docs_ja = [u.get("documento","") for u in erp_uploads_ativo]
                if documento in docs_ja:
                    st.warning(f"⚠️ Documento **{documento}** já importado. Será substituído.")

                cols_prev = [c for c in ["Codigo","Descricao","Documento","Qtd WMS","Qtd ERP",
                                          "Divergencia Qtd","Divergencia Vl"] if c in df_erp.columns]
                st.markdown(f"**{len(df_erp)} linhas** — Documento: `{documento}`")

                def _style_erp2(val):
                    try:
                        v = float(val)
                        if v < 0: return "color:#C0392B;font-weight:bold"
                        if v > 0: return "color:#27AE60;font-weight:bold"
                    except: pass
                    return ""

                st.dataframe(
                    df_erp[cols_prev].style
                    .map(_style_erp2, subset=[c for c in ["Divergencia Qtd","Divergencia Vl"] if c in cols_prev])
                    .format({"Qtd WMS":"{:,.2f}","Qtd ERP":"{:,.2f}",
                             "Divergencia Qtd":"{:,.2f}","Divergencia Vl":"R$ {:,.2f}"}, na_rep="—"),
                    use_container_width=True, hide_index=True)

                # Produtos cobertos neste upload
                prods_erp = set(df_erp["Codigo"].astype(str).str.zfill(6).tolist())
                novos_erp = pl_ciclo & prods_erp - ja_cont_ciclo
                c_a, c_b = st.columns(2)
                c_a.metric("Produtos neste upload", len(df_erp))
                c_b.metric("Novos na lista", len(novos_erp))

                if st.button("💾 Adicionar este upload ERP", type="primary", key="btn_add_erp2"):
                    db_salvar_erp_upload(engine_db, empresa_sel, filial_sel,
                                         num_ciclo_erp, documento, date.today().isoformat(),
                                         df_erp[cols_prev].to_dict("records"))
                    # Registra produtos contados via ERP no ciclo ativo
                    ciclo_f = ciclo_ativo  # usa cache local
                    ups_at  = ciclo_f.get("uploads",[]) if ciclo_f else []
                    # Calcula divergências deste upload
                    _n_div = int((df_erp["Divergencia Qtd"] != 0).sum()) if "Divergencia Qtd" in df_erp.columns else 0
                    up_info = {
                        "num_inv": documento, "data": date.today().strftime("%d/%m/%Y"),
                        "data_iso": date.today().isoformat(),
                        "responsavel": st.session_state.get("_app_operador","—"),
                        "acuracidade": "—",
                        "produtos": list(pl_ciclo & prods_erp),
                        "df_rows": [],
                    }
                    ups_at.append(up_info)
                    with engine_db.connect() as conn:
                        conn.execute(text("""
                            UPDATE inventario_ciclo_ativo
                            SET uploads_json=:v, atualizado_em=NOW()
                            WHERE empresa=:e AND filial=:f
                        """), {"v":json.dumps(ups_at),"e":empresa_sel,"f":filial_sel})
                        conn.commit()
                    # Atualiza cache local sem ir ao banco
                    _ck2 = f"ic_cache_{empresa_sel}_{filial_sel}"
                    _ciclo_upd = dict(ciclo_ativo) if ciclo_ativo else {}
                    _ciclo_upd["uploads"] = ups_at
                    st.session_state[f"{_ck2}_ciclo_ativo"] = _ciclo_upd
                    _erp_upd = list(st.session_state.get(f"{_ck2}_erp_uploads", []))
                    _erp_upd.append({"documento": documento, "data_upload": date.today().isoformat(),
                                     "dados": df_erp[cols_prev].to_dict("records")})
                    st.session_state[f"{_ck2}_erp_uploads"] = _erp_upd
                    st.session_state[_ck2] = True
                    if _n_div == 0:
                        st.success(f"✅ Upload {documento} adicionado — **sem divergências**. Você pode adicionar um novo upload ou avançar para **Fechar**.")
                    else:
                        st.success(f"✅ Upload {documento} adicionado — **{_n_div} divergência(s)**. Avance para **Conferência**.")
                    st.rerun()

            except Exception as e:
                st.error(f"Erro ao processar arquivo: {e}")

    # ── ETAPA 4 — NF DE AJUSTE ─────────────────────────────────────────
    elif etapa_nav == 4:
        if not empresa_sel or not filial_sel:
            st.warning("⚠️ Gere a lista primeiro (Etapa 1) para definir Empresa e Filial."); return
        st.markdown("### 5. Upload da NF de Ajuste")
        st.caption("Importe o PDF da Nota Fiscal de baixa/perda e informe os dados dos itens.")

        if not ciclo_ativo:
            st.warning("⚠️ Nenhum ciclo ativo. Gere a lista primeiro."); return

        num_ciclo_nf = ciclo_ativo.get("num_ciclo","")
        _nf_idx = len(nf_ajustes_ativo)  # índice único por NF nova

        # Lê justificativas diretamente do banco (não do cache)
        justs_conf   = st.session_state.get(f"{_cache_key}_justs", {})
        prods_ajuste = {p: j for p,j in justs_conf.items()
                        if j == "Ajuste de inventário" and not p.startswith("_")}

        # Limpa chaves antigas do session_state ao entrar com nova NF
        _nf_key = f"nf_itens_{num_ciclo_nf}_{_nf_idx}"

        if prods_ajuste:
            st.info(f"**{len(prods_ajuste)} produto(s)** com 'Ajuste de inventário': {', '.join(prods_ajuste.keys())}")
        else:
            st.success("✅ Nenhum produto marcado como 'Ajuste de inventário'. Esta etapa é opcional.")

        # NFs já salvas
        if nf_ajustes_ativo:
            st.markdown(f"**{len(nf_ajustes_ativo)} NF(s) já importada(s):**")
            for nf in nf_ajustes_ativo:
                with st.expander(f"NF {nf.get('num_nf','—')} · {nf.get('data_nf','—')} · {nf.get('natureza','—')} · {len(nf.get('dados',[]))} item(ns)"):
                    df_nf_prev = pd.DataFrame(nf["dados"])
                    if not df_nf_prev.empty:
                        st.dataframe(df_nf_prev, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.markdown("##### Importar nova NF de ajuste")

        arq_pdf = st.file_uploader("📎 Selecione o PDF da NF (DANFE)",
                                    type=["pdf"], key=f"nf_pdf_{num_ciclo_nf}_{_nf_idx}")

        if arq_pdf:
            pdf_bytes = arq_pdf.read()
            nf_dados, nf_erro = parsear_nf_danfe(pdf_bytes)

            if nf_erro:
                st.error(f"Erro ao ler PDF: {nf_erro}")
            elif not nf_dados["itens"]:
                st.warning("⚠️ Não foi possível extrair itens do PDF. Verifique se é um DANFE válido.")
            else:
                st.success(f"✅ NF **{nf_dados['num_nf']}** · {nf_dados['data']} · {nf_dados['natureza']} · **{len(nf_dados['itens'])} item(ns)**")

                col_nf1, col_nf2, col_nf3 = st.columns(3)
                col_nf1.markdown(f"**Nº NF:** {nf_dados['num_nf']}")
                col_nf2.markdown(f"**Data:** {nf_dados['data']}")
                col_nf3.markdown(f"**Natureza:** {nf_dados['natureza']}")

                df_itens = pd.DataFrame(nf_dados["itens"])
                st.dataframe(df_itens.style.format({
                    "Qtd":     "{:,.4f}",
                    "Vl Unit": "R$ {:,.2f}",
                    "Vl Total":"R$ {:,.2f}",
                }, na_rep="—"), use_container_width=True, hide_index=True)

                if st.button("💾 Salvar NF de ajuste", type="primary", key=f"btn_salvar_nf_{_nf_idx}"):
                    try:
                        data_nf_iso = datetime.strptime(nf_dados["data"], "%d/%m/%Y").date().isoformat() if nf_dados["data"] else date.today().isoformat()
                    except:
                        data_nf_iso = date.today().isoformat()
                    db_salvar_nf_ajuste(engine_db, empresa_sel, filial_sel, num_ciclo_nf,
                                         nf_dados["num_nf"], data_nf_iso, nf_dados["natureza"],
                                         nf_dados["itens"])
                    # Atualiza cache local
                    _nf_upd = list(st.session_state.get(f"{_cache_key}_nf_ajustes", []))
                    _nf_upd.append({"num_nf": nf_dados["num_nf"], "data_nf": data_nf_iso,
                                    "natureza": nf_dados["natureza"], "dados": nf_dados["itens"]})
                    st.session_state[f"{_cache_key}_nf_ajustes"] = _nf_upd
                    st.success(f"✅ NF {nf_dados['num_nf']} salva com {len(nf_dados['itens'])} item(ns)!")
                    st.rerun()
        else:
            st.info("Faça o upload do PDF da NF para preencher os dados automaticamente.")

    # ── ETAPA 3 — CONFERÊNCIA ────────────────────────────────────
    elif etapa_nav == 3:
        if not empresa_sel or not filial_sel:
            st.warning("⚠️ Gere a lista primeiro (Etapa 1) para definir Empresa e Filial."); return
        st.markdown("### 4. Conferência de divergências")
        st.caption("Revise as divergências do upload atual e registre justificativas.")

        if not ciclo_ativo:
            st.warning("⚠️ Nenhum ciclo ativo. Gere a lista primeiro."); return

        num_ciclo_conf = ciclo_ativo.get("num_ciclo","")

        # Documentos ERP já conferidos
        docs_conferidos = st.session_state.get(f"{_cache_key}_docs_conf", set())

        # Uploads ERP ainda NÃO conferidos
        uploads_pendentes = [u for u in erp_uploads_ativo if u.get("documento","") not in docs_conferidos]
        uploads_conferidos = [u for u in erp_uploads_ativo if u.get("documento","") in docs_conferidos]

        if uploads_conferidos:
            st.success(f"✅ {len(uploads_conferidos)} upload(s) já conferido(s): {', '.join(u.get('documento','') for u in uploads_conferidos)}")

        if not uploads_pendentes:
            st.success("✅ Todos os uploads foram conferidos!")
            if not erp_uploads_ativo:
                st.warning("⚠️ Faça o Upload ERP (etapa 2) primeiro.")
            return

        # Upload atual a conferir (o mais recente não conferido)
        upload_atual = uploads_pendentes[-1]
        doc_atual = upload_atual.get("documento","—")
        st.info(f"📋 Conferindo upload: **Documento {doc_atual}** · {upload_atual.get('data_upload','—')} · {len(upload_atual.get('dados',[]))} produto(s)")

        # Monta df de divergências apenas do upload atual
        df_div = pd.DataFrame()
        dados_atual = upload_atual.get("dados",[])
        if dados_atual:
            df_erp_atual = pd.DataFrame(dados_atual)
            if not df_erp_atual.empty and "Codigo" in df_erp_atual.columns:
                df_erp_atual["Codigo"] = df_erp_atual["Codigo"].astype(str).str.zfill(6)
                if "Divergencia Qtd" in df_erp_atual.columns:
                    df_div = df_erp_atual[df_erp_atual["Divergencia Qtd"] != 0].copy()
                else:
                    df_div = df_erp_atual.copy()

        OPCOES_JUST = [
            "Ajuste de inventário",
            "Diferença de contagem",
            "Movimentação não registrada",
            "Produto em trânsito",
            "Erro de digitação",
            "Outros",
        ]

        # Carrega justificativas já salvas para este documento
        justs_salvas = st.session_state.get(f"{_cache_key}_justs", {})

        if df_div.empty:
            st.success("✅ Nenhuma divergência neste upload.")
            if st.button("✔ Confirmar conferência sem divergências", type="primary", key="btn_conf_sem_div"):
                db_salvar_justificativas(engine_db, empresa_sel, filial_sel, num_ciclo_conf,
                                          {"_ok": "Sem divergências"}, documento=doc_atual)
                # Atualiza cache local
                _docs_upd = set(st.session_state.get(f"{_cache_key}_docs_conf", set()))
                _docs_upd.add(doc_atual)
                st.session_state[f"{_cache_key}_docs_conf"] = _docs_upd
                st.success(f"✅ Upload {doc_atual} conferido!")
                st.rerun()
        else:
            st.info(f"**{len(df_div)} produto(s) com divergência** neste upload:")

            cols_exib = [c for c in ["Codigo","Descricao","Documento","Qtd WMS","Qtd ERP",
                                      "Divergencia Qtd","Divergencia Vl"] if c in df_div.columns]
            df_just_edit = df_div[cols_exib].copy()
            df_just_edit["Codigo"] = df_just_edit["Codigo"].astype(str).str.zfill(6)
            df_just_edit["Justificativa"] = df_just_edit["Codigo"].map(
                lambda p: justs_salvas.get(p, OPCOES_JUST[0]))

            col_cfg = {
                "Codigo":         st.column_config.TextColumn("Código", disabled=True, width="small"),
                "Descricao":      st.column_config.TextColumn("Descrição", disabled=True, width="large"),
                "Documento":      st.column_config.TextColumn("Documento", disabled=True, width="small"),
                "Qtd WMS":        st.column_config.NumberColumn("Qtd WMS", disabled=True, format="%.2f"),
                "Qtd ERP":        st.column_config.NumberColumn("Qtd ERP", disabled=True, format="%.2f"),
                "Divergencia Qtd":st.column_config.NumberColumn("Δ Qtd", disabled=True, format="%.2f"),
                "Divergencia Vl": st.column_config.NumberColumn("Δ Valor", disabled=True, format="R$ %.2f"),
                "Justificativa":  st.column_config.SelectboxColumn("Justificativa", options=OPCOES_JUST, required=True, width="medium"),
            }

            df_result = st.data_editor(
                df_just_edit,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                column_config=col_cfg,
                key=f"just_editor_{num_ciclo_conf}_{doc_atual}")

            if st.button("💾 Salvar e confirmar conferência", type="primary", key="btn_salvar_just4"):
                justs_edit = dict(zip(df_result["Codigo"].astype(str), df_result["Justificativa"].astype(str)))
                db_salvar_justificativas(engine_db, empresa_sel, filial_sel, num_ciclo_conf,
                                          justs_edit, documento=doc_atual)
                # Atualiza cache local
                _justs_upd = dict(st.session_state.get(f"{_cache_key}_justs", {}))
                _justs_upd.update(justs_edit)
                st.session_state[f"{_cache_key}_justs"] = _justs_upd
                _docs_upd = set(st.session_state.get(f"{_cache_key}_docs_conf", set()))
                _docs_upd.add(doc_atual)
                st.session_state[f"{_cache_key}_docs_conf"] = _docs_upd
                n_ajuste = sum(1 for v in justs_edit.values() if v == "Ajuste de inventário")
                st.success(f"✅ Upload {doc_atual} conferido!")
                if n_ajuste > 0:
                    st.warning(f"⚠️ **{n_ajuste} produto(s)** com 'Ajuste de inventário' — faça o upload da NF na Etapa 4.")
                st.rerun()

    # ── ETAPA 5 — FECHAR INVENTÁRIO ──────────────────────────────────────
    elif etapa_nav == 5:
        if not empresa_sel or not filial_sel:
            st.warning("⚠️ Gere a lista primeiro (Etapa 1) para definir Empresa e Filial."); return
        st.markdown("### 6. Fechar inventário")

        if ciclo_ativo:
            num_ciclo_fech = ciclo_ativo.get("num_ciclo","")
            st.markdown(
                f"""<div style="background:#004550;border-radius:8px;padding:12px 16px;margin-bottom:12px;">
                  <div style="display:flex;justify-content:space-between;align-items:center;">
                    <div><span style="color:#EC6E21;font-weight:bold;">📋 {num_ciclo_fech}</span><br>
                    <span style="color:#fff;font-size:0.85rem;">Gerado em {ciclo_ativo["data_geracao"]} · {ciclo_ativo["qtd_lista"]} produtos · {ciclo_ativo.get("qtd_uploads", len(_uploads))} upload(s)</span></div>
                    <span style="color:#EC6E21;font-weight:bold;font-size:1.2rem;">{pct_ciclo:.1f}%</span>
                  </div></div>""", unsafe_allow_html=True)

            # Carrega TODOS os uploads ERP do ciclo e consolida
            erp_uploads_fech = st.session_state.get(f"{_cache_key}_erp_uploads", [])
            # Consolida todos os dados ERP em um único DataFrame
            if erp_uploads_fech:
                df_erp_fech = pd.concat(
                    [pd.DataFrame(u["dados"]) for u in erp_uploads_fech if u.get("dados")],
                    ignore_index=True
                ) if erp_uploads_fech else pd.DataFrame()
                # Remove duplicatas por código — mantém última ocorrência
                if not df_erp_fech.empty and "Codigo" in df_erp_fech.columns:
                    df_erp_fech = df_erp_fech.drop_duplicates(subset=["Codigo"], keep="last")
            else:
                df_erp_fech = pd.DataFrame()

            if df_erp_fech.empty:
                st.warning("⚠️ Nenhum dado do ERP encontrado. Faça o **Upload ERP** (etapa 2) antes de fechar.")
            else:
                docs = ", ".join(u.get("documento","—") for u in erp_uploads_fech)
                st.success(f"✅ {len(erp_uploads_fech)} upload(s) ERP · {len(df_erp_fech)} produtos · Documentos: **{docs}**")

            # Carrega justificativas e NFs de ajuste
            justs_fech = st.session_state.get(f"{_cache_key}_justs", {})
            nfs_fech   = st.session_state.get(f"{_cache_key}_nf_ajustes", [])

            # Mapa de itens da NF de ajuste: {codigo: {Qtd, Vl Total}}
            nf_map = {}
            for nf in nfs_fech:
                for item in nf.get("dados",[]):
                    cod = str(item.get("Codigo","")).zfill(6)
                    nf_map[cod] = {
                        "Qtd":      float(item.get("Qtd",0)),
                        "Vl Total": float(item.get("Vl Total",0)),
                        "Num NF":   nf.get("num_nf","—"),
                    }

            # Monta relatório final: direto do ERP Protheus (fonte da verdade)
            if not df_erp_fech.empty:
                df_fech = df_erp_fech.copy()

                # Garante colunas numéricas
                for col in ["Qtd WMS","Qtd ERP","Divergencia Qtd","Divergencia Vl"]:
                    if col in df_fech.columns:
                        df_fech[col] = pd.to_numeric(df_fech[col], errors="coerce").fillna(0)

                # Vl Unit do ERP interno para calcular Vl WMS e Vl ERP
                # Usa Divergencia Vl do Protheus como base: Vl Unit = Vl Div / Qtd Div (quando possível)
                vl_map = {}
                if not df_filial.empty and "Produto" in df_filial.columns and "Vl Unit" in df_filial.columns:
                    _vl = df_filial[["Produto","Vl Unit"]].copy()
                    _vl["Produto"] = _vl["Produto"].astype(str).str.zfill(6)
                    vl_map = _vl.drop_duplicates("Produto").set_index("Produto")["Vl Unit"].to_dict()

                df_fech["Codigo_6z"] = df_fech["Codigo"].astype(str).str.zfill(6)
                df_fech["Vl Unit"]   = df_fech["Codigo_6z"].map(lambda p: vl_map.get(p, 0))
                df_fech["Vl WMS"]    = df_fech["Qtd WMS"] * df_fech["Vl Unit"]
                df_fech["Vl ERP"]    = df_fech["Qtd ERP"] * df_fech["Vl Unit"]
                if "Divergencia Vl" not in df_fech.columns:
                    df_fech["Divergencia Vl"] = df_fech["Divergencia Qtd"] * df_fech["Vl Unit"]

                # Aplica lógica de NF de ajuste:
                # Se justificativa == "Ajuste de inventário" → usa qtd/valor da NF
                # Caso contrário → mantém valor do ERP
                def _qtd_final(row):
                    cod = str(row.get("Codigo_6z","")).zfill(6)
                    just = justs_fech.get(cod,"")
                    if just == "Ajuste de inventário" and cod in nf_map:
                        return nf_map[cod]["Qtd"]
                    return row.get("Qtd WMS", 0)

                def _vl_final(row):
                    cod = str(row.get("Codigo_6z","")).zfill(6)
                    just = justs_fech.get(cod,"")
                    if just == "Ajuste de inventário" and cod in nf_map:
                        return nf_map[cod]["Vl Total"]
                    return row.get("Vl WMS", 0)

                df_fech["Qtd Final"]      = df_fech.apply(_qtd_final, axis=1)
                df_fech["Vl Final"]       = df_fech.apply(_vl_final, axis=1)
                df_fech["Justificativa"]  = df_fech["Codigo_6z"].map(lambda p: justs_fech.get(p,"—"))
                df_fech["NF Ajuste"]      = df_fech["Codigo_6z"].map(lambda p: nf_map[p]["Num NF"] if p in nf_map else "—")

                # Tabela final
                cols_rel = [c for c in ["Codigo","Descricao","Documento",
                                        "Qtd WMS","Vl WMS","Qtd ERP","Vl ERP",
                                        "Divergencia Qtd","Divergencia Vl",
                                        "Justificativa","NF Ajuste"] if c in df_fech.columns]
                df_rel_fech = df_fech[cols_rel].rename(columns={
                    "Codigo":         "Código",
                    "Descricao":      "Descrição",
                    "Documento":      "Documento",
                    "Qtd WMS":        "Qtd WMS",
                    "Vl WMS":         "Vl WMS",
                    "Qtd ERP":        "Qtd ERP",
                    "Vl ERP":         "Vl ERP",
                    "Divergencia Qtd":"Divergência",
                    "Divergencia Vl": "Vl Divergência",
                    "Justificativa":  "Justificativa",
                    "NF Ajuste":      "NF Ajuste",
                })

                def _style_fech(val):
                    try:
                        v = float(val)
                        if v < 0: return "color:#C0392B;font-weight:bold"
                        if v > 0: return "color:#27AE60;font-weight:bold"
                    except: pass
                    return ""

                st.markdown("#### Relatório de fechamento")
                st.dataframe(
                    df_rel_fech.style
                    .map(_style_fech, subset=[c for c in ["Divergência","Vl Divergência"] if c in df_rel_fech.columns])
                    .format({
                        "Qtd WMS":        "{:,.2f}",
                        "Vl WMS":         "R$ {:,.2f}",
                        "Qtd ERP":        "{:,.2f}",
                        "Vl ERP":         "R$ {:,.2f}",
                        "Divergência":    "{:,.2f}",
                        "Vl Divergência": "R$ {:,.2f}",
                    }, na_rep="—"),
                    use_container_width=True, hide_index=True)

                # KPIs de resumo
                c1f,c2f,c3f,c4f = st.columns(4)
                c1f.metric("Total produtos", len(df_rel_fech))
                n_div_f  = int((df_rel_fech["Divergência"] != 0).sum()) if "Divergência" in df_rel_fech.columns else 0
                c2f.metric("Divergentes", n_div_f)
                vl_div_f = df_rel_fech["Vl Divergência"].sum() if "Vl Divergência" in df_rel_fech.columns else 0
                c3f.metric("Vl Total Divergência", f"R$ {vl_div_f:,.2f}")
                c4f.metric("Cobertura WMS", f"{pct_ciclo:.1f}%")

            if pct_ciclo >= 100:
                if not df_erp_fech.empty:
                    st.success("✅ 100% contados e ERP importado! Pronto para fechar.")
                    # PDF preview
                    _df_prev6 = montar_df_relatorio(_uploads, df_filial)
                    _pdf_prev6 = gerar_pdf_kpmg(
                        {**ciclo_ativo,
                         "data":         _uploads[-1].get("data","—") if _uploads else "—",
                         "responsavel":  _uploads[-1].get("responsavel","—") if _uploads else "—",
                         "acuracidade":  _uploads[-1].get("acuracidade","—") if _uploads else "—",
                         "cobertura_pct":pct_ciclo},
                        _df_prev6, empresa_sel, filial_sel) if not _df_prev6.empty else None
                    if _pdf_prev6:
                        st.download_button("📄 Pré-visualizar PDF",
                            data=_pdf_prev6,
                            file_name=f"kpmg_preview_{num_ciclo_fech}.pdf",
                            mime="application/pdf", key="ic_pdf_preview6")
                else:
                    st.warning("⚠️ Faça o Upload ERP (etapa 2) antes de fechar.")

                if not df_erp_fech.empty:
                    if st.button("🏁 Fechar inventário", key="ic_fechar", type="primary"):
                        todos = set()
                        for u in _uploads: todos.update(str(p).strip().zfill(6) for p in u.get("produtos",[]))
                        data_iso = _uploads[-1].get("data_iso",date.today().isoformat()) if _uploads else date.today().isoformat()
                        pct_f    = len(todos & pl_ciclo)/len(pl_ciclo)*100 if pl_ciclo else 0

                        # relatorio_json = dados WMS×ERP no formato padrão (coluna "Produto")
                        df_rel   = montar_df_relatorio(_uploads, df_filial)
                        rel_json = df_rel.to_json(orient="records", force_ascii=False) if not df_rel.empty else "[]"

                        # erp_json = dados brutos do Protheus COM documento (todos os registros, sem deduplicar)
                        df_erp_raw = pd.concat(
                            [pd.DataFrame(u["dados"]) for u in erp_uploads_ativo if u.get("dados")],
                            ignore_index=True) if erp_uploads_ativo else pd.DataFrame()
                        erp_json = df_erp_raw.to_json(orient="records", force_ascii=False) if not df_erp_raw.empty else "[]"
                        cf = {**ciclo_ativo,
                              "uploads":         _uploads,        # preserva lista completa para o PDF no histórico
                              "qtd_uploads":     len(_uploads),   # contador separado para exibição
                              "produtos_contados":list(todos),
                              "cobertura_pct":   pct_f,
                              "status":          "Concluído",
                              "num_inv":     _uploads[-1].get("num_inv","—") if _uploads else "—",
                              "data":        _uploads[-1].get("data","—") if _uploads else "—",
                              "responsavel": _uploads[-1].get("responsavel","—") if _uploads else "—",
                              "acuracidade": _uploads[-1].get("acuracidade","—") if _uploads else "—",
                              "relatorio_json": rel_json,
                              "erp_json":       erp_json}
                        try:
                            db_gravar_ciclo(engine_db, empresa_sel, filial_sel, cf)
                        except Exception as _err_grav:
                            st.error(f"❌ Erro ao gravar ciclo no banco: {_err_grav}\n\n"
                                     "Verifique se a coluna **erp_json** existe na tabela `inventario_ciclos`. "
                                     "Se necessário, execute: `ALTER TABLE inventario_ciclos ADD COLUMN IF NOT EXISTS erp_json TEXT DEFAULT '[]';`")
                            st.stop()
                        db_marcar_contados(engine_db, empresa_sel, filial_sel, list(todos),
                                           data=data_iso, num_ciclo=ciclo_ativo.get("num_ciclo",""))
                        db_fechar_ciclo_ativo(engine_db, empresa_sel, filial_sel)
                        # Injeta ciclo fechado no cache sem recarregar do banco
                        _ck_f = f"ic_cache_{empresa_sel}_{filial_sel}"
                        _ciclos_upd = list(st.session_state.get(f"{_ck_f}_ciclos", []))
                        _ciclos_upd.append(cf)
                        for _k in [k for k in st.session_state if k.startswith("ic_cache_") or k.startswith("_pdf5_")]:
                            del st.session_state[_k]
                        st.session_state[f"{_ck_f}_ciclos"]      = _ciclos_upd
                        st.session_state[f"{_ck_f}_ciclo_ativo"] = None
                        st.session_state[f"{_ck_f}_erp_uploads"] = []
                        st.session_state[f"{_ck_f}_nf_ajustes"]  = []
                        st.session_state[f"{_ck_f}_docs_conf"]   = set()
                        st.session_state[f"{_ck_f}_justs"]       = {}
                        st.session_state[f"{_ck_f}_contados"]    = contados
                        st.session_state[_ck_f] = True
                        st.session_state["ic_fechado_msg"] = True
                        st.session_state["ic_etapa_nav"]   = 6
                        st.rerun()
            else:
                # SKUs pendentes — oferece escolha
                fl = sorted(list(faltam))
                ms = len(faltam)-5 if len(faltam)>5 else 0
                ls = ", ".join(f"`{p}`" for p in fl[:5]) + (f" e mais {ms}" if ms else "")

                st.warning(f"⚠️ **Faltam {len(faltam)} SKU(s) para contar:** {ls}")
                st.markdown("Os itens não contados permanecerão na próxima lista com **alta prioridade**.")

                col_fin, col_vol = st.columns(2)
                with col_fin:
                    if st.button("🏁 Finalizar assim mesmo", type="primary", key="ic_fechar_parcial",
                                 use_container_width=True):
                        st.session_state["ic_confirmar_parcial"] = True
                        st.rerun()
                with col_vol:
                    if st.button("📋 Voltar ao Upload ERP", key="ic_voltar_upload",
                                 use_container_width=True):
                        st.session_state["ic_etapa_nav"] = 2
                        st.rerun()

                # Confirmação após clique em Finalizar assim mesmo
                if st.session_state.get("ic_confirmar_parcial"):
                    st.error(f"⚠️ Confirma o fechamento com **{len(faltam)} SKU(s) não contado(s)**?")
                    cc1, cc2 = st.columns(2)
                    with cc1:
                        if st.button("✅ Sim, fechar", type="primary", key="ic_fechar_conf",
                                     use_container_width=True):
                            # Fecha com os itens que foram contados
                            todos = set()
                            for u in _uploads: todos.update(str(p).strip().zfill(6) for p in u.get("produtos",[]))
                            data_iso = _uploads[-1].get("data_iso", date.today().isoformat()) if _uploads else date.today().isoformat()
                            pct_f = len(todos & pl_ciclo)/len(pl_ciclo)*100 if pl_ciclo else 0
                            df_rel   = montar_df_relatorio(_uploads, df_filial)
                            rel_json = df_rel.to_json(orient="records", force_ascii=False) if not df_rel.empty else "[]"
                            df_erp_raw2 = pd.concat(
                                [pd.DataFrame(u["dados"]) for u in erp_uploads_ativo if u.get("dados")],
                                ignore_index=True) if erp_uploads_ativo else pd.DataFrame()
                            erp_json2 = df_erp_raw2.to_json(orient="records", force_ascii=False) if not df_erp_raw2.empty else "[]"
                            cf = {**ciclo_ativo,
                                  "uploads": _uploads, "qtd_uploads": len(_uploads),
                                  "produtos_contados": list(todos),
                                  "data": date.today().strftime("%d/%m/%Y"),
                                  "responsavel": st.session_state.get("_app_operador","—"),
                                  "cobertura_pct": pct_f, "status": "Concluído (parcial)",
                                  "relatorio_json": rel_json, "erp_json": erp_json2}
                            try:
                                db_gravar_ciclo(engine_db, empresa_sel, filial_sel, cf)
                            except Exception as _e:
                                st.error(f"Erro ao gravar: {_e}"); st.stop()
                            db_marcar_contados(engine_db, empresa_sel, filial_sel, list(todos),
                                               data=data_iso, num_ciclo=ciclo_ativo.get("num_ciclo",""))
                            db_fechar_ciclo_ativo(engine_db, empresa_sel, filial_sel)
                            st.session_state.pop("ic_confirmar_parcial", None)
                            _ck_fp = f"ic_cache_{empresa_sel}_{filial_sel}"
                            _ciclos_upd2 = list(st.session_state.get(f"{_ck_fp}_ciclos", []))
                            _ciclos_upd2.append(cf)
                            for _k in [k for k in st.session_state if k.startswith("ic_cache_") or k.startswith("_pdf5_")]:
                                del st.session_state[_k]
                            st.session_state[f"{_ck_fp}_ciclos"]      = _ciclos_upd2
                            st.session_state[f"{_ck_fp}_ciclo_ativo"] = None
                            st.session_state[f"{_ck_fp}_erp_uploads"] = []
                            st.session_state[f"{_ck_fp}_nf_ajustes"]  = []
                            st.session_state[f"{_ck_fp}_docs_conf"]   = set()
                            st.session_state[f"{_ck_fp}_justs"]       = {}
                            st.session_state[f"{_ck_fp}_contados"]    = contados
                            st.session_state[_ck_fp] = True
                            st.session_state["ic_fechado_msg"] = True
                            st.session_state["ic_etapa_nav"]   = 6
                            st.rerun()
                    with cc2:
                        if st.button("❌ Cancelar", key="ic_fechar_cancel", use_container_width=True):
                            st.session_state.pop("ic_confirmar_parcial", None)
                            st.rerun()
        else:
            st.info("Nenhum ciclo ativo no momento.")

    # ── ETAPA 6 — HISTÓRICO ──────────────────────────────────────────────
    elif etapa_nav == 6:
        if not empresa_sel or not filial_sel:
            st.warning("⚠️ Gere a lista primeiro (Etapa 1) para definir Empresa e Filial."); return
        st.markdown("### 7. Histórico KPMG")

        if not ciclos:
            st.info("Nenhum ciclo fechado ainda. Feche um inventário para ver o histórico aqui.")
            return

        st.caption("☑️ Selecione um ou mais ciclos para gerar o PDF. Ao selecionar mais de um, o relatório será consolidado.")

        # Monta dfs_rel para cada ciclo (necessário para o PDF)
        # Limpa caches de PDF antigos que possam ter ficado como None
        # Limpa caches de PDF antigos que possam ter ficado como None
        # (podem ter sido gerados antes dos dados estarem prontos)
        dfs_rel_todos = {}
        for c in ciclos:
            num_c = c.get("num_ciclo","")
            _pdf_key = f"_pdf5_bytes_{num_c}"
            if st.session_state.get(_pdf_key) is None:
                st.session_state.pop(_pdf_key, None)

        for c in ciclos:
            num_c    = c.get("num_ciclo","")
            rel_json = c.get("relatorio_json","[]")
            prods_c  = {str(p).zfill(6) for p in c.get("produtos_contados", [])}

            # Tenta 1: relatorio_json salvo no banco — filtra só produtos inventariados
            df_c = pd.DataFrame()
            try:
                if rel_json and rel_json not in ("[]", "", None):
                    df_c = pd.read_json(io.StringIO(rel_json), orient="records")
                    # Se produtos_contados está disponível, restringe o relatório a eles
                    if not df_c.empty and prods_c and "Produto" in df_c.columns:
                        df_c = df_c[df_c["Produto"].astype(str).str.zfill(6).isin(prods_c)].copy()
            except Exception:
                df_c = pd.DataFrame()

            # Tenta 2: monta a partir dos uploads salvos no ciclo
            if df_c.empty:
                ups_c = c.get("uploads", [])
                if not isinstance(ups_c, list):
                    ups_c = []  # ciclos legados gravaram uploads como int — normaliza
                if ups_c and not df_filial.empty:
                    df_c = montar_df_relatorio(ups_c, df_filial)

            # Tenta 3: reconstrói a partir do erp_json salvo no ciclo
            if df_c.empty:
                erp_json_c = c.get("erp_json", "[]")
                try:
                    if erp_json_c and erp_json_c not in ("[]", "", None):
                        _df_erp_c = pd.read_json(io.StringIO(erp_json_c), orient="records")
                        if not _df_erp_c.empty:
                            _col_map = {
                                "Codigo":         "Produto",
                                "Descricao":      "Descrição",
                                "Qtd WMS":        "Invent WMS",
                                "Qtd ERP":        "Saldo ERP (Total)",
                                "Divergencia Qtd":"Diferença Invent",
                                "Divergencia Vl": "Vl Total Diferença",
                            }
                            _df_erp_c = _df_erp_c.rename(columns={k:v for k,v in _col_map.items() if k in _df_erp_c.columns})
                            if "Produto" in _df_erp_c.columns:
                                _df_erp_c["Produto"] = _df_erp_c["Produto"].astype(str).str.zfill(6)
                            if not df_filial.empty and "Produto" in df_filial.columns and "Vl Unit" in df_filial.columns:
                                _vl = df_filial[["Produto","Vl Unit"]].copy()
                                _vl["Produto"] = _vl["Produto"].astype(str).str.zfill(6)
                                _vl = _vl.drop_duplicates("Produto")
                                _df_erp_c = _df_erp_c.merge(_vl, on="Produto", how="left")
                                _df_erp_c["Vl Unit"] = pd.to_numeric(_df_erp_c.get("Vl Unit", 0), errors="coerce").fillna(0)
                                saldo_c = pd.to_numeric(_df_erp_c.get("Saldo ERP (Total)", 0), errors="coerce").fillna(0)
                                _df_erp_c["Vl Total ERP"] = saldo_c * _df_erp_c["Vl Unit"]
                            if "Saldo WMS" not in _df_erp_c.columns and "Invent WMS" in _df_erp_c.columns:
                                _df_erp_c["Saldo WMS"] = _df_erp_c["Invent WMS"]
                            if "Acuracidade" not in _df_erp_c.columns:
                                _df_erp_c["Acuracidade"] = c.get("acuracidade","—")
                            df_c = _df_erp_c
                except Exception:
                    df_c = pd.DataFrame()

            # Tenta 4: monta a partir de produtos_contados + df_filial (fallback final)
            if df_c.empty and not df_filial.empty:
                prods = [str(p).zfill(6) for p in c.get("produtos_contados", [])]
                if prods and "Produto" in df_filial.columns:
                    df_c = df_filial[df_filial["Produto"].astype(str).str.zfill(6).isin(prods)].copy()
                    if not df_c.empty:
                        for col in ["Saldo WMS","Invent WMS","Diferença Invent","Vl Total Diferença"]:
                            if col not in df_c.columns:
                                df_c[col] = 0
                        if "Acuracidade" not in df_c.columns:
                            df_c["Acuracidade"] = c.get("acuracidade","—")
                        if "Vl Total ERP" not in df_c.columns and "Vl Unit" in df_c.columns:
                            df_c["Vl Total ERP"] = (
                                pd.to_numeric(df_c.get("Saldo ERP (Total)",0), errors="coerce").fillna(0) *
                                pd.to_numeric(df_c["Vl Unit"], errors="coerce").fillna(0))

            dfs_rel_todos[num_c] = df_c

        # Monta mapa de justificativas e NFs por ciclo para o PDF
        _justs_por_ciclo = {}
        _nfs_por_ciclo   = {}
        for c in ciclos:
            num_c = c.get("num_ciclo","")
            _justs_por_ciclo[num_c] = db_obter_justificativas(engine_db, empresa_sel, filial_sel, num_c)
            _nfs_raw = db_obter_nf_ajustes(engine_db, empresa_sel, filial_sel, num_c)
            # mapa {codigo: num_nf}
            _nf_map_c = {}
            for nf in _nfs_raw:
                for item in nf.get("dados",[]):
                    cod = str(item.get("Codigo","")).zfill(6)
                    _nf_map_c[cod] = nf.get("num_nf","—")
            _nfs_por_ciclo[num_c] = _nf_map_c

        # Injeta justificativas e NFs nos ciclos para o PDF
        ciclos_com_extra = []
        for c in ciclos:
            num_c = c.get("num_ciclo","")
            c_ext = dict(c)
            c_ext["_justs_pdf"] = _justs_por_ciclo.get(num_c, {})
            c_ext["_nfs_pdf"]   = _nfs_por_ciclo.get(num_c, {})
            ciclos_com_extra.append(c_ext)

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

        for c in ciclos_com_extra:
            num_c = c.get("num_ciclo","—")
            df_c  = dfs_rel_todos.get(num_c, pd.DataFrame())
            n_sku = len(df_c) if not df_c.empty else c.get("qtd_contados", len(c.get("produtos_contados",[])))

            # Acuracidade calculada
            _acur_calc = None
            for col_div in ["Diferença Invent","Divergencia Qtd","Divergência"]:
                if not df_c.empty and col_div in df_c.columns:
                    total_c = len(df_c)
                    sem_div_c = int((pd.to_numeric(df_c[col_div], errors="coerce").fillna(0) == 0).sum())
                    _acur_calc = f"{sem_div_c/total_c*100:.1f}%" if total_c > 0 else "—"
                    break

            ck, cc, cd, cr, ca, cs, ccob, cst, cpdf = st.columns(
                [0.5, 2.5, 1.5, 2.5, 1.2, 1, 1.2, 1.2, 1.5])
            checked = ck.checkbox("", key=f"ck5_{num_c}", label_visibility="collapsed")
            cc.caption(num_c)
            cd.caption(c.get("data","—"))
            cr.caption(c.get("responsavel","—"))
            ca.caption(_acur_calc or str(c.get("acuracidade","—")))
            cs.caption(str(n_sku))
            ccob.caption(f"{c.get('cobertura_pct',0):.1f}%")
            cst.caption(c.get("status","—"))

            # Gera PDF agora — sem cache, sem pré-geração
            if not df_c.empty:
                try:
                    pdf_b = gerar_pdf_kpmg_consolidado([c], {num_c: df_c}, empresa_sel, filial_sel)
                    if pdf_b and isinstance(pdf_b, bytes):
                        cpdf.download_button(
                            "📄",
                            data=pdf_b,
                            file_name=f"kpmg_{num_c}.pdf",
                            mime="application/pdf",
                            key=f"dl5_{num_c}",
                            help=f"Baixar PDF — {num_c}")
                    else:
                        cpdf.caption("⚠️ PDF vazio")
                except Exception as _epdf:
                    cpdf.caption(f"⚠️ {str(_epdf)[:80]}")
            else:
                cpdf.caption("sem dados")

            if checked:
                sel_ciclos.append(c)

        # Botão PDF consolidado (aparece quando 2+ selecionados)
        st.markdown("---")
        if len(sel_ciclos) >= 2:
            st.info(f"**{len(sel_ciclos)} ciclos selecionados** — o PDF será consolidado.")
            dfs_sel = {c.get("num_ciclo",""): dfs_rel_todos.get(c.get("num_ciclo",""), pd.DataFrame()) for c in sel_ciclos}
            try:
                pdf_b = gerar_pdf_kpmg_consolidado(sel_ciclos, dfs_sel, empresa_sel, filial_sel)
                if pdf_b:
                    nomes = "_".join(c.get("num_ciclo","") for c in sel_ciclos[:2])
                    st.download_button(
                        "📄 Baixar PDF Consolidado",
                        data=pdf_b,
                        file_name=f"kpmg_consolidado_{nomes}.pdf",
                        mime="application/pdf",
                        key="dl5_consol",
                        type="primary")
                else:
                    st.error("⚠️ Erro ao gerar PDF consolidado.")
            except Exception as _econs:
                st.error(f"Erro: {_econs}")
        elif len(sel_ciclos) == 1:
            st.info("1 ciclo selecionado — selecione mais para consolidar.")
            if pdf_b:
                nomes = "_".join(c.get("num_ciclo","") for c in sel_ciclos[:2])
                st.download_button(
                    "📄 Baixar PDF Consolidado",
                    data=pdf_b,
                    file_name=f"kpmg_consolidado_{nomes}.pdf",
                    mime="application/pdf",
                    key="dl5_consol",
                    type="primary")
            else:
                st.error("⚠️ ReportLab não disponível. Adicione `reportlab` ao requirements.txt.")
        elif len(sel_ciclos) == 1:
            st.info("1 ciclo selecionado — selecione mais para consolidar.")

        st.markdown("---")
        if st.button("🔄 Novo período", key="ic_reset5"):
            db_resetar_tudo(engine_db, empresa_sel, filial_sel)
            st.session_state["ic_etapa_nav"] = 1
            st.success("Novo período iniciado!")
            st.rerun()

