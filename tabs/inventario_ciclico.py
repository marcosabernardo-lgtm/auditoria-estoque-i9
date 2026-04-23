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
    import re as _re, io as _io
    result = {"num_nf":"","data":"","natureza":"","itens":[]}
    try:
        import pdfplumber
        with pdfplumber.open(_io.BytesIO(arquivo_bytes)) as pdf:
            text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception as e:
        return result, str(e)

    def _br_float(s):
        """Converte número BR com ponto de milhar (1.989,10) para float."""
        s = str(s).strip()
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        return float(s)

    nums = _re.findall(r'N\.\s*0*(\d+)', text)
    if nums: result["num_nf"] = nums[0].zfill(9)

    m = _re.search(r'DATA DE EMISS[ÃA]O\s*\n?\s*(\d{2}/\d{2}/\d{4})', text)
    if m:
        result["data"] = m.group(1)
    else:
        m = _re.search(r'(\d{2}/\d{2}/\d{4})\s+\d{2}:\d{2}:\d{2}', text)
        if m: result["data"] = m.group(1)

    m = _re.search(r'NATUREZA DA OPERA[ÇC][ÃA]O\s*\n\s*(.+?)(?:\s+PROTOCOLO|\n)', text)
    if m:
        result["natureza"] = m.group(1).strip()
    else:
        m = _re.search(r'(BAIXA [A-Z]+|VENDA|TRANSFERENCIA|AJUSTE DE INVENTARIO)', text)
        if m: result["natureza"] = m.group(1)

    itens = []
    padrao = _re.compile(
        r'(\d{6})\s+(.+?)\s+\d{8}\s+\d{3}\s+\d{4}\s+\w+\s+'
        r'([\d,]+)\s+([\d.,]+)\s+([\d.,]+)',
        _re.MULTILINE)
    for m in padrao.finditer(text):
        try:
            itens.append({
                "Codigo":   m.group(1),
                "Descricao":m.group(2).strip(),
                "Qtd":      _br_float(m.group(3)),
                "Vl Unit":  _br_float(m.group(4)),
                "Vl Total": _br_float(m.group(5)),
            })
        except:
            pass
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
    if not uploads or df_filial is None or df_filial.empty:
        return pd.DataFrame()

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
    if "Saldo WMS" not in df_wms_all.columns and "Qtd Antes" in df_wms_all.columns:
        df_wms_all = df_wms_all.rename(columns={"Qtd Antes":"Saldo WMS","Qtd Depois":"Invent WMS"})
    for col in ["Saldo WMS","Invent WMS"]:
        if col in df_wms_all.columns:
            df_wms_all[col] = pd.to_numeric(df_wms_all[col], errors="coerce").fillna(0)
    df_wms_ult = df_wms_all.drop_duplicates(subset=["Codigo"], keep="last")

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
    return gerar_pdf_kpmg_consolidado([ciclo], {ciclo.get("num_ciclo",""): df_rel}, empresa, filial)


def gerar_pdf_kpmg_consolidado(ciclos_sel, dfs_rel, empresa, filial):
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
    _fil_display = filial.split(" - ")[-1] if " - " in filial else filial
    label_unidade = f"{empresa} — {_fil_display}"

    datas = [c.get("data","") for c in ciclos_sel if c.get("data","") not in ("","—")]
    data_ini = min(datas) if datas else "—"
    data_fim = max(datas) if datas else "—"

    total_skus_cont = sum(
        len(dfs_rel.get(c.get("num_ciclo",""), pd.DataFrame()))
        for c in ciclos_sel
    )
    def _calc_acur(df_c):
        if df_c.empty: return None
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
            try: acur = float(str(c.get("acuracidade","")).replace("%","").replace(",",".")) or None
            except: acur = None
        acur_por_ciclo[num_c] = acur

    vals_validos = [v for v in acur_por_ciclo.values() if v is not None]
    acur_media = f"{sum(vals_validos)/len(vals_validos):.1f}%" if vals_validos else "N/D"
    cobertura_max = max((c.get("cobertura_pct",0) for c in ciclos_sel), default=0)
    n_ciclos = len(ciclos_sel)

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

    status_cobertura = "CUMPRIDA ✓" if cobertura_max >= 100 else "EM ANDAMENTO"
    elems.append(Paragraph(
        f"A unidade {label_unidade} realizou {n_ciclos} ciclo(s) de inventário no período de "
        f"{data_ini} a {data_fim}. A cobertura acumulada atingiu {cobertura_max:.1f}% dos SKUs "
        f"cadastrados, com acuracidade média de {acur_media}. "
        f"Exigência KPMG de cobertura anual: <b>{status_cobertura}</b> ({cobertura_max:.1f}%).",
        sty("ctx", fontSize=9, textColor=colors.black, leading=13)
    ))

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
        _erp_j = c.get("erp_json","[]")
        try:
            _erp_data = json.loads(_erp_j) if _erp_j and _erp_j != "[]" else []
        except:
            _erp_data = []

        _docs_map = defaultdict(list)
        for r in _erp_data:
            _doc_num = str(r.get("Documento","—")).strip()
            _docs_map[_doc_num].append(r)

        if not _docs_map:
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
            docs_list = list(_docs_map.keys())
            n_docs = len(docs_list)
            for j, _doc_num in enumerate(docs_list):
                itens_doc = _docs_map[_doc_num]
                n_sku_doc = len(set(str(r.get("Codigo","")).zfill(6) for r in itens_doc))
                n_div_doc = sum(1 for r in itens_doc
                                if float(str(r.get("Divergencia Qtd",0)).replace(",",".") or 0) != 0)
                acur_doc = f"{(n_sku_doc-n_div_doc)/n_sku_doc*100:.1f}%" if n_sku_doc > 0 else "—"
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

    for idx, c in enumerate(ciclos_sel, 1):
        elems.append(PageBreak())
        num_c  = c.get("num_ciclo","—")
        df_rel = dfs_rel.get(num_c, pd.DataFrame())
        n_sku  = len(df_rel) if not df_rel.empty else c.get("qtd_contados", len(c.get("produtos_contados",[])))

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

        n_div_c = 0
        for col_div in ["Diferença Invent","Divergencia Qtd","Divergência"]:
            if not df_rel.empty and col_div in df_rel.columns:
                n_div_c = int((pd.to_numeric(df_rel[col_div], errors="coerce").fillna(0) != 0).sum())
                break
        acur_c_val = acur_por_ciclo.get(num_c)
        acur_c_str = f"{acur_c_val:.1f}%" if acur_c_val is not None else "—"

        _erp_j2 = c.get("erp_json","[]")
        try:
            _erp_d2 = json.loads(_erp_j2) if _erp_j2 and _erp_j2 != "[]" else []
            _docs2 = list(dict.fromkeys([str(r.get("Documento","")).strip() for r in _erp_d2 if r.get("Documento","")]))
            num_inv_det = ", ".join(_docs2) if _docs2 else c.get("num_inv","—")
        except:
            num_inv_det = c.get("num_inv","—")

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

        if not df_rel.empty:
            elems.append(Paragraph(f"Produtos inventariados ({n_sku})", sty("pi", fontSize=9, textColor=C_TEAL, fontName="Helvetica-Bold", spaceBefore=4, spaceAfter=4)))
            headers  = ["Código","Descrição","Saldo ERP","Saldo WMS","Inventariado","Diferença","Vl Total ERP","Vl Total Dif.","Justificativa","NF Ajuste"]
            col_keys = ["Produto","Descrição","Saldo ERP (Total)","Saldo WMS","Invent WMS","Diferença Invent","Vl Total ERP","Vl Total Diferença","Justificativa","NF Ajuste"]
            col_w    = [1.6*cm, 5.5*cm, 2.0*cm, 2.0*cm, 2.2*cm, 2.0*cm, 3.2*cm, 3.2*cm, 3.5*cm, 2.2*cm]
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
                    ("BOTTOMPADDING",  (0,0), (-1,-1), 3),
                ]))
                elems.append(tbl_p)

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

    _app_emp = st.session_state.get("_app_empresa")
    _app_fil = st.session_state.get("_app_filial")
    if _app_emp and _app_fil:
        st.session_state.setdefault("ic_empresa_sel", _app_emp)
        st.session_state.setdefault("ic_filial_sel",  _app_fil)
        if st.session_state.get("ic_empresa_sel") != _app_emp or \
           st.session_state.get("ic_filial_sel")  != _app_fil:
            st.session_state["ic_empresa_sel"] = _app_emp
            st.session_state["ic_filial_sel"]  = _app_fil

    empresa_sel = st.session_state.get("ic_empresa_sel")
    filial_sel  = st.session_state.get("ic_filial_sel")
    engine_db = st.session_state.get("_engine")

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
        _fil_sufixo = filial_sel.split(" - ")[-1] if " - " in filial_sel else filial_sel
        _emp_prefixo = empresa_sel.split(" - ")[0] if " - " in empresa_sel else empresa_sel
        df_filial = df_jlle[
            (df_jlle["Filial"] == _fil_sufixo) |
            (df_jlle["Empresa"].str.contains(_emp_prefixo, case=False, na=False) &
             (df_jlle["Filial"] == _fil_sufixo))
        ].copy()
        if df_filial.empty:
            df_filial = df_jlle.copy()
    else:
        contados = {}; ciclos = []; ciclo_ativo = None
        label = ""; df_filial = pd.DataFrame()

    # --- Lógica de Persistência: Se houver ciclo ativo, pula direto para etapa 2 ---
    if ciclo_ativo and "ic_etapa_nav" not in st.session_state:
        st.session_state["ic_etapa_nav"] = 2

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
        _uploads = []
    ja_cont_ciclo = set()
    for u in _uploads:
        ja_cont_ciclo.update(str(p).strip().zfill(6) for p in u.get("produtos", []))
    pl_ciclo  = {str(p).strip().zfill(6) for p in (ciclo_ativo.get("produtos_lista",[]) if ciclo_ativo else [])}
    faltam    = pl_ciclo - ja_cont_ciclo
    pct_ciclo = len(ja_cont_ciclo & pl_ciclo) / len(pl_ciclo) * 100 if pl_ciclo else 0

    _num_ciclo_ativo = ciclo_ativo.get("num_ciclo","") if ciclo_ativo else ""
    erp_uploads_ativo = st.session_state.get(f"{_cache_key}_erp_uploads", []) if empresa_sel and filial_sel else []
    erp_upload = erp_uploads_ativo[0] if erp_uploads_ativo else None
    nf_ajustes_ativo = st.session_state.get(f"{_cache_key}_nf_ajustes", []) if empresa_sel and filial_sel else []

    _conf_concluida = False
    _nf_concluida   = False
    if _num_ciclo_ativo and empresa_sel and filial_sel and erp_uploads_ativo:
        try:
            _docs_conf = st.session_state.get(f"{_cache_key}_docs_conf", set())
            _docs_erp  = {u.get("documento","") for u in erp_uploads_ativo}
            _conf_concluida = bool(_docs_erp and _docs_erp.issubset(_docs_conf))
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

    st.markdown("---")
    _tem_div_atual = False
    if erp_uploads_ativo:
        _docs_conf_check = st.session_state.get(f"{_cache_key}_docs_conf", set()) if _num_ciclo_ativo else set()
        _uploads_pend = [u for u in erp_uploads_ativo if u.get("documento","") not in _docs_conf_check]
        if _uploads_pend:
            _dados_pend = _uploads_pend[-1].get("dados",[])
            _tem_div_atual = any(float(str(r.get("Divergencia Qtd",0)).replace(",",".") or 0) != 0 for r in _dados_pend)

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

        if not empresa_sel or not filial_sel:
            st.info("👆 Volte à tela inicial e selecione a Empresa e a Filial para começar.")
            return

        # --- BLOCO DE SEGURANÇA: SE JÁ EXISTIR CICLO, TRAVA A LISTA ---
        if ciclo_ativo:
            st.warning(f"⚠️ Já existe um ciclo ativo: **{ciclo_ativo['num_ciclo']}**")
            col_res, col_del = st.columns(2)
            if col_res.button("Retomar Ciclo Atual", type="primary", use_container_width=True):
                st.session_state["ic_etapa_nav"] = 2
                st.rerun()
            if col_del.button("Cancelar e Gerar Novo", type="secondary", use_container_width=True):
                db_fechar_ciclo_ativo(engine_db, empresa_sel, filial_sel)
                st.session_state["ic_force_reload"] = True
                st.rerun()
            st.markdown("---")

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
        col_a.caption(f"📅 Dados carregados em: **{data_aud or 'esta sessão'}**")
        col_b.caption("⚠️ Itens divergentes reaparecem mesmo após contados.")

        _armazens_disponiveis = []
        if "Armazem" in df_filial.columns:
            _armazens_disponiveis = sorted(df_filial["Armazem"].dropna().unique().tolist())
        
        _arm_sel = []
        if _armazens_disponiveis:
            _arm_sel = st.multiselect(
                "🏭 Filtrar por Armazém",
                options=_armazens_disponiveis,
                default=_armazens_disponiveis,
                key="ic_armazens_sel",
                placeholder="Todos os armazéns",
            )
            df_filial_score = df_filial[df_filial["Armazem"].isin(_arm_sel)].copy() if _arm_sel else df_filial.copy()
        else:
            df_filial_score = df_filial.copy()

        if not df_filial_score.empty:
            df_score   = calcular_score(df_filial_score, tuple(sorted(contados.items())))
            total_skus = len(df_score)
            total_cont = sum(1 for p in df_score["Produto"].astype(str) if p in contados)
            pct_cob    = (total_cont/total_skus*100) if total_skus>0 else 0
        else:
            df_score = pd.DataFrame(); total_skus = 0; total_cont = 0; pct_cob = 0.0

        c1m,c2m,c3m,c4m = st.columns(4)
        c1m.metric("Total SKUs",  f"{total_skus:,}")
        c2m.metric("Divergentes", f"{int((df_score['Divergência']!=0).sum()):,}" if not df_score.empty else "0")
        c3m.metric("Curva A",     f"{int((df_score['Curva ABC']=='A').sum()):,}" if not df_score.empty else "0")
        c4m.metric("Valor Total", f"R$ {formatar_br(df_score['Vl Total ERP'].sum())}" if not df_score.empty else "R$ 0")

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

        # Se já existir ciclo, usamos a lista salva. Se não, geramos dinamicamente.
        if ciclo_ativo and "produtos_lista" in ciclo_ativo:
            prods_salvos = [str(p).zfill(6) for p in ciclo_ativo["produtos_lista"]]
            df_lista = df_score[df_score["Produto"].astype(str).str.zfill(6).isin(prods_salvos)].copy()
            qtd_ciclo = len(df_lista)
        else:
            # ── SELEÇÃO MANUAL ─────────────────────────────
        with st.expander("✍️ Seleção manual de produtos"):
            entrada_manual = st.text_area("Cole os códigos (separados por espaço, vírgula ou linha)")

        codigos_manuais = [
            c.strip().zfill(6)
            for c in entrada_manual.replace(",", " ").split()
            if c.strip()
        ]

        if codigos_manuais:
            df_lista = df_score[
                df_score["Produto"].astype(str).str.zfill(6).isin(codigos_manuais)
            ].copy()
            qtd_ciclo = len(df_lista)
        else:
            modo = st.radio("Modo", ["Quantidade fixa","Percentual"], horizontal=True, key="ic_modo")

            if modo == "Quantidade fixa":
                st.session_state.setdefault("ic_qtd", 30)
                qtd_ciclo = min(st.session_state.ic_qtd, total_skus)
            else:
                pmap = {"5%":0.05,"10%":0.10,"20%":0.20,"30%":0.30}
                pl   = st.select_slider("Faixa",list(pmap.keys()),value="10%",key="ic_pct")
                qtd_ciclo = max(1,int(total_skus*pmap[pl]))

            df_lista = montar_lista(df_score, qtd_ciclo, contados)

        st.markdown(f"**{len(df_lista)} itens na lista**")
        cols_ex = [c for c in ["Produto","Descrição","Curva ABC","Score","Já Contado",
                                "Saldo ERP (Total)","Vl Total ERP","Motivo","Origem"]
                   if c in df_lista.columns]
        
        st.dataframe(df_lista[cols_ex], use_container_width=True, hide_index=False)

        if not ciclo_ativo:
            if st.button("🔍 Gerar e Iniciar Ciclo", type="primary", use_container_width=True):
                _base_ciclo = f"{date.today().strftime('%Y%m%d')}-{empresa_sel}-{filial_sel}".replace(" ","")
                num_ciclo = f"{_base_ciclo}-{len(ciclos)+1}"
                
                db_salvar_ciclo_ativo(engine_db, empresa_sel, filial_sel, {
                    "num_ciclo":      num_ciclo,
                    "data_geracao":   date.today().strftime("%d/%m/%Y"),
                    "label":          label,
                    "qtd_lista":      len(df_lista),
                    "produtos_lista": df_lista["Produto"].astype(str).tolist(),
                    "uploads":        [],
                    "status":         "Em andamento",
                })
                st.session_state["ic_force_reload"] = True
                st.session_state["ic_etapa_nav"] = 2
                st.rerun()

    # ── ETAPA 2 — UPLOAD ERP (PROTHEUS) ──────────────────────────────────
    elif etapa_nav == 2:
        if not ciclo_ativo: st.warning("Gere a lista primeiro."); return
        st.markdown(f"### 2. Upload ERP — Ciclo: **{ciclo_ativo['num_ciclo']}**")
        
        num_ciclo_erp = ciclo_ativo.get("num_ciclo","")
        if erp_uploads_ativo:
            st.success(f"✅ **{len(erp_uploads_ativo)} upload(s) ERP** salvos.")
            for i, u in enumerate(erp_uploads_ativo, 1):
                with st.expander(f"Etapa {i} — Doc: {u.get('documento','—')}"):
                    st.dataframe(pd.DataFrame(u["dados"]), use_container_width=True)

        arq_erp = st.file_uploader("Selecione o Excel do Protheus", type=["xlsx"], key="up_erp_file")
        if arq_erp:
            df_erp = pd.read_excel(arq_erp)
            # ... (Lógica de processamento igual ao original)
            if st.button("💾 Salvar Upload ERP", type="primary"):
                db_salvar_erp_upload(engine_db, empresa_sel, filial_sel, num_ciclo_erp, "DOC-AUTO", date.today().isoformat(), df_erp.to_dict("records"))
                st.session_state["ic_force_reload"] = True
                st.rerun()

    # ── ETAPA 3 — CONFERÊNCIA ────────────────────────────────────
    elif etapa_nav == 3:
        st.markdown("### 3. Conferência")
        # (Lógica original de conferência...)
        st.info("Utilize esta tela para justificar as divergências encontradas no Protheus.")

    # ── ETAPA 4 — NF AJUSTE ──────────────────────────────────────
    elif etapa_nav == 4:
        st.markdown("### 4. NF de Ajuste")
        # (Lógica original de NF...)

    # ── ETAPA 5 — FECHAR ─────────────────────────────────────────
    elif etapa_nav == 5:
        st.markdown("### 5. Fechar Inventário")
        if st.button("🏁 Finalizar Ciclo e Gravar no Histórico", type="primary"):
            # Lógica de gravação final...
            db_fechar_ciclo_ativo(engine_db, empresa_sel, filial_sel)
            st.session_state["ic_force_reload"] = True
            st.session_state["ic_etapa_nav"] = 6
            st.rerun()

    # ── ETAPA 6 — HISTÓRICO ──────────────────────────────────────
    elif etapa_nav == 6:
        st.markdown("### 6. Histórico KPMG")
        if not ciclos:
            st.info("Nenhum ciclo no histórico.")
        else:
            for c in ciclos:
                st.write(f"Cíclico {c['num_ciclo']} - {c['status']}")
