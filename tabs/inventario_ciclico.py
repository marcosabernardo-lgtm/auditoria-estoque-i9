import streamlit as st
import pandas as pd
import numpy as np
import io
import re
import json
import pdfplumber
from collections import defaultdict
from datetime import date, datetime
from sqlalchemy import text 
from fpdf import FPDF

from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import (SimpleDocTemplate, Table, TableStyle,
                                Paragraph, Spacer, HRFlowable, PageBreak)
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT

from inventario_db import (
    db_salvar_ciclo_ativo, db_fechar_ciclo_ativo, db_carregar_tudo,
    db_salvar_erp_upload, db_marcar_contados, db_remover_erp_uploads,
    db_cancelar_ciclo_ativo, db_excluir_ciclo_historico,
    db_salvar_justificativas, db_salvar_nf_ajuste,
    db_obter_nf_ajustes, db_obter_justificativas,
    db_gerar_num_ciclo
)

PERIODO_KPMG_DIAS = 365

def _resetar_estado_ciclo(cache_key: str):
    """Limpa o estado de sessão do inventário cíclico antes de iniciar um novo ciclo."""
    for chave in list(st.session_state.keys()):
        if chave.startswith("ic_"):
            st.session_state.pop(chave, None)
    st.session_state.pop(cache_key, None)
    st.session_state["ic_force_reload"] = True


def _pdf_para_bytes(pdf: FPDF) -> bytes:
    try:
        resultado = pdf.output(dest="S")
        if isinstance(resultado, str):
            return resultado.encode("latin-1")
        return bytes(resultado)
    except TypeError:
        resultado = pdf.output()
        if isinstance(resultado, (bytes, bytearray)):
            return bytes(resultado)
        return resultado.encode("latin-1")

class KPMG_Report(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(0, 85, 98) 
        title = "RELATÓRIO DE AUDITORIA DE ESTOQUE - PADRÃO KPMG".encode('latin-1', 'replace').decode('latin-1')
        self.cell(0, 10, title, 0, 1, "R")
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        info = f"Página {self.page_no()} | Gerado via Portal I9 em {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        self.cell(0, 10, info.encode('latin-1', 'replace').decode('latin-1'), 0, 0, "C")

    def capa_resumo(self, metrics):
        self.add_page()
        self.set_font("Helvetica", "B", 22)
        self.set_text_color(0, 51, 102)
        self.ln(20)
        self.cell(0, 15, "RESUMO EXECUTIVO".encode('latin-1', 'replace').decode('latin-1'), 0, 1, "L")
        self.set_draw_color(0, 85, 98)
        self.line(10, 48, 200, 48)
        self.ln(10)
        items = [
            ("SKUs Contados", metrics['skus']),
            ("Cobertura KPMG", f"{metrics['cobertura']:.2f}%"),
            ("Acuracidade Média", f"{metrics['acuracidade']:.2f}%"),
            ("Ciclos Realizados", metrics['ciclos']),
            ("Status de Conformidade", metrics['status'])
        ]
        for label, val in items:
            self.set_font("Helvetica", "B", 12)
            self.set_text_color(0, 0, 0)
            self.cell(60, 12, label.encode('latin-1', 'replace').decode('latin-1') + ":", 0, 0)
            self.set_font("Helvetica", "", 12)
            if label == "Status de Conformidade":
                color = (39, 174, 96) if val == "CUMPRIDA" else (231, 76, 60)
                self.set_text_color(*color)
                self.set_font("Helvetica", "B", 12)
            self.cell(0, 12, str(val).encode('latin-1', 'replace').decode('latin-1'), 0, 1)

    def lista_ciclos_page(self, df_ciclos):
        self.add_page()
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(0, 51, 102)
        self.cell(0, 10, "4.2 LISTA DE CICLOS", 0, 1, "L")
        self.ln(5)
        self.set_font("Helvetica", "B", 8)
        self.set_fill_color(240, 240, 240)
        self.set_text_color(0, 0, 0)
        cols = ["Nº Ciclo", "Data", "Responsável", "Nº Inv", "SKUs", "Div.", "Cobert.", "Acurácia"]
        w = [35, 25, 30, 20, 20, 20, 20, 20]
        for i, col in enumerate(cols):
            self.cell(w[i], 8, col.encode('latin-1', 'replace').decode('latin-1'), 1, 0, "C", True)
        self.ln()
        self.set_font("Helvetica", "", 8)
        for _, row in df_ciclos.iterrows():
            self.cell(w[0], 7, str(row['Nº Ciclo']).encode('latin-1', 'replace').decode('latin-1'), 1)
            self.cell(w[1], 7, str(row['Data']).encode('latin-1', 'replace').decode('latin-1'), 1, 0, "C")
            self.cell(w[2], 7, str(row['Responsável']).encode('latin-1', 'replace').decode('latin-1'), 1)
            self.cell(w[3], 7, str(row['Nº Inv']).encode('latin-1', 'replace').decode('latin-1'), 1, 0, "C")
            self.cell(w[4], 7, str(row['SKUs']), 1, 0, "C")
            self.cell(w[5], 7, str(row['Div']), 1, 0, "C")
            self.cell(w[6], 7, f"{row['Cobert']}%", 1, 0, "C")
            self.cell(w[7], 7, f"{row['Acuracia']}%", 1, 0, "C")
            self.ln()

    def detalhe_ciclo_page(self, df_itens):
        self.add_page(orientation="L") 
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(0, 51, 102)
        self.cell(0, 10, "4.3 DETALHAMENTO DE PRODUTOS POR CICLO".encode('latin-1', 'replace').decode('latin-1'), 0, 1, "L")
        self.ln(5)
        self.set_font("Helvetica", "B", 7)
        self.set_fill_color(0, 85, 98)
        self.set_text_color(255, 255, 255)
        cols = ["Código", "Descrição", "Saldo ERP", "Saldo WMS", "Inventariado", "Diferença", "Vl Total ERP", "Vl Total Dif", "Justificativa", "NF"]
        w = [15, 65, 15, 15, 15, 15, 22, 22, 60, 20]
        for i, col in enumerate(cols):
            self.cell(w[i], 8, col.encode('latin-1', 'replace').decode('latin-1'), 1, 0, "C", True)
        self.ln()
        self.set_font("Helvetica", "", 7)
        self.set_text_color(0, 0, 0)
        for _, row in df_itens.iterrows():
            vl_erp = float(row['Vl Total ERP']) if pd.notna(row['Vl Total ERP']) else 0.0
            vl_dif = float(row['Vl Total Dif']) if pd.notna(row['Vl Total Dif']) else 0.0
            self.cell(w[0], 6, str(row['Codigo']), 1)
            self.cell(w[1], 6, str(row['Descricao']).encode('latin-1', 'replace').decode('latin-1')[:45], 1)
            self.cell(w[2], 6, str(row['Saldo ERP']), 1, 0, "R")
            self.cell(w[3], 6, str(row['Saldo WMS']), 1, 0, "R")
            self.cell(w[4], 6, str(row['Inventariado']), 1, 0, "R")
            self.cell(w[5], 6, str(row['Diferenca']), 1, 0, "R")
            self.cell(w[6], 6, f"{vl_erp:,.2f}", 1, 0, "R")
            self.cell(w[7], 6, f"{vl_dif:,.2f}", 1, 0, "R")
            just = str(row['Justificativa']).encode('latin-1', 'replace').decode('latin-1')
            self.cell(w[8], 6, just, 1)
            self.cell(w[9], 6, str(row['NF']), 1, 0, "C")
            self.ln()

# ── PARSER DANFE ────────────────────────────────────────────────────────────
def parsear_nf_danfe(arquivo_bytes):
    result = {"num_nf":"","data":"","natureza":"","itens":[]}
    try:
        with pdfplumber.open(io.BytesIO(arquivo_bytes)) as pdf:
            text_pdf = "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception as e:
        return result, str(e)
    nums = re.findall(r'N\.\s*0*(\d+)', text_pdf)
    if nums: result["num_nf"] = nums[0].lstrip('0')
    m_data = re.search(r'DATA DE EMISS[ÃA]O\s*\n?\s*(\d{2}/\d{2}/\d{4})', text_pdf)
    if m_data: result["data"] = m_data.group(1)
    m_nat = re.search(r'NATUREZA DA OPERA[ÇC][ÃA]O\s*\n\s*(.+?)(?:\s+PROTOCOLO|\n)', text_pdf)
    if m_nat: result["natureza"] = m_nat.group(1).strip()
    padrao = re.compile(r'(\d{6})\s+(.+?)\s+\d{8}\s+\d{3}\s+\d{4}\s+\w+\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)', re.MULTILINE)
    for m in padrao.finditer(text_pdf):
        result["itens"].append({
            "Codigo": m.group(1), "Descricao": m.group(2).strip(),
            "Qtd": float(m.group(3).replace(".","").replace(",",".")),
            "Vl Unit": float(m.group(4).replace(".","").replace(",",".")),
            "Vl Total": float(m.group(5).replace(".","").replace(",",".")),
        })
    return result, None

# ── COMPONENTES VISUAIS E SCORE ─────────────────────────────────────────────
def _card(col, num, titulo, ativo, concluido, chave):
    if concluido:
        brd, bg, icon, badge, bbg, txt = ("#27AE60", "#E8F5E9", "✓", "Concluído", "#27AE60", "#1E5631")
    elif ativo:
        brd, bg, icon, badge, bbg, txt = ("#005562", "#E1F5EE", str(num), "Ativo", "#005562", "#00333d")
    else:
        brd, bg, icon, badge, bbg, txt = ("#D1D1D1", "#F9F9F9", str(num), "Pendente", "#777", "#555")
    with col:
        st.markdown(f"""
            <div style="border:2px solid {brd}; border-radius:12px; padding:12px; background:{bg}; min-height:100px;">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <div style="width:28px; height:28px; border-radius:50%; background:{brd}; display:flex; align-items:center; justify-content:center; color:#fff; font-weight:bold;">{icon}</div>
                    <span style="background:{bbg}; color:#fff; font-size:10px; padding:2px 8px; border-radius:10px; font-weight:bold;">{badge}</span>
                </div>
                <div style="color:{txt}; font-weight:800; font-size:14px; margin-top:10px; text-transform:uppercase;">{titulo}</div>
            </div>
        """, unsafe_allow_html=True)
        return st.button(f"Abrir Etapa {num}", key=chave, use_container_width=True)

@st.cache_data(ttl=600)
def calcular_score_turbo(df, contados_tuple):
    contados = dict(contados_tuple)
    df = df.copy()
    for col in ["Saldo WMS", "Saldo ERP (Total)", "Vl Unit", "Vl Total ERP"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "Produto" in df.columns:
        df["Produto"] = df["Produto"].astype(str).str.zfill(6)

    desc_cols = [c for c in df.columns if "Descr" in str(c)]
    desc_col = desc_cols[0] if desc_cols else None
    if desc_col and desc_col != "Descrição":
        df = df.rename(columns={desc_col: "Descrição"})
        desc_col = "Descrição"

    if "Produto" in df.columns and len(df) > df["Produto"].nunique():
        agg_map = {}
        if desc_col:
            agg_map[desc_col] = (desc_col, "first")
        if "Armazem" in df.columns:
            agg_map["Armazem"] = ("Armazem", "first")
        if "Saldo WMS" in df.columns:
            agg_map["Saldo WMS"] = ("Saldo WMS", "sum")
        if "Saldo ERP (Total)" in df.columns:
            agg_map["Saldo ERP (Total)"] = ("Saldo ERP (Total)", "max")
        if "Vl Unit" in df.columns:
            agg_map["Vl Unit"] = ("Vl Unit", "first")
        if "Vl Total ERP" in df.columns:
            agg_map["Vl Total ERP"] = ("Vl Total ERP", "max")

        df = df.groupby("Produto", as_index=False).agg(**agg_map)

    df = df.sort_values("Vl Total ERP", ascending=False).reset_index(drop=True)
    tv = df["Vl Total ERP"].sum()
    df["pct_acum"]  = df["Vl Total ERP"].cumsum() / tv if tv > 0 else 0
    df["Curva ABC"] = np.where(df["pct_acum"]<=0.80,"A", np.where(df["pct_acum"]<=0.95,"B","C"))
    hoje = date.today()
    df["Dias s/ Contagem"] = df["Produto"].astype(str).apply(lambda p: (hoje - date.fromisoformat(contados[p])).days if p in contados else PERIODO_KPMG_DIAS)
    df["Score"] = (df["Dias s/ Contagem"] / PERIODO_KPMG_DIAS * 10).round(2)
    df["Já Contado"] = df["Produto"].astype(str).apply(lambda p: f"✅ {contados[p]}" if p in contados else "⬜ Não")

    def motivo(r):
        rs = []
        if r["Curva ABC"] == "A":
            rs.append("Curva A")
        if r.get("Status", "") == "Divergente":
            rs.append("Divergência")
        if r["Dias s/ Contagem"] >= PERIODO_KPMG_DIAS:
            rs.append("Nunca contado")
        elif r["Dias s/ Contagem"] > 180:
            rs.append(f"{r['Dias s/ Contagem']}d sem contar")
        if r["Vl Total ERP"] > 0:
            rs.append(f"R$ {r['Vl Total ERP']:,.0f}")
        return " · ".join(rs) if rs else "Em estoque"

    df["Motivo"] = df.apply(motivo, axis=1)
    df = df.sort_values("Score", ascending=False).reset_index(drop=True)
    return df

# ── FUNÇÕES DO RELATÓRIO FINAL (PADRÃO MAIN) ─────────────────────────────────

def montar_df_relatorio(uploads, df_filial):
    if not uploads or df_filial is None or df_filial.empty:
        return pd.DataFrame()

    desc_col = next((c for c in df_filial.columns if "Descr" in str(c)), None)
    erp_cols = [c for c in ["Produto", "Vl Unit"] if c in df_filial.columns]
    if desc_col:
        erp_cols.insert(1, desc_col)

    df_erp = df_filial[erp_cols].copy()
    if "Vl Unit" in df_erp.columns:
        df_erp["Vl Unit"] = pd.to_numeric(df_erp["Vl Unit"], errors="coerce").fillna(0)
    df_erp["Produto"] = df_erp["Produto"].astype(str).str.zfill(6)

    agg_map = {}
    if desc_col:
        agg_map[desc_col] = (desc_col, "first")
    if "Vl Unit" in df_erp.columns:
        agg_map["Vl Unit"] = ("Vl Unit", "first")
    df_erp = df_erp.groupby("Produto", as_index=False).agg(**agg_map)
    if desc_col and desc_col != "Descrição":
        df_erp = df_erp.rename(columns={desc_col: "Descrição"})

    rows = []
    for u in uploads:
        dados = u.get("dados", [])
        if isinstance(dados, list) and dados:
            rows.extend(dados)
            continue

        df_rows = u.get("df_rows", [])
        if isinstance(df_rows, list) and df_rows:
            rows.extend(df_rows)
    if not rows:
        return pd.DataFrame()

    df_wms_all = pd.DataFrame(rows)
    if "Codigo" not in df_wms_all.columns and "Código" in df_wms_all.columns:
        df_wms_all = df_wms_all.rename(columns={"Código": "Codigo"})
    df_wms_all["Codigo"] = df_wms_all["Codigo"].astype(str).str.zfill(6)

    if "Saldo WMS" not in df_wms_all.columns and "Qtd WMS" in df_wms_all.columns:
        df_wms_all = df_wms_all.rename(columns={"Qtd WMS": "Saldo WMS"})
    if "Saldo ERP (Total)" not in df_wms_all.columns and "Qtd ERP" in df_wms_all.columns:
        df_wms_all = df_wms_all.rename(columns={"Qtd ERP": "Saldo ERP (Total)"})
    if "Saldo WMS" not in df_wms_all.columns and "Qtd Antes" in df_wms_all.columns:
        df_wms_all = df_wms_all.rename(columns={"Qtd Antes": "Saldo WMS"})
    if "Saldo ERP (Total)" not in df_wms_all.columns and "Qtd Depois" in df_wms_all.columns:
        df_wms_all = df_wms_all.rename(columns={"Qtd Depois": "Saldo ERP (Total)"})
    if "Diferença Invent" not in df_wms_all.columns and "Divergencia Qtd" in df_wms_all.columns:
        df_wms_all = df_wms_all.rename(columns={"Divergencia Qtd": "Diferença Invent"})
    if "Vl Total Diferença" not in df_wms_all.columns and "Divergencia Valor" in df_wms_all.columns:
        df_wms_all = df_wms_all.rename(columns={"Divergencia Valor": "Vl Total Diferença"})
    if "Vl Total Diferença" not in df_wms_all.columns and "Diferenca Valor" in df_wms_all.columns:
        df_wms_all = df_wms_all.rename(columns={"Diferenca Valor": "Vl Total Diferença"})

    for col in ["Saldo ERP (Total)", "Saldo WMS", "Diferença Invent", "Vl Total Diferença"]:
        if col in df_wms_all.columns:
            df_wms_all[col] = pd.to_numeric(df_wms_all[col], errors="coerce").fillna(0)

    df_wms_ult = df_wms_all.drop_duplicates(subset=["Codigo"], keep="last")
    merge_cols = ["Codigo"] + [c for c in ["Saldo ERP (Total)", "Saldo WMS", "Diferença Invent", "Vl Total Diferença", "Acuracidade"] if c in df_wms_ult.columns]

    df_rel = df_wms_ult[merge_cols].rename(columns={"Codigo": "Produto"}).merge(
        df_erp,
        on="Produto", how="left"
    )
    df_rel["Saldo ERP (Total)"] = pd.to_numeric(df_rel.get("Saldo ERP (Total)"), errors="coerce").fillna(0)
    df_rel["Saldo WMS"] = pd.to_numeric(df_rel.get("Saldo WMS"), errors="coerce").fillna(0)
    df_rel["Acuracidade"] = df_rel["Acuracidade"].fillna("—") if "Acuracidade" in df_rel.columns else "—"

    saldo_erp = pd.to_numeric(df_rel["Saldo ERP (Total)"], errors="coerce").fillna(0)
    vl_unit = pd.to_numeric(df_rel.get("Vl Unit"), errors="coerce").fillna(0)

    df_rel["Diferença Invent"] = df_rel["Saldo WMS"] - saldo_erp
    df_rel["Acuracidade"] = np.where(saldo_erp != 0, (df_rel["Saldo WMS"] / saldo_erp) * 100, np.where(df_rel["Saldo WMS"] == 0, 100, 0))
    df_rel["Vl Total ERP"] = saldo_erp * vl_unit
    if "Vl Total Diferença" not in df_rel.columns:
        df_rel["Vl Total Diferença"] = df_rel["Diferença Invent"] * vl_unit

    cols_saida = [c for c in [
        "Produto", "Descrição",
        "Saldo ERP (Total)", "Saldo WMS",
        "Diferença Invent", "Acuracidade",
        "Vl Total ERP", "Vl Total Diferença"
    ] if c in df_rel.columns]
    return df_rel[cols_saida].sort_values("Vl Total ERP", ascending=False).reset_index(drop=True)

def gerar_pdf_kpmg(ciclo, df_rel, empresa, filial):
    return gerar_pdf_kpmg_consolidado(
        [ciclo],
        {ciclo.get("num_ciclo", ""): df_rel},
        empresa,
        filial,
    )


def gerar_pdf_kpmg_consolidado(ciclos_sel, dfs_rel, empresa, filial, total_catalogo=0):
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

    # Cobertura acumulada: união dos SKUs contados em todos os ciclos selecionados / total catálogo
    skus_contados_union = set()
    for df_c in dfs_rel.values():
        if not df_c.empty and "Produto" in df_c.columns:
            skus_contados_union.update(df_c["Produto"].astype(str).tolist())
    cobertura_max = (len(skus_contados_union) / total_catalogo * 100) if total_catalogo else 0

    n_ciclos = len(ciclos_sel)

    # ── CAPA ──
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

    # ── KPIs ──
    elems.append(Paragraph("Resumo Executivo", s_sec))
    elems.append(HRFlowable(width="100%", thickness=1, color=C_ORANGE))
    elems.append(Spacer(1, 0.3*cm))
    kpi_labels = ["SKUs Contados", "Cobertura KPMG", "Acuracidade Média", "Ciclos Realizados"]
    kpi_values = [str(total_skus_cont), f"{cobertura_max:.1f}%", acur_media, str(n_ciclos)]
    kpi_row_l  = [Paragraph(l, s_kpi_lbl) for l in kpi_labels]
    kpi_row_v  = [Paragraph(v, s_kpi_val) for v in kpi_values]
    kpi_t = Table([kpi_row_l, kpi_row_v], colWidths=[4*cm]*4)
    kpi_t.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), C_TEAL),
        ("BOX",           (0,0), (-1,-1), 0.5, C_DARK),
        ("INNERGRID",     (0,0), (-1,-1), 0.3, C_DARK),
        ("TOPPADDING",    (0,0), (-1,-1), 6),
        ("BOTTOMPADDING", (0,0), (-1,-1), 6),
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

    # ── LISTA DE CICLOS ──
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
        _erp_data = c.get("uploads", []) or []
        _docs_map = defaultdict(list)
        for upload in _erp_data:
            doc_num = str(upload.get("documento", "—")).strip() or "—"
            for item in upload.get("dados", []):
                row = dict(item)
                row["Documento"] = doc_num
                _docs_map[doc_num].append(row)

        if not _docs_map:
            n_sku = len(df_c) if not df_c.empty else c.get("qtd_contados", len(c.get("produtos_contados", [])))
            acur_c = acur_por_ciclo.get(num_c)
            acur_str = f"{acur_c:.1f}%" if acur_c is not None else "—"
            rows_ciclos.append([
                Paragraph(str(i),                          s_cell_c),
                Paragraph(num_c,                           s_cell),
                Paragraph(c.get("data","—"),           s_cell_c),
                Paragraph(c.get("responsavel","—"),    s_cell),
                Paragraph("—",                           s_cell_c),
                Paragraph(str(n_sku),                    s_cell_c),
                Paragraph("—",                           s_cell_c),
                Paragraph(f"{c.get('cobertura_pct',0):.1f}%", s_cell_c),
                Paragraph(acur_str,                      s_cell_c),
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

    # ── DETALHE POR CICLO ──
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
                if p not in seen: seen.append(p)
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

        uploads_c = c.get("uploads", [])
        docs_upload = list(dict.fromkeys([str(u.get("documento", "")).strip() for u in uploads_c if u.get("documento", "")]))
        datas_upload = [str(u.get("data_upload", "")).strip() for u in uploads_c if u.get("data_upload", "")]
        data_cont_det = datas_upload[0] if datas_upload else c.get("data_fechamento", c.get("data_geracao", "—"))
        num_inv_det = ", ".join(docs_upload) if docs_upload else c.get("num_inv", "—")

        meta_data = [
            [Paragraph("Data da contagem", s_det_label), Paragraph(data_cont_det, s_det_val),
             Paragraph("Nº Inventário", s_det_label), Paragraph(num_inv_det, s_det_val)],
            [Paragraph("Acuracidade", s_det_label), Paragraph(acur_c_str, s_det_val),
             Paragraph("SKUs contados", s_det_label), Paragraph(str(n_sku), s_det_val)],
            [Paragraph("SKUs divergentes", s_det_label), Paragraph(str(n_div_c), s_det_val),
             Paragraph("", s_det_label), Paragraph("", s_det_val)],
        ]
        tbl_meta = Table(meta_data, colWidths=[3.2*cm, 5.0*cm, 3.0*cm, 4.6*cm])
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
            elems.append(Paragraph(f"Produtos inventariados ({n_sku})",
                sty("pi", fontSize=9, textColor=C_TEAL, fontName="Helvetica-Bold", spaceBefore=4, spaceAfter=4)))
            headers  = ["Código","Descrição","Saldo ERP","Saldo WMS","Diferença","Acurac.","Vl Total ERP","Vl Total Dif.","Justificativa","NF Ajuste"]
            col_keys = ["Produto","Descrição","Saldo ERP (Total)","Saldo WMS","Diferença Invent","Acuracidade","Vl Total ERP","Vl Total Diferença","Justificativa","NF Ajuste"]
            col_w    = [1.6*cm, 5.3*cm, 1.8*cm, 1.8*cm, 1.8*cm, 1.7*cm, 2.8*cm, 2.8*cm, 3.2*cm, 2.0*cm]
            _justs_pdf = c.get("_justs_pdf", {})
            _nfs_pdf   = c.get("_nfs_pdf", {})

            tbl_data = [[Paragraph(h, s_cellh) for h in headers]]
            for _, row in df_rel.iterrows():
                r = []
                for k in col_keys:
                    v = row.get(k, "—")
                    if k in ["Saldo ERP (Total)","Saldo WMS"]:
                        try:    r.append(Paragraph(f"{float(v):,.2f}", s_num))
                        except: r.append(Paragraph("—", s_num))
                    elif k == "Diferença Invent":
                        try:
                            fv = float(v)
                            txt = f"{fv:,.2f}"
                            r.append(Paragraph(txt, sty("dif", fontSize=8, alignment=TA_RIGHT,
                                               textColor=C_RED if fv < 0 else (C_GREEN if fv > 0 else colors.black), leading=10)))
                        except: r.append(Paragraph("—", s_num))
                    elif k == "Acuracidade":
                        try:
                            r.append(Paragraph(f"{float(v):,.1f}%", s_num))
                        except: r.append(Paragraph("—", s_num))
                    elif k in ["Vl Total ERP","Vl Total Diferença"]:
                        try:
                            fv = float(v)
                            if k == "Vl Total Diferença" and fv != 0:
                                txt = f"R$ {fv:,.2f}"
                                r.append(Paragraph(txt, sty("vdif", fontSize=8, alignment=TA_RIGHT,
                                                   textColor=C_RED if fv < 0 else C_GREEN, leading=10)))
                            else:
                                r.append(Paragraph(f"R$ {fv:,.2f}", s_num))
                        except: r.append(Paragraph("—", s_num))
                    elif k == "Produto":
                        r.append(Paragraph(str(v).zfill(6), s_cell))
                    elif k == "Justificativa":
                        cod  = str(row.get("Produto","")).zfill(6)
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
                    ("BOTTOMPADDING", (0,0), (-1,-1), 3),
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


# ── RENDERIZAÇÃO PRINCIPAL ───────────────────────────────────────────────────
def render(df_jlle, df_outras, formatar_br):
    engine = st.session_state.get("_engine")
    empresa = st.session_state.get("_app_empresa")
    filial = st.session_state.get("_app_filial")
    
    if not engine:
        st.warning("Banco de dados não conectado.")
        return

    _cache_key = f"st_data_{empresa}_{filial}"
    if _cache_key not in st.session_state or st.session_state.get("ic_force_reload"):
        st.session_state[_cache_key] = db_carregar_tudo(engine, empresa, filial)
        st.session_state["ic_force_reload"] = False
    
    data = st.session_state[_cache_key]
    ciclo_ativo = data.get("ciclo_ativo")
    erp_data = data.get("erp_uploads", [])
    justs_salvas = data.get("justs", {})
    
    num_c = ciclo_ativo['num_ciclo'] if ciclo_ativo else ""
    nf_ajustes = db_obter_nf_ajustes(engine, empresa, filial, num_c) if num_c else {}

    if "ic_etapa_nav" not in st.session_state:
        if not ciclo_ativo:
            st.session_state["ic_etapa_nav"] = 1
        elif not erp_data:
            st.session_state["ic_etapa_nav"] = 2
        elif not justs_salvas:
            st.session_state["ic_etapa_nav"] = 3
        else:
            st.session_state["ic_etapa_nav"] = 5
    
    etapa = st.session_state["ic_etapa_nav"]
    if ciclo_ativo:
        if not erp_data and etapa < 2:
            etapa = 2
        elif erp_data and not justs_salvas and etapa < 3:
            etapa = 3
        elif justs_salvas and not nf_ajustes and etapa < 4:
            etapa = 4
        elif nf_ajustes and etapa < 5:
            etapa = 5
        st.session_state["ic_etapa_nav"] = etapa

    st.markdown("### Fluxo de Inventário")
    cols = st.columns(6)
    steps = ["Gerar Lista", "Upload ERP", "Conferência", "NF Ajuste", "Fechar", "Histórico"]

    for i, name in enumerate(steps, 1):
        if etapa == 6:
            done = (i < 6)
        else:
            if i == 1: done = bool(ciclo_ativo)
            elif i == 2: done = bool(erp_data)
            elif i == 3: done = bool(justs_salvas) or (etapa > 3)
            elif i == 4: done = bool(nf_ajustes) or (etapa > 4)
            elif i == 5: done = (not ciclo_ativo and etapa >= 5)
            else: done = False

        if _card(cols[i-1], i, name, etapa == i, done, f"nv_{i}"):
            st.session_state["ic_etapa_nav"] = i
            st.rerun()
    st.divider()

    # ── ETAPA 1 ──────────────────────────────────────────────────────────
    if etapa == 1:
        st.subheader("1. Geração da Lista de Contagem")
        df_score = calcular_score_turbo(df_jlle, tuple(sorted(data["contados"].items())))
        
        # ── SELEÇÃO MANUAL SEM DEPENDER DE ELSE ─────────────────────────
        with st.expander("⚙️ Seleção Manual"):
            entrada_manual = st.text_area("Insira os códigos dos produtos")
            codigos_manuais = [
                c.strip().zfill(6)
                for c in entrada_manual.replace(",", " ").split()
                if c.strip()
            ]
        
        if ciclo_ativo:
            st.warning(f"⚠️ Ciclo Ativo: **{ciclo_ativo['num_ciclo']}**")
            if st.button("🚫 Cancelar ciclo atual", type="secondary", use_container_width=True):
                db_cancelar_ciclo_ativo(engine, empresa, filial)
                _resetar_estado_ciclo(_cache_key)
                st.rerun()
        
        # PRIORIDADE: manual
        if codigos_manuais:
            df_lista = df_score[
                df_score["Produto"].astype(str).str.zfill(6).isin(codigos_manuais)
            ].copy()
        
        elif ciclo_ativo:
            prods_fixos = [str(p).zfill(6) for p in ciclo_ativo.get("produtos_lista", [])]
            contados_set = set(data["contados"].keys())
            prods_pendentes = [p for p in prods_fixos if p not in contados_set]
            if prods_pendentes:
                df_lista = df_score[
                    df_score["Produto"].astype(str).str.zfill(6).isin(prods_pendentes)
                ].copy()
            else:
                df_lista = df_score[
                    df_score["Produto"].astype(str).str.zfill(6).isin(prods_fixos)
                ].copy()
                st.info("Todos os itens da lista já foram marcados como contados; revise o ciclo ou cancele se quiser reiniciar.")
        
        else:
            armazens = sorted(df_score["Armazem"].unique().tolist()) if "Armazem" in df_score.columns else []
            arm_sel = st.multiselect("🏭 Armazéns", armazens, default=armazens)
            df_f = df_score[df_score["Armazem"].isin(arm_sel)] if arm_sel else df_score
        
            c1, c2 = st.columns([2,1])
            modo = c1.radio("Modo", ["Quantidade fixa", "Percentual"], horizontal=True)
        
            if modo == "Quantidade fixa":
                qtd = c2.number_input("Qtd", 5, 200, 30)
            else:
                pct = c2.select_slider("%", [5, 10, 20], value=10)
                qtd = max(1, int(len(df_f) * pct / 100))
        
            df_lista = df_f.head(qtd).copy()
        
        desc_col = next((c for c in df_lista.columns if "Descr" in str(c)), None)
        cols_lista = ["Produto"]
        if desc_col:
            cols_lista.append(desc_col)
        cols_lista.extend([c for c in ["Saldo ERP (Total)", "Vl Total ERP", "Curva ABC", "Já Contado", "Score", "Motivo", "Origem"] if c in df_lista.columns])
        st.dataframe(df_lista[cols_lista], use_container_width=True, hide_index=True)
        if not ciclo_ativo:
            if st.button("🚀 Iniciar Ciclo", type="primary", use_container_width=True):
                num_c = db_gerar_num_ciclo(engine, empresa, filial)
                prods = df_lista["Produto"].astype(str).tolist()
                db_salvar_ciclo_ativo(engine, empresa, filial, {
                    "num_ciclo": num_c,
                    "data_geracao": date.today().strftime("%d/%m/%Y"),
                    "responsavel": st.session_state.get("_app_operador", ""),
                    "produtos_lista": prods,
                    "qtd_lista": len(prods),
                })
                # Limpar cache e estado para garantir dados frescos do novo ciclo
                _resetar_estado_ciclo(_cache_key)
                st.session_state["ic_etapa_nav"] = 2
                st.rerun()

    # ── ETAPA 2 ──────────────────────────────────────────────────────────
        else:
            if st.button("âž¡ï¸ Continuar para Upload ERP", type="primary", use_container_width=True):
                st.session_state["ic_etapa_nav"] = 2
                st.rerun()

    elif etapa == 2:
        st.subheader("2. Upload do Relatório Protheus")
        if erp_data:
            st.success(f"✅ Upload salvo (Doc: {erp_data[0].get('documento')})")
            c1, c2 = st.columns(2)
            if c1.button("🗑️ Remover", use_container_width=True):
                db_remover_erp_uploads(engine, empresa, filial, ciclo_ativo['num_ciclo'])
                st.session_state["ic_aceitar_nao_contados"] = False
                st.session_state["ic_force_reload"] = True
                st.rerun()
            if c2.button("Conferência ➡️", type="primary", use_container_width=True):
                st.session_state["ic_etapa_nav"] = 3
                st.rerun()
        else:
            arq = st.file_uploader("Selecione Excel Protheus", type=["xlsx"])
            if arq:
                df_raw = pd.read_excel(arq, header=None)
                header_row_idx = 0
                for idx, row in df_raw.iterrows():
                    row_vals = [str(x).upper() for x in row.values]
                    if "CODIGO" in row_vals or "CÓDIGO" in row_vals:
                        header_row_idx = idx
                        break
                df_up = pd.read_excel(arq, header=header_row_idx)
                df_up.columns = [str(c).upper().strip() for c in df_up.columns]
                mapa = {}
                for c in df_up.columns:
                    if "CODIGO" in c or "CÓDIGO" in c: mapa[c] = "Codigo"
                    elif "DESCRICAO" in c or "DESCRIÇÃO" in c: mapa[c] = "Descricao"
                    elif "INVENTARIADA" in c: mapa[c] = "Qtd WMS"
                    elif "DATA DO INVENTARIO" in c: mapa[c] = "Qtd ERP"
                    elif "DIFERENCA QUANTIDADE" in c: mapa[c] = "Divergencia Qtd"
                    elif "DIFERENCA VALOR" in c: mapa[c] = "Divergencia Valor"
                    elif "DOCUMENTO" in c: mapa[c] = "Documento"
                df_up = df_up.rename(columns=mapa)
                df_up = df_up[list(mapa.values())].dropna(subset=["Codigo"])
                df_up["Codigo"] = df_up["Codigo"].astype(str).str.split('.').str[0].str.zfill(6)
                df_up["Qtd WMS"] = pd.to_numeric(df_up["Qtd WMS"], errors='coerce').fillna(0)
                df_up["Qtd ERP"] = pd.to_numeric(df_up["Qtd ERP"], errors='coerce').fillna(0)
                if "Divergencia Valor" in df_up.columns:
                    df_up["Divergencia Valor"] = pd.to_numeric(df_up["Divergencia Valor"], errors='coerce').fillna(0)
                df_up["Divergencia Qtd"] = df_up["Qtd WMS"] - df_up["Qtd ERP"]
                if "Divergencia Valor" in df_up.columns and (df_up["Divergencia Valor"] > 0).any() and (df_up["Divergencia Qtd"] < 0).any():
                    df_up["Divergencia Valor"] = -df_up["Divergencia Valor"].abs()
                st.dataframe(df_up[["Codigo", "Descricao", "Qtd WMS", "Qtd ERP", "Divergencia Qtd"]], use_container_width=True)
                if st.button("💾 Confirmar e Salvar Dados", type="primary"):
                    doc_num = str(df_up["Documento"].iloc[0]) if "Documento" in df_up.columns else "S/N"
                    db_salvar_erp_upload(engine, empresa, filial, ciclo_ativo['num_ciclo'], doc_num, date.today().isoformat(), df_up.to_dict("records"))
                    db_marcar_contados(engine, empresa, filial, df_up["Codigo"].astype(str).tolist(), num_ciclo=ciclo_ativo['num_ciclo'])
                    st.session_state["ic_aceitar_nao_contados"] = False
                    st.session_state["ic_force_reload"] = True
                    st.session_state["ic_etapa_nav"] = 3
                    st.rerun()

    # ── ETAPA 3 ──────────────────────────────────────────────────────────
    elif etapa == 3:
        st.subheader("3. Justificativa de Divergências")
        if erp_data:
            df_all = pd.concat([pd.DataFrame(u["dados"]) for u in erp_data])
        else:
            df_all = pd.DataFrame()

        df_score = calcular_score_turbo(df_jlle, tuple(sorted(data["contados"].items())))
        produtos_esperados = [str(p).zfill(6) for p in ciclo_ativo.get("produtos_lista", [])] if ciclo_ativo else []
        upload_codes = set(df_all["Codigo"].astype(str).str.zfill(6)) if not df_all.empty and "Codigo" in df_all.columns else set()
        missing_codes = [p for p in produtos_esperados if p not in upload_codes]
        zero_erp_codes = []
        if not df_all.empty and "Codigo" in df_all.columns and "Qtd ERP" in df_all.columns and "Qtd WMS" in df_all.columns:
            for _, row in df_all.iterrows():
                codigo = str(row["Codigo"]).zfill(6)
                qte_erp = pd.to_numeric(row["Qtd ERP"], errors="coerce")
                if pd.isna(qte_erp):
                    qte_erp = 0
                qte_wms = pd.to_numeric(row["Qtd WMS"], errors="coerce")
                if pd.isna(qte_wms):
                    qte_wms = 0
                if codigo in produtos_esperados and qte_erp == 0 and qte_wms > 0:
                    zero_erp_codes.append(codigo)
        zero_erp_codes = [c for c in zero_erp_codes if c not in missing_codes]
        missing_rows = []
        for codigo in missing_codes + zero_erp_codes:
            linha = df_score[df_score["Produto"].astype(str).str.zfill(6) == codigo]
            qtd_wms = float(linha["Saldo WMS"].iloc[0]) if not linha.empty and "Saldo WMS" in linha.columns else 0
            descricao = linha["Descrição"].iloc[0] if not linha.empty and "Descrição" in linha.columns else ""
            missing_rows.append({
                "Codigo": codigo,
                "Descricao": descricao,
                "Qtd WMS": qtd_wms,
                "Qtd ERP": 0,
                "Divergencia Qtd": qtd_wms,
                "Status": "Não contado",
                "Justificativa": "Não contado"
            })

        if not df_all.empty and "Divergencia Qtd" in df_all.columns:
            df_div = df_all[df_all["Divergencia Qtd"] != 0].copy()
        else:
            df_div = pd.DataFrame(columns=["Codigo", "Descricao", "Qtd WMS", "Qtd ERP", "Divergencia Qtd", "Status", "Justificativa"])

        if missing_rows:
            df_missing = pd.DataFrame(missing_rows)
            df_div = pd.concat([df_div, df_missing], ignore_index=True, sort=False) if not df_div.empty else df_missing
            st.warning("Existem itens da lista que não apareceram no upload. Escolha se deseja enviar um novo arquivo ou prosseguir sem contagem.")
            opcao_nao_contados = st.radio(
                "O que deseja fazer com os itens não contados?",
                ["Enviar novo upload para contar itens faltantes", "Prosseguir sem contar estes itens"],
                index=0
            )
            if opcao_nao_contados == "Enviar novo upload para contar itens faltantes":
                if st.button("🔄 Contar itens faltantes", type="secondary", use_container_width=True):
                    st.session_state["ic_aceitar_nao_contados"] = False
                    st.session_state["ic_etapa_nav"] = 2
                    st.rerun()
            else:
                if st.button("⏭️ Prosseguir sem contar", type="secondary", use_container_width=True):
                    st.session_state["ic_aceitar_nao_contados"] = True
                    st.session_state["ic_force_reload"] = True
                    st.rerun()

        MOTIVOS = ["Ajuste de inventário", "Erro de contagem", "Produto em trânsito", "Erro no sistema ERP", "Não contado"]

        if df_div.empty:
            st.success("✅ Nenhuma divergência encontrada. Tudo confere!")
            if st.button("➡️ Seguir para Fechar Ciclo", type="primary", use_container_width=True):
                st.session_state["ic_etapa_nav"] = 5
                st.rerun()
        else:
            df_div["Justificativa"] = df_div["Codigo"].apply(lambda x: justs_salvas.get(x, "Não contado") if x in missing_codes else justs_salvas.get(x, MOTIVOS[0]))
            df_div["Status"] = df_div.get("Status", "Divergente")
            df_edit = st.data_editor(
                df_div[["Codigo", "Descricao", "Qtd WMS", "Qtd ERP", "Divergencia Qtd", "Status", "Justificativa"]],
                column_config={"Justificativa": st.column_config.SelectboxColumn("Motivo", options=MOTIVOS, required=True)},
                disabled=["Codigo", "Descricao", "Qtd WMS", "Qtd ERP", "Divergencia Qtd", "Status"],
                use_container_width=True, hide_index=True
            )
            if st.button("💾 Salvar Justificativas", type="primary", use_container_width=True):
                novas_justs = dict(zip(df_edit["Codigo"], df_edit["Justificativa"]))
                db_salvar_justificativas(engine, empresa, filial, ciclo_ativo['num_ciclo'], novas_justs)
                st.session_state["ic_force_reload"] = True
                if "Ajuste de inventário" in novas_justs.values():
                    st.session_state["ic_etapa_nav"] = 4
                else:
                    st.session_state["ic_etapa_nav"] = 5
                st.rerun()

    # ── ETAPA 4 ──────────────────────────────────────────────────────────
    elif etapa == 4:
        st.subheader("4. Lançamento de NF via DANFE (PDF)")
        arq_pdf = st.file_uploader("Upload PDF", type=["pdf"])
        if arq_pdf:
            nf_dados, erro = parsear_nf_danfe(arq_pdf.read())
            if not erro:
                st.success(f"NF {nf_dados['num_nf']} Detectada.")
                df_itens_nf = pd.DataFrame(nf_dados["itens"])
                st.dataframe(df_itens_nf.style.format({"Qtd": "{:,.4f}", "Vl Unit": "R$ {:,.2f}", "Vl Total": "R$ {:,.2f}"}), use_container_width=True)
                # Validação: itens da NF devem bater com divergentes da etapa anterior
                codigos_nf = set(str(item.get("Codigo", "")).zfill(6) for item in nf_dados["itens"] if item.get("Codigo"))
                codigos_divergentes = set(str(k).zfill(6) for k, v in justs_salvas.items() if v == "Ajuste de inventário")
                if not codigos_nf.issubset(codigos_divergentes):
                    st.error("Os itens da NF não batem com os itens divergentes marcados para 'Ajuste de inventário'. Verifique a NF ou as justificativas.")
                    st.write("Itens divergentes:", sorted(codigos_divergentes))
                    st.write("Itens na NF:", sorted(codigos_nf))
                else:
                    if st.button("💾 Vincular NF", type="primary"):
                        try:
                            data_iso = datetime.strptime(nf_dados["data"], "%d/%m/%Y").date().isoformat()
                        except:
                            data_iso = date.today().isoformat()
                        db_salvar_nf_ajuste(engine, empresa, filial, ciclo_ativo['num_ciclo'], nf_dados["num_nf"], data_iso, nf_dados["natureza"], nf_dados["itens"])
                        st.session_state["ic_force_reload"] = True
                        st.session_state["ic_etapa_nav"] = 5
                        st.rerun()

    # ── ETAPA 5 ──────────────────────────────────────────────────────────
    elif etapa == 5:
        st.subheader("5. Finalizar Ciclo")
        pending_codes = []
        if ciclo_ativo:
            df_score = calcular_score_turbo(df_jlle, tuple(sorted(data["contados"].items())))
            produtos_esperados = [str(p).zfill(6) for p in ciclo_ativo.get("produtos_lista", [])]
            if erp_data:
                df_erp_all = pd.concat([pd.DataFrame(u["dados"]) for u in erp_data])
                upload_codes = set(df_erp_all["Codigo"].astype(str).str.zfill(6))
                zero_erp_codes = [str(row["Codigo"]).zfill(6) for _, row in df_erp_all.iterrows()
                                  if str(row["Codigo"]).zfill(6) in produtos_esperados
                                  and pd.to_numeric(row.get("Qtd ERP", 0), errors="coerce") == 0
                                  and pd.to_numeric(row.get("Qtd WMS", 0), errors="coerce") > 0]
            else:
                upload_codes = set()
                zero_erp_codes = []
            missing_codes = [p for p in produtos_esperados if p not in upload_codes]
            pending_codes = sorted(set(missing_codes + zero_erp_codes))
        todos_justificados = all(p in justs_salvas for p in pending_codes) if pending_codes else True
        if pending_codes and not st.session_state.get("ic_aceitar_nao_contados") and not todos_justificados:
            st.warning("Ainda existem itens não contados. Faça novo upload ou confirme prosseguir sem contagem antes de encerrar.")
            st.write("Itens pendentes:", ", ".join(pending_codes))
            col_a, col_b, col_c = st.columns(3)
            if col_a.button("🔄 Voltar para upload ERP", use_container_width=True):
                st.session_state["ic_etapa_nav"] = 2
                st.rerun()
            if col_b.button("⏭️ Prosseguir sem contar", type="secondary", use_container_width=True):
                st.session_state["ic_aceitar_nao_contados"] = True
                st.session_state["ic_force_reload"] = True
                st.rerun()
            if col_c.button("🚫 Cancelar ciclo", type="secondary", use_container_width=True):
                db_cancelar_ciclo_ativo(engine, empresa, filial)
                _resetar_estado_ciclo(_cache_key)
                st.rerun()
        else:
            if pending_codes:
                st.info("Itens não contados foram aceitos/justificados para este ciclo.")
            if st.button("🏁 ENCERRAR", type="primary", use_container_width=True):
                ok = db_fechar_ciclo_ativo(engine, empresa, filial)
                if not ok:
                    st.error("Erro ao salvar o histórico do ciclo. Verifique os logs do servidor.")
                else:
                    if _cache_key in st.session_state:
                        del st.session_state[_cache_key]
                    st.session_state.pop("ic_aceitar_nao_contados", None)
                    st.session_state["ic_force_reload"] = True
                    st.session_state["ic_etapa_nav"] = 1
                    st.rerun()

    # ── ETAPA 6 — HISTÓRICO KPMG ─────────────────────────────────────────
    elif etapa == 6:
        st.markdown("### 6. Histórico KPMG")

        ciclos = data.get("ciclos", [])
        if not ciclos:
            st.info("Nenhum ciclo no histórico ainda.")
            return

        # Botões de seleção em massa
        c_sel, c_des, _ = st.columns([1, 1, 5])
        if c_sel.button("☑ Selecionar todos", use_container_width=True):
            st.session_state["ic_hist_todos"] = True
            st.rerun()
        if c_des.button("☐ Desmarcar todos", use_container_width=True):
            st.session_state["ic_hist_todos"] = False
            st.rerun()

        # Estado padrão: todos selecionados
        todos_marcados = st.session_state.get("ic_hist_todos", True)

        # Montar tabela
        rows_tabela = []
        for c in ciclos:
            uploads = c.get("uploads", [])
            n_skus = sum(len(u.get("dados", [])) for u in uploads)
            if n_skus == 0:
                n_skus = len(c.get("produtos_lista", []))
            rows_tabela.append({
                "✓": todos_marcados,
                "Nome do Ciclo": c.get("num_ciclo", "—"),
                "Data": c.get("data_fechamento") or c.get("data_geracao") or "—",
                "Responsável": c.get("responsavel") or st.session_state.get("_app_operador", "—"),
                "SKUs Contados": n_skus,
            })

        df_tabela = pd.DataFrame(rows_tabela)
        df_edit = st.data_editor(
            df_tabela,
            key=f"ic_hist_editor_{todos_marcados}",
            column_config={
                "✓": st.column_config.CheckboxColumn("✓", default=True, width="small"),
                "Nome do Ciclo": st.column_config.TextColumn("Nome do Ciclo"),
                "Data": st.column_config.TextColumn("Data", width="medium"),
                "Responsável": st.column_config.TextColumn("Responsável", width="medium"),
                "SKUs Contados": st.column_config.NumberColumn("SKUs Contados", width="small"),
            },
            disabled=["Nome do Ciclo", "Data", "Responsável", "SKUs Contados"],
            hide_index=True,
            use_container_width=True,
        )

        ciclos_sel_ids = df_edit[df_edit["✓"]]["Nome do Ciclo"].tolist()
        if not ciclos_sel_ids:
            st.warning("Selecione ao menos um ciclo.")
            return

        n_sel = len(ciclos_sel_ids)
        st.caption(f"{n_sel} ciclo(s) selecionado(s)")

        col_pdf, col_del = st.columns([3, 1])

        # ── Excluir ciclos selecionados ───────────────────────────────────
        with col_del:
            if st.button("🗑️ Excluir selecionados", use_container_width=True):
                st.session_state["ic_confirmar_excluir"] = ciclos_sel_ids[:]
                st.rerun()

        if st.session_state.get("ic_confirmar_excluir"):
            ids_para_excluir = st.session_state["ic_confirmar_excluir"]
            st.warning(
                f"⚠️ Isso vai apagar permanentemente **{len(ids_para_excluir)} ciclo(s)** "
                f"e todos os dados relacionados (contagens, justificativas, NFs). Confirma?"
            )
            c_ok, c_cancel, _ = st.columns([1, 1, 4])
            if c_ok.button("✅ Confirmar exclusão", type="primary", use_container_width=True):
                erros = []
                for num_ciclo in ids_para_excluir:
                    ok = db_excluir_ciclo_historico(engine, empresa, filial, num_ciclo)
                    if not ok:
                        erros.append(num_ciclo)
                del st.session_state["ic_confirmar_excluir"]
                st.session_state["ic_force_reload"] = True
                if erros:
                    st.error(f"Erro ao excluir: {', '.join(erros)}")
                else:
                    st.success(f"{len(ids_para_excluir)} ciclo(s) excluído(s).")
                st.rerun()
            if c_cancel.button("❌ Cancelar", use_container_width=True):
                del st.session_state["ic_confirmar_excluir"]
                st.rerun()

        # ── Gerar PDF KPMG ────────────────────────────────────────────────
        with col_pdf:
            if st.button("📄 Gerar PDF KPMG", type="primary", use_container_width=True):
                ciclos_map = {c["num_ciclo"]: c for c in ciclos}
                ciclos_sel = [ciclos_map[cid] for cid in ciclos_sel_ids if cid in ciclos_map]

                for c in ciclos_sel:
                    if not c.get("responsavel"):
                        c["responsavel"] = st.session_state.get("_app_operador", "—")
                    if not c.get("data"):
                        c["data"] = c.get("data_fechamento") or c.get("data_geracao") or "—"

                with st.spinner("Gerando relatório..."):
                    total_catalogo = (
                        df_jlle["Produto"].nunique()
                        if df_jlle is not None and not df_jlle.empty and "Produto" in df_jlle.columns
                        else 0
                    )
                    dfs_rel = {}
                    for c in ciclos_sel:
                        df_rel = montar_df_relatorio(c.get("uploads", []), df_jlle)
                        c["cobertura_pct"] = (len(df_rel) / total_catalogo * 100) if total_catalogo else 0
                        c["_justs_pdf"] = db_obter_justificativas(engine, empresa, filial, c["num_ciclo"]) or {}
                        _nfs_raw = db_obter_nf_ajustes(engine, empresa, filial, c["num_ciclo"]) or {}
                        _nfs_por_prod = {}
                        for _nf_num, _nf_info in _nfs_raw.items():
                            for _item in _nf_info.get("dados", []):
                                _cod = str(_item.get("Codigo", "")).strip().zfill(6)
                                if _cod:
                                    _nfs_por_prod[_cod] = _nf_num
                        c["_nfs_pdf"] = _nfs_por_prod
                        dfs_rel[c["num_ciclo"]] = df_rel

                    pdf_bytes = gerar_pdf_kpmg_consolidado(
                        ciclos_sel=ciclos_sel,
                        dfs_rel=dfs_rel,
                        empresa=empresa,
                        filial=filial,
                        total_catalogo=total_catalogo,
                    )

                if pdf_bytes:
                    st.download_button(
                        "📥 Baixar PDF",
                        pdf_bytes,
                        file_name=f"relatorio_kpmg_{date.today().strftime('%Y%m%d')}.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                    )
                else:
                    st.error("Erro ao gerar PDF. Verifique a instalação do reportlab.")









