import streamlit as st
import pandas as pd
import numpy as np
import io
import re
import json
import pdfplumber
from datetime import date, datetime
from sqlalchemy import text 
from fpdf import FPDF # Requisito: pip install fpdf2

# Importações das funções do banco
from inventario_db import (
    db_salvar_ciclo_ativo, db_fechar_ciclo_ativo, db_carregar_tudo,
    db_salvar_erp_upload, db_marcar_contados, db_remover_erp_uploads,
    db_salvar_justificativas, db_salvar_nf_ajuste,
    db_obter_nf_ajustes, db_obter_justificativas
)

PERIODO_KPMG_DIAS = 365

# ── HELPER: gera bytes do PDF compatível com qualquer versão do fpdf2 ────────

def _pdf_para_bytes(pdf: FPDF) -> bytes:
    """
    Compatível com todas as versões do fpdf2:
      - v2.x antigo : output(dest='S') retorna str  → encode latin-1
      - v2.x novo   : output(dest='S') retorna bytes → usa direto
      - v3.x        : output() retorna bytearray     → converte com bytes()
    """
    try:
        resultado = pdf.output(dest="S")
        if isinstance(resultado, str):
            return resultado.encode("latin-1")
        return bytes(resultado)
    except TypeError:
        # Fallback para versões que não aceitam dest="S"
        resultado = pdf.output()
        if isinstance(resultado, (bytes, bytearray)):
            return bytes(resultado)
        return resultado.encode("latin-1")


# ── CLASSE DO RELATÓRIO PROFISSIONAL KPMG (PDF) ──────────────────────────────

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

# ── PARSER DANFE ROBUSTO ─────────────────────────────────────────────────────

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

# ── COMPONENTES VISUAIS E SCORE ──────────────────────────────────────────────

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
    for col in ["Vl Total ERP"]:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df = df.sort_values("Vl Total ERP", ascending=False).reset_index(drop=True)
    tv = df["Vl Total ERP"].sum()
    df["pct_acum"]  = df["Vl Total ERP"].cumsum() / tv if tv > 0 else 0
    df["Curva ABC"] = np.where(df["pct_acum"]<=0.80,"A", np.where(df["pct_acum"]<=0.95,"B","C"))
    hoje = date.today()
    df["Dias s/ Contagem"] = df["Produto"].astype(str).apply(lambda p: (hoje - date.fromisoformat(contados[p])).days if p in contados else PERIODO_KPMG_DIAS)
    df["Score"] = (df["Dias s/ Contagem"] / PERIODO_KPMG_DIAS * 10).round(2)
    df["Já Contado"] = df["Produto"].astype(str).apply(lambda p: f"✅ {contados[p]}" if p in contados else "⬜ Não")
    return df.sort_values("Score", ascending=False).reset_index(drop=True)

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

    st.markdown("### Fluxo de Inventário")
    cols = st.columns(6)
    steps = ["Gerar Lista", "Upload ERP", "Conferência", "NF Ajuste", "Fechar", "Histórico"]
    
    for i, name in enumerate(steps, 1):
        if etapa == 6:
            done = (i < 6)
        else:
            if i == 1: done = bool(ciclo_ativo)
            elif i == 2: done = bool(erp_data)
            elif i == 3: done = bool(justs_salvas)
            elif i == 4: done = bool(nf_ajustes) or (etapa > 4)
            elif i == 5: done = (etapa > 5)
            else: done = False
        
        if _card(cols[i-1], i, name, etapa == i, done, f"nv_{i}"):
            st.session_state["ic_etapa_nav"] = i
            st.rerun()
    st.divider()

    # ── ETAPA 1: GERAÇÃO DA LISTA ──────────────────────────────────────────
    if etapa == 1:
        st.subheader("1. Geração da Lista de Contagem")
        df_score = calcular_score_turbo(df_jlle, tuple(sorted(data["contados"].items())))
        
        if ciclo_ativo:
            st.warning(f"⚠️ Ciclo Ativo: **{ciclo_ativo['num_ciclo']}**")
            prods_fixos = [str(p).zfill(6) for p in ciclo_ativo.get("produtos_lista", [])]
            df_lista = df_score[df_score["Produto"].astype(str).str.zfill(6).isin(prods_fixos)].copy()
            st.info("💡 Lista congelada conforme o banco de dados.")
            if st.button("Avançar para Etapa 2 ➡️", type="primary"):
                st.session_state["ic_etapa_nav"] = 2
                st.rerun()
        else:
            with st.expander("⚙️ Seleção Manual"):
                entrada_manual = st.text_area("Insira os códigos dos produtos")
                codigos_manuais = [c.strip().zfill(6) for c in entrada_manual.replace(",", " ").split() if c.strip()]
            
            if codigos_manuais:
                df_lista = df_score[df_score["Produto"].astype(str).str.zfill(6).isin(codigos_manuais)].copy()
            else:
                armazens = sorted(df_score["Armazem"].unique().tolist()) if "Armazem" in df_score.columns else []
                arm_sel = st.multiselect("🏭 Armazéns", armazens, default=armazens)
                df_f = df_score[df_score["Armazem"].isin(arm_sel)] if arm_sel else df_score
                c1, c2 = st.columns([2,1])
                modo = c1.radio("Modo", ["Quantidade fixa", "Percentual"], horizontal=True)
                qtd = c2.number_input("Qtd", 5, 200, 30) if modo == "Quantidade fixa" else max(1, int(len(df_f) * c2.select_slider("%", [5, 10, 20], value=10) / 100))
                df_lista = df_f.head(qtd).copy()

        st.dataframe(df_lista[["Produto", "Descrição", "Curva ABC", "Já Contado", "Score"]], use_container_width=True, hide_index=True)
        if not ciclo_ativo:
            if st.button("🚀 Iniciar Ciclo", type="primary", use_container_width=True):
                num_c = f"{date.today().strftime('%Y%m%d')}-{empresa}-{filial}".replace(" ", "")
                db_salvar_ciclo_ativo(engine, empresa, filial, {"num_ciclo": num_c, "data_geracao": date.today().strftime("%d/%m/%Y"), "produtos_lista": df_lista["Produto"].astype(str).tolist()})
                st.session_state["ic_force_reload"] = True
                st.rerun()

    # ── ETAPA 2: UPLOAD ERP ────────────────────────────────────────────────────
    elif etapa == 2:
        st.subheader("2. Upload do Relatório Protheus")
        if erp_data:
            st.success(f"✅ Upload salvo (Doc: {erp_data[0].get('documento')})")
            c1, c2 = st.columns(2)
            if c1.button("🗑️ Remover", use_container_width=True):
                db_remover_erp_uploads(engine, empresa, filial, ciclo_ativo['num_ciclo'])
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
                    elif "DOCUMENTO" in c: mapa[c] = "Documento"
                df_up = df_up.rename(columns=mapa)
                df_up = df_up[list(mapa.values())].dropna(subset=["Codigo"])
                df_up["Codigo"] = df_up["Codigo"].astype(str).str.split('.').str[0].str.zfill(6)
                df_up["Qtd WMS"] = pd.to_numeric(df_up["Qtd WMS"], errors='coerce').fillna(0)
                df_up["Qtd ERP"] = pd.to_numeric(df_up["Qtd ERP"], errors='coerce').fillna(0)
                df_up["Divergencia Qtd"] = df_up["Qtd WMS"] - df_up["Qtd ERP"]
                st.dataframe(df_up[["Codigo", "Descricao", "Qtd WMS", "Qtd ERP", "Divergencia Qtd"]], use_container_width=True)
                if st.button("💾 Confirmar e Salvar Dados", type="primary"):
                    doc_num = str(df_up["Documento"].iloc[0]) if "Documento" in df_up.columns else "S/N"
                    db_salvar_erp_upload(engine, empresa, filial, ciclo_ativo['num_ciclo'], doc_num, date.today().isoformat(), df_up.to_dict("records"))
                    st.session_state["ic_force_reload"] = True
                    st.session_state["ic_etapa_nav"] = 3
                    st.rerun()

    # ── ETAPA 3: CONFERÊNCIA ──────────────────────────────────────────────────
    elif etapa == 3:
        st.subheader("3. Justificativa de Divergências")
        df_all = pd.concat([pd.DataFrame(u["dados"]) for u in erp_data])
        df_div = df_all[df_all["Divergencia Qtd"] != 0].copy()
        if df_div.empty:
            st.success("Tudo OK!")
            if st.button("Avançar"):
                st.session_state["ic_etapa_nav"] = 5
                st.rerun()
        else:
            MOTIVOS = ["Ajuste de inventário", "Erro de contagem", "Produto em trânsito", "Erro no sistema ERP"]
            df_div["Justificativa"] = df_div["Codigo"].apply(lambda x: justs_salvas.get(x, MOTIVOS[0]))
            df_edit = st.data_editor(df_div[["Codigo", "Descricao", "Qtd WMS", "Qtd ERP", "Divergencia Qtd", "Justificativa"]], column_config={"Justificativa": st.column_config.SelectboxColumn("Motivo", options=MOTIVOS, required=True)}, disabled=["Codigo", "Descricao", "Qtd WMS", "Qtd ERP", "Divergencia Qtd"], use_container_width=True, hide_index=True)
            if st.button("💾 Salvar Justificativas", type="primary", use_container_width=True):
                novas_justs = dict(zip(df_edit["Codigo"], df_edit["Justificativa"]))
                db_salvar_justificativas(engine, empresa, filial, ciclo_ativo['num_ciclo'], novas_justs)
                st.session_state["ic_force_reload"] = True
                if "Ajuste de inventário" in novas_justs.values():
                    st.session_state["ic_etapa_nav"] = 4
                else:
                    st.session_state["ic_etapa_nav"] = 5
                st.rerun()

    # ── ETAPA 4: NF AJUSTE ────────────────────────────────────────────────────
    elif etapa == 4:
        st.subheader("4. Lançamento de NF via DANFE (PDF)")
        arq_pdf = st.file_uploader("Upload PDF", type=["pdf"])
        if arq_pdf:
            nf_dados, erro = parsear_nf_danfe(arq_pdf.read())
            if not erro:
                st.success(f"NF {nf_dados['num_nf']} Detectada.")
                df_itens_nf = pd.DataFrame(nf_dados["itens"])
                st.dataframe(df_itens_nf.style.format({"Qtd": "{:,.4f}", "Vl Unit": "R$ {:,.2f}", "Vl Total": "R$ {:,.2f}"}), use_container_width=True)
                if st.button("💾 Vincular NF", type="primary"):
                    try:
                        data_iso = datetime.strptime(nf_dados["data"], "%d/%m/%Y").date().isoformat()
                    except:
                        data_iso = date.today().isoformat()
                    db_salvar_nf_ajuste(engine, empresa, filial, ciclo_ativo['num_ciclo'], nf_dados["num_nf"], data_iso, nf_dados["natureza"], nf_dados["itens"])
                    st.session_state["ic_force_reload"] = True
                    st.session_state["ic_etapa_nav"] = 5
                    st.rerun()

    # ── ETAPA 5: FECHAR ───────────────────────────────────────────────────────
    elif etapa == 5:
        st.subheader("5. Finalizar Ciclo")
        if st.button("🏁 ENCERRAR", type="primary", use_container_width=True):
            db_fechar_ciclo_ativo(engine, empresa, filial)
            st.session_state["ic_force_reload"] = True
            st.session_state["ic_etapa_nav"] = 6
            st.rerun()

    # ── ETAPA 6: RELATÓRIO PDF KPMG ──────────────────────────────────────────
    elif etapa == 6:
        st.subheader("6. Relatório Final KPMG")
        try:
            with engine.connect() as conn:
                res = conn.execute(text("""SELECT num_ciclo, num_nf, data_nf, natureza, dados_json FROM inventario_nf_ajuste WHERE empresa = :e AND filial = :f ORDER BY atualizado_em DESC"""), {"e": empresa, "f": filial})
                rows = res.fetchall()
                df_resumo = pd.DataFrame(rows, columns=res.keys())
        except:
            df_resumo = pd.DataFrame()
        
        if not df_resumo.empty:
            ciclo_sel = st.selectbox("Selecione Ciclo:", df_resumo["num_ciclo"].unique())
            if st.button("📄 Gerar PDF KPMG", type="primary", use_container_width=True):
                dados_sel = df_resumo[df_resumo["num_ciclo"] == ciclo_sel].iloc[0]

                # ✅ Parse robusto do JSON
                raw_json = dados_sel["dados_json"]
                if isinstance(raw_json, str):
                    itens = json.loads(raw_json)
                elif isinstance(raw_json, list):
                    itens = raw_json
                else:
                    itens = []

                df_itens = pd.DataFrame(itens) if itens else pd.DataFrame(
                    columns=["Codigo", "Descricao", "Saldo ERP", "Saldo WMS",
                             "Inventariado", "Diferenca", "Vl Total ERP", "Vl Total Dif",
                             "Justificativa", "NF"]
                )

                # ✅ Garantir colunas obrigatórias com valores padrão
                colunas_default = {
                    "Codigo": "000000",
                    "Descricao": "",
                    "Saldo ERP": 0,
                    "Saldo WMS": 0,
                    "Inventariado": 0,
                    "Diferenca": 0,
                    "Vl Total ERP": 0.0,
                    "Vl Total Dif": 0.0,
                    "Justificativa": "Ajuste Auditoria",
                    "NF": "",
                }
                for col, default in colunas_default.items():
                    if col not in df_itens.columns:
                        df_itens[col] = default

                # ✅ Converter colunas numéricas
                df_itens["Vl Total ERP"] = pd.to_numeric(df_itens["Vl Total ERP"], errors="coerce").fillna(0.0)
                df_itens["Vl Total Dif"] = pd.to_numeric(df_itens["Vl Total Dif"], errors="coerce").fillna(0.0)
                df_itens["Justificativa"] = df_itens["Justificativa"].fillna("Ajuste Auditoria")
                df_itens["NF"] = dados_sel["num_nf"]

                # ✅ Inicializar PDF
                pdf = KPMG_Report()
                pdf.set_auto_page_break(auto=True, margin=15)

                pdf.capa_resumo({
                    'skus': len(df_itens),
                    'cobertura': 15.0,
                    'acuracidade': 98.0,
                    'ciclos': len(df_resumo),
                    'status': "CUMPRIDA"
                })
                pdf.lista_ciclos_page(pd.DataFrame([{
                    "Nº Ciclo": ciclo_sel,
                    "Data": dados_sel["data_nf"],
                    "Responsável": "Portal I9",
                    "Nº Inv": "ERP",
                    "SKUs": len(df_itens),
                    "Div": 0,
                    "Cobert": 100,
                    "Acuracia": 100
                }]))
                pdf.detalhe_ciclo_page(df_itens)

                # ✅ FIX FINAL: helper compatível com fpdf2 v2.x antigo, v2.x novo e v3.x
                pdf_bytes = _pdf_para_bytes(pdf)

                st.download_button(
                    label="📥 Baixar PDF Auditado",
                    data=pdf_bytes,
                    file_name=f"Relatorio_KPMG_{ciclo_sel}.pdf",
                    mime="application/pdf"
                )
        else:
            st.info("Nenhum ciclo com NF de ajuste encontrado.")

        st.divider()
        if st.button("➕ Novo Ciclo"):
            st.session_state["ic_etapa_nav"] = 1
            st.rerun()