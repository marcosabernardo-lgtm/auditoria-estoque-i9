import streamlit as st
import pandas as pd
import io
import json
from datetime import date, datetime
from sqlalchemy import text


# ── Parser DANFE (reutiliza o mesmo do inventário cíclico) ───────────────────
def parsear_nf_danfe(arquivo_bytes):
    import re, io as _io
    result = {"num_nf":"","data":"","natureza":"","itens":[]}
    try:
        import pdfplumber
        with pdfplumber.open(_io.BytesIO(arquivo_bytes)) as pdf:
            text_pdf = "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception as e:
        return result, str(e)
    nums = re.findall(r'N\.\s*0*(\d+)', text_pdf)
    if nums: result["num_nf"] = nums[0].zfill(9)
    m = re.search(r'DATA DE EMISS[ÃA]O\s*\n?\s*(\d{2}/\d{2}/\d{4})', text_pdf)
    if m: result["data"] = m.group(1)
    else:
        m = re.search(r'(\d{2}/\d{2}/\d{4})\s+\d{2}:\d{2}:\d{2}', text_pdf)
        if m: result["data"] = m.group(1)
    m = re.search(r'NATUREZA DA OPERA[ÇC][ÃA]O\s*\n\s*(.+?)(?:\s+PROTOCOLO|\n)', text_pdf)
    if m: result["natureza"] = m.group(1).strip()
    else:
        m = re.search(r'(BAIXA [A-Z]+|VENDA|TRANSFERENCIA|AJUSTE DE INVENTARIO)', text_pdf)
        if m: result["natureza"] = m.group(1)
    itens = []
    padrao = re.compile(
        r'(\d{6})\s+(.+?)\s+\d{8}\s+\d{3}\s+\d{4}\s+\w+\s+([\d,]+)\s+([\d,]+(?:\d{3})?)\s+([\d,]+)',
        re.MULTILINE)
    for m in padrao.finditer(text_pdf):
        itens.append({
            "Codigo": m.group(1), "Descricao": m.group(2).strip(),
            "Qtd": float(m.group(3).replace(",",".")),
            "Vl Unit": float(m.group(4).replace(",",".")),
            "Vl Total": float(m.group(5).replace(",",".")),
        })
    result["itens"] = itens
    return result, None


# ── DB helpers ────────────────────────────────────────────────────────────────
def db_salvar_ajuste(engine, empresa, filial, num_nf, data_nf, natureza,
                     justificativa, dados, operador, origem="manual", num_ciclo=""):
    if engine is None: return False
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO inventario_ajustes
                    (empresa, filial, num_nf, data_nf, natureza, justificativa,
                     dados_json, operador, origem, num_ciclo, criado_em)
                VALUES (:e,:f,:nf,:data,:nat,:just,:dados,:op,:orig,:ciclo,NOW())
            """), {"e":empresa,"f":filial,"nf":num_nf,"data":data_nf,"nat":natureza,
                   "just":justificativa,"dados":json.dumps(dados, ensure_ascii=False),
                   "op":operador,"orig":origem,"ciclo":num_ciclo})
            conn.commit()
        return True
    except Exception as ex:
        st.error(f"Erro ao salvar: {ex}")
        return False


def db_obter_ajustes(engine, empresa, filial, mes=None, ano=None):
    if engine is None: return []
    try:
        where = "WHERE a.empresa=:e AND a.filial=:f"
        params = {"e": empresa, "f": filial}
        if mes and ano:
            where += " AND EXTRACT(MONTH FROM a.data_nf)=:mes AND EXTRACT(YEAR FROM a.data_nf)=:ano"
            params["mes"] = mes
            params["ano"] = ano
        elif ano:
            where += " AND EXTRACT(YEAR FROM a.data_nf)=:ano"
            params["ano"] = ano
        with engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT id, num_nf, data_nf, natureza, justificativa,
                       dados_json, operador, origem, num_ciclo, criado_em
                FROM inventario_ajustes a
                {where}
                ORDER BY criado_em DESC
            """), params).fetchall()
        result = []
        for r in rows:
            try: dados = json.loads(r[5] or "[]")
            except: dados = []
            result.append({
                "id": r[0], "num_nf": r[1], "data_nf": str(r[2]) if r[2] else "",
                "natureza": r[3], "justificativa": r[4], "dados": dados,
                "operador": r[6], "origem": r[7], "num_ciclo": r[8],
                "criado_em": str(r[9]) if r[9] else "",
            })
        return result
    except Exception as ex:
        st.error(f"Erro ao buscar ajustes: {ex}")
        return []


def db_obter_ajustes_datas(engine, empresa, filial, data_de, data_ate):
    """Busca ajustes manuais entre duas datas."""
    if engine is None: return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, num_nf, data_nf, natureza, justificativa,
                       dados_json, operador, origem, num_ciclo, criado_em
                FROM inventario_ajustes
                WHERE empresa=:e AND filial=:f
                  AND data_nf BETWEEN :de AND :ate
                ORDER BY criado_em DESC
            """), {"e":empresa,"f":filial,"de":str(data_de),"ate":str(data_ate)}).fetchall()
        result = []
        for r in rows:
            try: dados = json.loads(r[5] or "[]")
            except: dados = []
            result.append({
                "id":r[0],"num_nf":r[1],"data_nf":str(r[2]) if r[2] else "",
                "natureza":r[3],"justificativa":r[4],"dados":dados,
                "operador":r[6],"origem":r[7],"num_ciclo":r[8],
                "criado_em":str(r[9]) if r[9] else "",
            })
        return result
    except Exception as ex:
        st.error(f"Erro ao buscar ajustes: {ex}")
        return []


def db_obter_ajustes_ciclos_datas(engine, empresa, filial, data_de, data_ate):
    """Busca NFs do cíclico entre duas datas."""
    if engine is None: return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT num_nf, data_nf, natureza, dados_json, num_ciclo, atualizado_em
                FROM inventario_nf_ajuste
                WHERE empresa=:e AND filial=:f
                  AND data_nf BETWEEN :de AND :ate
                ORDER BY atualizado_em DESC
            """), {"e":empresa,"f":filial,"de":str(data_de),"ate":str(data_ate)}).fetchall()
        result = []
        for r in rows:
            try: dados = json.loads(r[3] or "[]")
            except: dados = []
            result.append({
                "num_nf":r[0],"data_nf":str(r[1]) if r[1] else "",
                "natureza":r[2],"dados":dados,"num_ciclo":r[4],
                "origem":"ciclico","justificativa":"Ajuste de inventário",
                "operador":"—","criado_em":str(r[5]) if r[5] else "",
            })
        return result
    except Exception as ex:
        return []


def db_obter_ajustes_periodo(engine, empresa, filial, mes_de, ano_de, mes_ate, ano_ate):
    """Busca ajustes manuais em um intervalo de mês/ano."""
    if engine is None: return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, num_nf, data_nf, natureza, justificativa,
                       dados_json, operador, origem, num_ciclo, criado_em
                FROM inventario_ajustes
                WHERE empresa=:e AND filial=:f
                  AND (EXTRACT(YEAR FROM data_nf)*100 + EXTRACT(MONTH FROM data_nf))
                      BETWEEN :de AND :ate
                ORDER BY criado_em DESC
            """), {"e":empresa,"f":filial,
                   "de":ano_de*100+mes_de,"ate":ano_ate*100+mes_ate}).fetchall()
        result = []
        for r in rows:
            try: dados = json.loads(r[5] or "[]")
            except: dados = []
            result.append({
                "id":r[0],"num_nf":r[1],"data_nf":str(r[2]) if r[2] else "",
                "natureza":r[3],"justificativa":r[4],"dados":dados,
                "operador":r[6],"origem":r[7],"num_ciclo":r[8],
                "criado_em":str(r[9]) if r[9] else "",
            })
        return result
    except Exception as ex:
        st.error(f"Erro ao buscar ajustes: {ex}")
        return []


def db_obter_ajustes_ciclos_periodo(engine, empresa, filial, mes_de, ano_de, mes_ate, ano_ate):
    """Busca NFs do cíclico em um intervalo de mês/ano."""
    if engine is None: return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT num_nf, data_nf, natureza, dados_json, num_ciclo, atualizado_em
                FROM inventario_nf_ajuste
                WHERE empresa=:e AND filial=:f
                  AND (EXTRACT(YEAR FROM data_nf)*100 + EXTRACT(MONTH FROM data_nf))
                      BETWEEN :de AND :ate
                ORDER BY atualizado_em DESC
            """), {"e":empresa,"f":filial,
                   "de":ano_de*100+mes_de,"ate":ano_ate*100+mes_ate}).fetchall()
        result = []
        for r in rows:
            try: dados = json.loads(r[3] or "[]")
            except: dados = []
            result.append({
                "num_nf":r[0],"data_nf":str(r[1]) if r[1] else "",
                "natureza":r[2],"dados":dados,"num_ciclo":r[4],
                "origem":"ciclico","justificativa":"Ajuste de inventário",
                "operador":"—","criado_em":str(r[5]) if r[5] else "",
            })
        return result
    except Exception as ex:
        return []


def db_obter_ajustes_ciclos(engine, empresa, filial, mes=None, ano=None):
    """Busca NFs de ajuste dos inventários cíclicos."""
    if engine is None: return []
    try:
        where = "WHERE n.empresa=:e AND n.filial=:f"
        params = {"e": empresa, "f": filial}
        if mes and ano:
            where += " AND EXTRACT(MONTH FROM n.data_nf)=:mes AND EXTRACT(YEAR FROM n.data_nf)=:ano"
            params["mes"] = mes
            params["ano"] = ano
        elif ano:
            where += " AND EXTRACT(YEAR FROM n.data_nf)=:ano"
            params["ano"] = ano
        with engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT n.num_nf, n.data_nf, n.natureza, n.dados_json,
                       n.num_ciclo, n.atualizado_em
                FROM inventario_nf_ajuste n
                {where}
                ORDER BY n.atualizado_em DESC
            """), params).fetchall()
        result = []
        for r in rows:
            try: dados = json.loads(r[3] or "[]")
            except: dados = []
            result.append({
                "num_nf": r[0], "data_nf": str(r[1]) if r[1] else "",
                "natureza": r[2], "dados": dados,
                "num_ciclo": r[4], "origem": "ciclico",
                "justificativa": "Ajuste de inventário",
                "operador": "—", "criado_em": str(r[5]) if r[5] else "",
            })
        return result
    except Exception as ex:
        return []


# ── Render principal ──────────────────────────────────────────────────────────
def render(empresa_sel, filial_sel, formatar_br):
    st.markdown(
        """<div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;">
           <span style="font-size:1.8rem;">📋</span>
           <div>
             <div style="color:#fff;font-size:1.4rem;font-weight:700;">Ajustes de Inventário</div>
             <div style="color:#aac8cc;font-size:0.85rem;">Upload de NFs de ajuste e relatório consolidado</div>
           </div>
        </div>""", unsafe_allow_html=True)

    if not empresa_sel or not filial_sel:
        st.warning("⚠️ Selecione empresa e filial na tela inicial.")
        return

    engine = st.session_state.get("_engine")
    operador = st.session_state.get("_app_operador", "—")

    aba1, aba2 = st.tabs(["📤 Registrar NF de Ajuste", "📊 Relatório Consolidado"])

    # ── ABA 1: Registrar NF ───────────────────────────────────────────────────
    with aba1:
        st.markdown("### Importar NF de Ajuste")
        st.caption("Faça o upload do PDF da DANFE. Os dados serão extraídos automaticamente.")

        arq_pdf = st.file_uploader("📎 Selecione o PDF da NF (DANFE)",
                                    type=["pdf"], key=f"aj_pdf_upload_{st.session_state.get('aj_upload_count',0)}")

        if arq_pdf:
            pdf_bytes = arq_pdf.read()
            nf_dados, nf_erro = parsear_nf_danfe(pdf_bytes)

            if nf_erro:
                st.error(f"Erro ao ler PDF: {nf_erro}")
            elif not nf_dados["itens"]:
                st.warning("⚠️ Não foi possível extrair itens do PDF.")
            else:
                st.success(f"✅ NF **{nf_dados['num_nf']}** · {nf_dados['data']} · {nf_dados['natureza']} · **{len(nf_dados['itens'])} item(ns)**")

                col_a, col_b, col_c = st.columns(3)
                col_a.markdown(f"**Nº NF:** {nf_dados['num_nf']}")
                col_b.markdown(f"**Data:** {nf_dados['data']}")
                col_c.markdown(f"**Natureza:** {nf_dados['natureza']}")

                st.markdown("**Itens da NF:**")
                df_itens = pd.DataFrame(nf_dados["itens"])
                st.dataframe(df_itens.style.format({
                    "Qtd": "{:,.4f}", "Vl Unit": "R$ {:,.2f}", "Vl Total": "R$ {:,.2f}"
                }, na_rep="—"), use_container_width=True, hide_index=True)

                st.markdown("---")
                justificativa = st.text_area(
                    "📝 Justificativa",
                    placeholder="Descreva o motivo do ajuste de inventário...",
                    key="aj_justificativa",
                    height=100)

                col_btn, col_op = st.columns([2, 3])
                with col_op:
                    st.caption(f"👤 Operador: **{operador}**")
                with col_btn:
                    if st.button("💾 Salvar NF de Ajuste", type="primary",
                                 use_container_width=True, key="aj_btn_salvar"):
                        if not justificativa.strip():
                            st.error("⚠️ Informe a justificativa antes de salvar.")
                        else:
                            try:
                                data_nf_iso = datetime.strptime(nf_dados["data"], "%d/%m/%Y").date().isoformat() if nf_dados["data"] else date.today().isoformat()
                            except:
                                data_nf_iso = date.today().isoformat()
                            ok = db_salvar_ajuste(
                                engine, empresa_sel, filial_sel,
                                nf_dados["num_nf"], data_nf_iso, nf_dados["natureza"],
                                justificativa.strip(), nf_dados["itens"], operador)
                            if ok:
                                st.session_state["aj_upload_count"] = st.session_state.get("aj_upload_count", 0) + 1
                                st.success(f"✅ NF {nf_dados['num_nf']} salva com sucesso!")
                                st.rerun()
        else:
            st.info("Faça o upload do PDF da NF para preencher os dados automaticamente.")

    # ── ABA 2: Relatório Consolidado ──────────────────────────────────────────
    with aba2:
        st.markdown("### Relatório Consolidado de Ajustes")
        st.caption("Inclui NFs registradas manualmente e via Inventário Cíclico.")

        # Origem acima dos filtros de data
        origem_sel = st.radio("Origem", ["Todos","Manual","Cíclico"],
                              horizontal=True, key="aj_origem")

        st.markdown("**Período:**")
        _primeiro_dia = date(date.today().year, date.today().month, 1)
        col_d1, col_d2 = st.columns(2)
        with col_d1:
            data_de = st.date_input("De", value=_primeiro_dia,
                                    format="DD/MM/YYYY", key="aj_data_de")
        with col_d2:
            data_ate = st.date_input("Até", value=date.today(),
                                     format="DD/MM/YYYY", key="aj_data_ate")

        if st.button("🔍 Carregar relatório", type="primary", key="aj_btn_rel"):
            ajustes_manual  = db_obter_ajustes_datas(engine, empresa_sel, filial_sel,
                                                      data_de, data_ate)
            ajustes_ciclico = db_obter_ajustes_ciclos_datas(engine, empresa_sel, filial_sel,
                                                             data_de, data_ate)

            # Filtra por origem
            if origem_sel == "Manual":
                todos = ajustes_manual
            elif origem_sel == "Cíclico":
                todos = ajustes_ciclico
            else:
                todos = ajustes_manual + ajustes_ciclico

            if not todos:
                st.info("Nenhum ajuste encontrado para o período selecionado.")
            else:
                # Monta DataFrame consolidado (uma linha por item da NF)
                rows_rel = []
                for aj in todos:
                    for item in aj.get("dados", []):
                        rows_rel.append({
                            "Nº NF":        aj.get("num_nf","—"),
                            "Data NF":      aj.get("data_nf","—"),
                            "Natureza":     aj.get("natureza","—"),
                            "Código":       str(item.get("Codigo","")).zfill(6),
                            "Descrição":    item.get("Descricao","—"),
                            "Qtd":          float(item.get("Qtd", 0)),
                            "Vl Unit":      float(item.get("Vl Unit", 0)),
                            "Vl Total":     float(item.get("Vl Total", 0)),
                            "Justificativa":aj.get("justificativa","—"),
                            "Origem":       aj.get("origem","manual").capitalize(),
                            "Ciclo":        aj.get("num_ciclo","—"),
                            "Operador":     aj.get("operador","—"),
                        })

                df_rel = pd.DataFrame(rows_rel)

                # KPIs
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("NFs", df_rel["Nº NF"].nunique())
                c2.metric("Itens", len(df_rel))
                c3.metric("Vl Total", f"R$ {formatar_br(df_rel['Vl Total'].sum())}")
                c4.metric("Operadores", df_rel["Operador"].nunique())

                # Tabela
                st.dataframe(
                    df_rel.style.format({
                        "Qtd": "{:,.4f}",
                        "Vl Unit": "R$ {:,.2f}",
                        "Vl Total": "R$ {:,.2f}",
                    }, na_rep="—"),
                    use_container_width=True, hide_index=True)

                # Export Excel
                def _to_excel(df):
                    buf = io.BytesIO()
                    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
                        df.to_excel(w, index=False, sheet_name="Ajustes")
                    return buf.getvalue()

                periodo = f"{data_de.strftime('%d%m%Y')}_a_{data_ate.strftime('%d%m%Y')}"
                st.download_button(
                    "📥 Exportar Excel",
                    data=_to_excel(df_rel),
                    file_name=f"ajustes_{empresa_sel}_{filial_sel.split(' - ')[-1]}_{periodo}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="aj_btn_excel")
