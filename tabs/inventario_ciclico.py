import streamlit as st
import pandas as pd
import numpy as np
import io
from datetime import date, datetime

PERIODO_KPMG_DIAS = 365

# ── Helpers de chave ──────────────────────────────────────────────────────────

def _slug(empresa, filial):
    return f"{empresa}_{filial}".replace(" ", "_").replace("-", "")

def _chave_contados(e, f):   return f"ic_contados_{_slug(e,f)}"
def _chave_ciclos(e, f):     return f"ic_ciclos_{_slug(e,f)}"
def _chave_ciclo_ativo(e, f):return f"ic_ciclo_ativo_{_slug(e,f)}"

def inicializar(e, f):
    for k, v in [(_chave_contados(e,f), {}), (_chave_ciclos(e,f), []), (_chave_ciclo_ativo(e,f), None)]:
        if k not in st.session_state:
            st.session_state[k] = v

def obter_contados(e, f):    return st.session_state.get(_chave_contados(e,f), {})
def obter_ciclos(e, f):      return st.session_state.get(_chave_ciclos(e,f), [])
def obter_ciclo_ativo(e, f): return st.session_state.get(_chave_ciclo_ativo(e,f))

def salvar_ciclo_ativo(e, f, ciclo):
    st.session_state[_chave_ciclo_ativo(e,f)] = ciclo

def fechar_ciclo(e, f, resultado):
    """Fecha o ciclo ativo, registra no histórico e atualiza contados."""
    ciclo = obter_ciclo_ativo(e, f)
    if not ciclo:
        return
    # Marca produtos como contados
    data_iso = resultado.get("data_iso", date.today().isoformat())
    contados = st.session_state[_chave_contados(e,f)]
    for p in resultado.get("produtos_contados", []):
        contados[p] = data_iso
    # Registra ciclo no histórico
    ciclo_fechado = {**ciclo, **resultado, "status": "Concluído"}
    st.session_state[_chave_ciclos(e,f)].append(ciclo_fechado)
    st.session_state[_chave_ciclo_ativo(e,f)] = None

def resetar(e, f):
    st.session_state[_chave_contados(e,f)]    = {}
    st.session_state[_chave_ciclos(e,f)]      = []
    st.session_state[_chave_ciclo_ativo(e,f)] = None


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

    # Consolida localizações
    if "Produto" in df.columns and len(df) > df["Produto"].nunique():
        cols_soma  = [c for c in ["Saldo WMS"] if c in df.columns]
        cols_fixos = [c for c in ["Produto","Descrição","Empresa","Filial",
                                   "Saldo ERP (Total)","Vl Unit","Vl Total ERP"] if c in df.columns]
        df_w = df.groupby("Produto", as_index=False)[cols_soma].sum() if cols_soma else df[["Produto"]].drop_duplicates()
        df_f = df[cols_fixos].drop_duplicates(subset=["Produto"], keep="first")
        df   = df_f.merge(df_w, on="Produto", how="left")
        if "Saldo ERP (Total)" in df.columns and "Saldo WMS" in df.columns:
            df["Divergência"] = df["Saldo ERP (Total)"] - df["Saldo WMS"]

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
    top = df_score.head(qtd).copy()
    top["Origem"] = top["Produto"].astype(str).apply(lambda p: "⬜ Cobertura KPMG" if p in nao_cont else "🔴 Alta prioridade")
    ja = set(top["Produto"].astype(str))
    extras = df_score[df_score["Produto"].astype(str).isin(nao_cont) & ~df_score["Produto"].astype(str).isin(ja)].copy()
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

    inicializar(empresa_sel, filial_sel)
    contados  = obter_contados(empresa_sel, filial_sel)
    ciclos    = obter_ciclos(empresa_sel, filial_sel)
    ciclo_ativo = obter_ciclo_ativo(empresa_sel, filial_sel)

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

    if not ciclo_ativo:
        # ── Gerar novo ciclo ──────────────────────────────────────────────
        st.markdown("### Gerar lista do ciclo")
        modo = st.radio("Modo", ["Quantidade fixa","Percentual"], horizontal=True, key="ic_modo")
        if modo == "Quantidade fixa":
            cols = st.columns(4)
            if "ic_qtd" not in st.session_state: st.session_state.ic_qtd = 50
            for col, qtd in zip(cols, [30,50,80,100]):
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

        # Botão para abrir ciclo
        num_ciclo = f"{date.today().strftime('%Y%m%d')}-{empresa_sel}-{filial_sel}".replace(" ","")
        col_dl, col_open = st.columns([2,1])
        with col_dl:
            st.download_button(
                "📥 Baixar Excel para Contagem",
                data=gerar_xlsx_lista(df_exibir, label),
                file_name=f"inv_{num_ciclo}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with col_open:
            if st.button("▶ Abrir este ciclo", key="ic_abrir", type="primary"):
                salvar_ciclo_ativo(empresa_sel, filial_sel, {
                    "num_ciclo":    num_ciclo,
                    "data_geracao": date.today().strftime("%d/%m/%Y"),
                    "label":        label,
                    "qtd_lista":    len(df_exibir),
                    "produtos_lista": df_exibir["Produto"].astype(str).tolist(),
                    "status":       "Aguardando resultado WMS",
                })
                st.success(f"Ciclo **{num_ciclo}** aberto! Faça a contagem no WMS e volte para fazer o upload.")
                st.rerun()

    else:
        # ── Ciclo em andamento → aguardando upload WMS ────────────────────
        st.markdown(f"### Ciclo em andamento")

        st.markdown(
            f"""<div style="background:#004550;border-radius:8px;padding:12px 16px;margin-bottom:12px;">
              <div style="display:flex;justify-content:space-between;align-items:center;">
                <div>
                  <span style="color:#EC6E21;font-weight:bold;font-size:1rem;">📋 {ciclo_ativo['num_ciclo']}</span><br>
                  <span style="color:#fff;font-size:0.85rem;">Gerado em {ciclo_ativo['data_geracao']} · {ciclo_ativo['qtd_lista']} produtos</span>
                </div>
                <span style="background:#EC6E21;color:#fff;padding:4px 10px;border-radius:12px;font-size:0.8rem;">
                  {ciclo_ativo['status']}
                </span>
              </div></div>""",
            unsafe_allow_html=True)

        st.markdown("#### Upload do resultado WMS")
        st.caption("Faça o upload do Excel gerado pelo WMS. O sistema vai comparar com a lista do ciclo.")

        arquivo = st.file_uploader("Selecione o arquivo Excel do WMS", type=["xlsx"], key="ic_upload")

        if arquivo:
            try:
                res = processar_resultado_wms(arquivo)
                df_wms = res["df"]

                # Metadados
                c1,c2,c3,c4 = st.columns(4)
                c1.metric("Nº Inventário", res.get("num_inv","—"))
                c2.metric("Data",          res.get("data","—"))
                c3.metric("Responsável",   res.get("responsavel","—"))
                c4.metric("Acuracidade",   res.get("acuracidade","—"))

                # Comparação: lista gerada vs WMS
                produtos_lista  = set(ciclo_ativo["produtos_lista"])
                produtos_wms    = set(res["produtos"])
                contados_no_ciclo   = produtos_lista & produtos_wms
                nao_contados_ciclo  = produtos_lista - produtos_wms
                extras_wms          = produtos_wms - produtos_lista

                st.markdown("#### Resultado da comparação")
                col_ok, col_miss, col_extra = st.columns(3)
                col_ok.metric("✅ Contados",          len(contados_no_ciclo))
                col_miss.metric("⚠️ Não encontrados", len(nao_contados_ciclo))
                col_extra.metric("➕ Extras no WMS",   len(extras_wms))

                # Tabela detalhada
                df_wms["Status Ciclo"] = df_wms["Codigo"].apply(
                    lambda p: "✅ Contado" if p in contados_no_ciclo else
                              "➕ Extra (não estava na lista)" if p in extras_wms else "—"
                )
                # Adiciona produtos não encontrados
                if nao_contados_ciclo:
                    df_miss = pd.DataFrame({
                        "Codigo": list(nao_contados_ciclo),
                        "Descricao": "—",
                        "Qtd Antes": 0, "Qtd Depois": 0, "Qtd Diferença": 0,
                        "Acuracidade": "—",
                        "Status Ciclo": "⚠️ Não contado"
                    })
                    df_comp = pd.concat([df_wms, df_miss], ignore_index=True)
                else:
                    df_comp = df_wms.copy()

                df_comp = df_comp.sort_values("Status Ciclo")
                st.dataframe(
                    df_comp[["Codigo","Descricao","Qtd Antes","Qtd Depois","Qtd Diferença","Acuracidade","Status Ciclo"]],
                    use_container_width=True, hide_index=True)

                # Cobertura do ciclo
                pct_ciclo = len(contados_no_ciclo) / len(produtos_lista) * 100 if produtos_lista else 0

                col_conf, col_cancel = st.columns([2,1])
                with col_conf:
                    if st.button("✅ Confirmar e fechar ciclo", key="ic_fechar", type="primary"):
                        fechar_ciclo(empresa_sel, filial_sel, {
                            "num_inv":           res.get("num_inv","—"),
                            "data":              res.get("data","—"),
                            "data_iso":          res.get("data_iso", date.today().isoformat()),
                            "responsavel":       res.get("responsavel","—"),
                            "acuracidade":       res.get("acuracidade","—"),
                            "produtos_contados": list(contados_no_ciclo),
                            "cobertura_pct":     pct_ciclo,
                        })
                        st.success(f"Ciclo fechado! {len(contados_no_ciclo)} produtos registrados.")
                        st.rerun()
                with col_cancel:
                    if st.button("✖ Cancelar ciclo", key="ic_cancelar"):
                        st.session_state[_chave_ciclo_ativo(empresa_sel, filial_sel)] = None
                        st.rerun()

            except Exception as e:
                st.error(f"Erro ao processar arquivo: {e}")
        else:
            if st.button("✖ Cancelar ciclo", key="ic_cancelar_sem_upload"):
                st.session_state[_chave_ciclo_ativo(empresa_sel, filial_sel)] = None
                st.rerun()

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

            col_dl, col_reset = st.columns([3,1])
            with col_dl:
                st.download_button(
                    "📥 Exportar Histórico KPMG",
                    data=gerar_xlsx_historico(ciclos, label),
                    file_name=f"historico_kpmg_{_slug(empresa_sel,filial_sel)}_{date.today().strftime('%d%m%Y')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            with col_reset:
                if st.button("🔄 Novo período anual", key="ic_reset"):
                    resetar(empresa_sel, filial_sel)
                    st.success("Novo período iniciado!")
                    st.rerun()
