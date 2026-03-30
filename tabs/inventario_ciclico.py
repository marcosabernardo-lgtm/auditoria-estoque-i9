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
def _chave_uploads(e, f):    return f"ic_uploads_{_slug(e,f)}"

def inicializar(e, f):
    for k, v in [
        (_chave_contados(e,f),    {}),
        (_chave_ciclos(e,f),      []),
        (_chave_ciclo_ativo(e,f), None),
        (_chave_uploads(e,f),     []),   # lista de uploads do ciclo atual
    ]:
        if k not in st.session_state:
            st.session_state[k] = v

def obter_uploads(e, f):     return st.session_state.get(_chave_uploads(e,f), [])

def acumular_upload(e, f, upload_info: dict):
    """Acumula um novo upload no ciclo atual (união de produtos)."""
    uploads = st.session_state[_chave_uploads(e,f)]
    uploads.append(upload_info)
    st.session_state[_chave_uploads(e,f)] = uploads

def produtos_contados_no_ciclo(e, f) -> set:
    """Retorna união de todos os produtos contados nos uploads do ciclo."""
    todos = set()
    for u in obter_uploads(e, f):
        todos.update(u.get("produtos", []))
    return todos

def obter_contados(e, f):    return st.session_state.get(_chave_contados(e,f), {})
def obter_ciclos(e, f):      return st.session_state.get(_chave_ciclos(e,f), [])
def obter_ciclo_ativo(e, f): return st.session_state.get(_chave_ciclo_ativo(e,f))

def salvar_ciclo_ativo(e, f, ciclo):
    st.session_state[_chave_ciclo_ativo(e,f)] = ciclo

def fechar_ciclo(e, f):
    """Fecha o ciclo, marca todos os produtos contados e registra no histórico."""
    ciclo   = obter_ciclo_ativo(e, f)
    uploads = obter_uploads(e, f)
    if not ciclo:
        return

    # União de todos os produtos contados nos uploads
    todos_contados = produtos_contados_no_ciclo(e, f)
    data_iso = uploads[-1].get("data_iso", date.today().isoformat()) if uploads else date.today().isoformat()

    # Atualiza contados globais
    contados = st.session_state[_chave_contados(e,f)]
    for p in todos_contados:
        contados[p] = data_iso

    # Monta registro do ciclo
    produtos_lista = set(ciclo.get("produtos_lista", []))
    pct = len(todos_contados & produtos_lista) / len(produtos_lista) * 100 if produtos_lista else 0

    ciclo_fechado = {
        **ciclo,
        "uploads":          len(uploads),
        "produtos_contados":list(todos_contados),
        "cobertura_pct":    pct,
        "data_fechamento":  date.today().strftime("%d/%m/%Y"),
        "status":           "Concluído",
        # Metadados do último upload
        "num_inv":          uploads[-1].get("num_inv","—") if uploads else "—",
        "data":             uploads[-1].get("data","—")    if uploads else "—",
        "responsavel":      uploads[-1].get("responsavel","—") if uploads else "—",
        "acuracidade":      uploads[-1].get("acuracidade","—") if uploads else "—",
    }
    st.session_state[_chave_ciclos(e,f)].append(ciclo_fechado)
    st.session_state[_chave_ciclo_ativo(e,f)] = None
    st.session_state[_chave_uploads(e,f)]     = []

def resetar(e, f):
    st.session_state[_chave_contados(e,f)]    = {}
    st.session_state[_chave_ciclos(e,f)]      = []
    st.session_state[_chave_ciclo_ativo(e,f)] = None
    st.session_state[_chave_uploads(e,f)]     = []


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

    # Consolida localizações — soma WMS e ERP separadamente por produto
    if "Produto" in df.columns and len(df) > df["Produto"].nunique():
        # Colunas que devem ser somadas (saldos físicos)
        cols_soma  = [c for c in ["Saldo WMS", "Saldo ERP (Total)"] if c in df.columns]
        # Colunas fixas (iguais em todas as localizações do produto)
        cols_fixos = [c for c in ["Produto","Descrição","Empresa","Filial",
                                   "Vl Unit","Vl Total ERP"] if c in df.columns]
        df_soma = df.groupby("Produto", as_index=False)[cols_soma].sum()
        df_fixo = df[cols_fixos].drop_duplicates(subset=["Produto"], keep="first")
        df      = df_fixo.merge(df_soma, on="Produto", how="left")
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

    # Guarda a lista atual no session_state para cruzar com o upload
    st.session_state[f"ic_lista_atual_{_slug(empresa_sel,filial_sel)}"] = {
        "num_ciclo":      num_ciclo,
        "data_geracao":   date.today().strftime("%d/%m/%Y"),
        "label":          label,
        "qtd_lista":      len(df_exibir),
        "produtos_lista": df_exibir["Produto"].astype(str).tolist(),
    }

    # Upload do resultado WMS direto aqui
    st.markdown("---")
    # Progresso acumulado dos uploads anteriores
    uploads_anteriores = obter_uploads(empresa_sel, filial_sel)
    ja_contados_ciclo  = produtos_contados_no_ciclo(empresa_sel, filial_sel)
    lista_atual        = st.session_state.get(f"ic_lista_atual_{_slug(empresa_sel,filial_sel)}", {})
    produtos_lista     = set(lista_atual.get("produtos_lista", []))
    faltam             = produtos_lista - ja_contados_ciclo
    pct_ciclo          = len(ja_contados_ciclo & produtos_lista) / len(produtos_lista) * 100 if produtos_lista else 0
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
            c1.metric("Nº Inventário", res.get("num_inv","—"))
            c2.metric("Data",          res.get("data","—"))
            c3.metric("Responsável",   res.get("responsavel","—"))
            c4.metric("Acuracidade",   res.get("acuracidade","—"))

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
                acumular_upload(empresa_sel, filial_sel, {
                    "num_inv":     res.get("num_inv","—"),
                    "data":        res.get("data","—"),
                    "data_iso":    res.get("data_iso", date.today().isoformat()),
                    "responsavel": res.get("responsavel","—"),
                    "acuracidade": res.get("acuracidade","—"),
                    "produtos":    list(produtos_lista & produtos_wms),
                })
                st.success(f"Etapa adicionada! {len(novos_desta_etapa)} novos produtos contados.")
                st.rerun()

        except Exception as e:
            st.error(f"Erro ao processar arquivo: {e}")

    # Botão fechar — só libera com 100%
    st.markdown("---")
    if pct_ciclo >= 100:
        if st.button("🏁 Fechar inventário", key="ic_fechar", type="primary"):
            fechar_ciclo(empresa_sel, filial_sel)
            st.success("Inventário fechado e registrado no histórico KPMG!")
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
