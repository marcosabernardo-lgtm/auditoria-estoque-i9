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


def calcular_score(df, contados):
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

    col_e, col_f = st.columns(2)
    with col_e:
        empresa_sel = st.selectbox("🏢 Empresa",
            sorted(df_jlle["Empresa"].dropna().unique()), key="ic_emp")
    with col_f:
        filial_sel = st.selectbox("📍 Filial",
            sorted(df_jlle[df_jlle["Empresa"]==empresa_sel]["Filial"].dropna().unique()), key="ic_fil")

    label     = f"{empresa_sel} — {filial_sel}"
    df_filial = df_jlle[(df_jlle["Empresa"]==empresa_sel)&(df_jlle["Filial"]==filial_sel)].copy()
    if df_filial.empty:
        st.warning(f"Sem dados para **{label}**."); return

    engine_db   = st.session_state.get("_engine")
    contados    = db_obter_contados(engine_db, empresa_sel, filial_sel)
    ciclos      = db_obter_ciclos(engine_db, empresa_sel, filial_sel)
    ciclo_ativo = db_obter_ciclo_ativo(engine_db, empresa_sel, filial_sel)

    if st.session_state.pop("ic_fechado_msg", False):
        st.success("✅ Inventário fechado e registrado no histórico KPMG!")

    df_score   = calcular_score(df_filial, contados)
    total_skus = len(df_score)
    total_cont = sum(1 for p in df_score["Produto"].astype(str) if p in contados)
    pct_cob    = (total_cont/total_skus*100) if total_skus>0 else 0

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
    c1,c2,c3,c4 = st.columns(4)
    b1 = _card(c1,1,"Gerar lista","Define o ciclo e gera a lista",
               ativo=(etapa_nav==1), concluido=(ciclo_ativo is not None), chave="ic_n1")
    b2 = _card(c2,2,"Upload WMS","Importa o resultado do WMS",
               ativo=(etapa_nav==2), concluido=(len(_uploads)>0), chave="ic_n2")
    b3 = _card(c3,3,"Adicionar etapa","Confirma e acumula uploads",
               ativo=(etapa_nav==3), concluido=(pct_ciclo>=100), chave="ic_n3")
    b4 = _card(c4,4,"Fechar inventário","Registra no histórico KPMG",
               ativo=(etapa_nav==4), concluido=(len(ciclos)>0), chave="ic_n4")

    if b1: st.session_state["ic_etapa_nav"]=1; st.rerun()
    if b2: st.session_state["ic_etapa_nav"]=2; st.rerun()
    if b3: st.session_state["ic_etapa_nav"]=3; st.rerun()
    if b4: st.session_state["ic_etapa_nav"]=4; st.rerun()

    st.markdown("---")

    # ── ETAPA 1 ───────────────────────────────────────────────────────────
    if etapa_nav == 1:
        st.markdown("### 1. Gerar lista do ciclo")
        data_aud = st.session_state.get("_data_auditoria")
        col_a,col_b = st.columns([3,2])
        col_a.caption(f"📅 Dados carregados em: **{data_aud or 'esta sessão'}**")
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
                st.dataframe(df_wms[["Codigo","Descricao","Qtd Antes","Qtd Depois","Qtd Diferença","Acuracidade","Status"]],
                             use_container_width=True, hide_index=True)

                st.session_state["ic_upload_pendente"] = {
                    "num_inv":res.get("num_inv","—"),"data":res.get("data","—"),
                    "data_iso":res.get("data_iso",date.today().isoformat()),
                    "responsavel":res.get("responsavel","—"),"acuracidade":res.get("acuracidade","—"),
                    "produtos":list(pl_ciclo & prods_wms),
                }
                st.info("✔ Arquivo carregado. Vá para **Adicionar etapa** para confirmar.")
            except Exception as e:
                st.error(f"Erro ao processar arquivo: {e}")

    # ── ETAPA 3 ───────────────────────────────────────────────────────────
    elif etapa_nav == 3:
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
                    st.success(f"✅ Etapa adicionada! {len(up['produtos'])} produtos contados.")
                    st.rerun()
                except Exception as err:
                    st.error(f"Erro ao adicionar: {err}")
        else:
            st.info("Faça o upload na etapa **Upload WMS** primeiro, depois volte aqui para confirmar.")

    # ── ETAPA 4 ───────────────────────────────────────────────────────────
    elif etapa_nav == 4:
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
                if st.button("🏁 Fechar inventário", key="ic_fechar", type="primary"):
                    todos = set()
                    for u in _uploads: todos.update(str(p).strip().zfill(6) for p in u.get("produtos",[]))
                    data_iso = _uploads[-1].get("data_iso",date.today().isoformat()) if _uploads else date.today().isoformat()
                    pct_f    = len(todos & pl_ciclo)/len(pl_ciclo)*100 if pl_ciclo else 0
                    cf = {**ciclo_ativo,"uploads":len(_uploads),"produtos_contados":list(todos),
                          "cobertura_pct":pct_f,"status":"Concluído",
                          "num_inv":_uploads[-1].get("num_inv","—") if _uploads else "—",
                          "data":_uploads[-1].get("data","—") if _uploads else "—",
                          "responsavel":_uploads[-1].get("responsavel","—") if _uploads else "—",
                          "acuracidade":_uploads[-1].get("acuracidade","—") if _uploads else "—"}
                    db_gravar_ciclo(engine_db,empresa_sel,filial_sel,cf)
                    db_marcar_contados(engine_db,empresa_sel,filial_sel,list(todos),
                                       data=data_iso,num_ciclo=ciclo_ativo.get("num_ciclo",""))
                    db_fechar_ciclo_ativo(engine_db,empresa_sel,filial_sel)
                    st.session_state["ic_fechado_msg"]=True
                    st.session_state["ic_etapa_nav"]=1
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

        if ciclos:
            st.markdown("---")
            with st.expander(f"📋 Histórico KPMG — {len(ciclos)} ciclo(s)"):
                df_h = pd.DataFrame([{"Nº Ciclo":c.get("num_ciclo","—"),"Data Contagem":c.get("data","—"),
                    "Responsável":c.get("responsavel","—"),"Acuracidade":c.get("acuracidade","—"),
                    "SKUs Contados":c.get("qtd_contados",0),"Cobertura %":f"{c.get('cobertura_pct',0):.1f}%",
                    "Status":c.get("status","—")} for c in ciclos])
                st.dataframe(df_h, use_container_width=True, hide_index=True)
                col_dl,col_rs = st.columns([3,1])
                with col_dl:
                    st.download_button("📥 Exportar Histórico KPMG",
                        data=gerar_xlsx_historico(ciclos,label),
                        file_name=f"historico_kpmg_{empresa_sel}_{filial_sel}_{date.today().strftime('%d%m%Y')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                with col_rs:
                    if st.button("🔄 Novo período",key="ic_reset"):
                        db_resetar_tudo(engine_db,empresa_sel,filial_sel)
                        st.session_state["ic_etapa_nav"]=1
                        st.success("Novo período iniciado!"); st.rerun()
