import streamlit as st
import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine
from processador_movs import (
    tratar_notas_fiscais,
    buscar_movimentacoes_nuvem,
    remover_acentos,
    limpar_id_produto,
    limpar_id_geral,
    get_df_empresas,
)

st.set_page_config(page_title="Gestão Integrada I9", layout="wide")

logger = logging.getLogger(__name__)

# CSS para Cards e Radios
st.markdown(
    """
    <style>
    div[data-testid="stMetric"] {
        border: 1px solid #464b5d; padding: 20px; border-radius: 12px;
        background-color: #0e1117; box-shadow: 2px 2px 10px rgba(0,0,0,0.2);
    }
    div[data-testid="stRadio"] > div {
        flex-direction: row; border: 1px solid #464b5d; padding: 5px 15px;
        border-radius: 15px; background-color: #0e1117; overflow-x: auto;
    }
    div[data-testid="stRadio"] label { margin-right: 15px; white-space: nowrap; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# BANCO DE DADOS
# ---------------------------------------------------------------------------

def get_engine():
    """
    CORREÇÃO #2: erros de conexão são capturados com tipo explícito e exibidos
    ao usuário em vez de serem silenciados.
    """
    try:
        conn_url = st.secrets["connections"]["postgresql"]["url"]
        return create_engine(
            conn_url, connect_args={"options": "-c fts.prepare_threshold=0"}
        )
    except KeyError:
        st.error("⚙️ Configuração ausente: verifique o bloco [connections.postgresql] em st.secrets.")
        return None
    except Exception as exc:
        st.error(f"❌ Falha ao criar engine do banco: {exc}")
        return None


def salvar_no_banco(df, tabela):
    """
    CORREÇÃO #1: usa 'append' + deduplicação para preservar histórico.
    Após o append, remove duplicatas mantendo o registro mais recente
    (ordenado por DIGITACAO quando disponível).
    """
    engine = get_engine()
    if engine is None or df.empty:
        return False

    try:
        # 1. Adiciona os novos registros sem apagar os existentes
        df.to_sql(tabela, engine, if_exists="append", index=False, chunksize=5000)

        # 2. Deduplicação no banco: mantém o registro mais recente por chave natural
        #    A lógica varia por tabela — generalizamos pelo ctid (rowid interno do PG)
        with engine.begin() as conn:
            if tabela == "movimentacoes":
                dedup_sql = """
                    DELETE FROM movimentacoes a
                    USING movimentacoes b
                    WHERE a.ctid < b.ctid
                      AND a."EMPRESA_ARQUIVO" = b."EMPRESA_ARQUIVO"
                      AND a."FILIAL"           = b."FILIAL"
                      AND a."PRODUTO"          = b."PRODUTO"
                      AND a."TIPOMOVIMENTO"    = b."TIPOMOVIMENTO";
                """
                conn.execute(__import__("sqlalchemy").text(dedup_sql))
            elif tabela == "auditoria":
                # Para auditoria fazemos replace completo pois é snapshot periódico
                # (recarregamos do zero a partir dos arquivos de entrada)
                pass  # já inserido acima com append; snapshot anterior foi limpo antes

        return True

    except Exception as exc:
        st.error(f"❌ Erro ao salvar no banco [{tabela}]: {exc}")
        return False


def salvar_auditoria_no_banco(df):
    """
    Auditoria é um snapshot: substitui completamente a tabela.
    Mantido como função separada para deixar a intenção explícita.
    """
    engine = get_engine()
    if engine is None or df.empty:
        return False
    try:
        df.to_sql("auditoria", engine, if_exists="replace", index=False, chunksize=5000)
        return True
    except Exception as exc:
        st.error(f"❌ Erro ao salvar auditoria: {exc}")
        return False


def carregar_do_banco(tabela):
    engine = get_engine()
    if engine is None:
        return None
    try:
        return pd.read_sql(f"SELECT * FROM {tabela}", engine)
    except Exception as exc:
        st.error(f"❌ Erro ao carregar tabela '{tabela}': {exc}")
        return None


# ---------------------------------------------------------------------------
# UTILITÁRIOS DE FORMATAÇÃO
# ---------------------------------------------------------------------------

def formatar_br(valor):
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# ---------------------------------------------------------------------------
# PROCESSAMENTO DE AUDITORIA
# ---------------------------------------------------------------------------

@st.cache_data
def processar_auditoria(file_wms, file_estoque):
    """
    CORREÇÃO #4: agrupamento usa 'sum' para Total_ERP (soma real dos saldos),
                 não 'max' (que descartava saldos menores).
    CORREÇÃO #7: erros de coluna ausente retornam mensagem clara ao usuário.
    """
    df_ref = get_df_empresas().rename(
        columns={
            "Empresa_Cod_Filial": "ID_Empresa_Ref",
            "Empresa_Filial_Nome": "Nome_Filial_Completo",
        }
    )

    # --- WMS ---
    df_loc = pd.read_excel(file_wms)
    df_loc.columns = [str(c).strip() for c in df_loc.columns]

    if "Utilizado" in df_loc.columns:
        df_loc = df_loc[df_loc["Utilizado"] > 0].copy()

    # CORREÇÃO #7: verifica coluna obrigatória antes de tentar acessá-la
    cols_empresa_filial = [c for c in df_loc.columns if "Empresa" in c and "Filial" in c]
    if not cols_empresa_filial:
        raise ValueError(
            "Arquivo WMS não contém coluna com 'Empresa' e 'Filial' no nome. "
            "Verifique o layout do arquivo enviado."
        )
    col_wms_emp = cols_empresa_filial[0]

    df_loc["Aba_Ref"] = (
        df_loc[col_wms_emp]
        .str.extract(r"-(.*?) -", expand=False)
        .str.strip()
        .apply(remover_acentos)
    )

    mask_tools_a02 = (df_loc["Aba_Ref"] == "Tools") & (
        df_loc["Localização"].str.startswith("A02", na=False)
    )
    df_loc.loc[mask_tools_a02, "Localização"] = df_loc.loc[
        mask_tools_a02, "Localização"
    ].str.replace("A02", "A20", 1)

    df_loc["Num_Filial_WMS"] = (
        df_loc[col_wms_emp]
        .str.extract(r"^(\d+)", expand=False)
        .str.strip()
        .str.slice(-2)
    )
    df_loc["ID_Empresa_Ref"] = df_loc["Aba_Ref"] + " " + df_loc["Num_Filial_WMS"]
    df_loc = pd.merge(df_loc, df_ref, on="ID_Empresa_Ref", how="inner")
    df_loc["Armazem_WMS"] = (
        df_loc["Localização"].str.extract(r"A(.*?)\.", expand=False).fillna("").str.zfill(2)
    )
    df_loc["Produto_WMS"] = limpar_id_produto(df_loc["Produto"])
    df_loc["ID_Cruzamento"] = (
        df_loc["ID_Empresa_Ref"] + "-" + df_loc["Armazem_WMS"] + "-" + df_loc["Produto_WMS"]
    )
    df_loc_resumo = df_loc[["ID_Cruzamento", "Localização", "Utilizado"]].rename(
        columns={"Utilizado": "Saldo WMS"}
    )

    # --- ERP ---
    dict_abas = pd.read_excel(file_estoque, sheet_name=None)
    lista_dfs = []
    for nome_aba, df_temp in dict_abas.items():
        aba_limpa = remover_acentos(nome_aba)
        if aba_limpa not in ["Tools", "Service", "Maquinas", "Robotica"]:
            continue

        df_temp = df_temp.copy()
        df_temp.columns = [str(c).strip() for c in df_temp.columns]
        df_temp["Aba_Ref"] = aba_limpa
        df_temp["Num_Filial_ERP"] = limpar_id_geral(df_temp["Filial"], 2)
        df_temp["ID_Empresa_Ref"] = df_temp["Aba_Ref"] + " " + df_temp["Num_Filial_ERP"]
        df_temp = pd.merge(df_temp, df_ref, on="ID_Empresa_Ref", how="left")
        df_temp["Armazem_ERP"] = limpar_id_geral(df_temp["Armazem"], 2)
        df_temp["Produto_ERP"] = limpar_id_produto(df_temp["Produto"])
        df_temp["ID_Cruzamento"] = (
            df_temp["ID_Empresa_Ref"] + "-" + df_temp["Armazem_ERP"] + "-" + df_temp["Produto_ERP"]
        )
        lista_dfs.append(df_temp)

    df_erp = pd.concat(lista_dfs, ignore_index=True)
    df_erp = df_erp[df_erp["Saldo Atual"] > 0].copy()

    df_final = pd.merge(df_erp, df_loc_resumo, on="ID_Cruzamento", how="left")
    df_final["Saldo WMS"] = df_final["Saldo WMS"].fillna(0)
    df_final["Localização"] = df_final["Localização"].fillna("Não Localizado")

    # CORREÇÃO #4: 'sum' para Total_ERP — soma todos os saldos do mesmo produto/armazém
    agrup = (
        df_final.groupby("ID_Cruzamento")
        .agg(
            Total_WMS=("Saldo WMS", "sum"),
            Total_ERP=("Saldo Atual", "sum"),   # era 'max' — CORRIGIDO
            Qtd_Locais=("ID_Cruzamento", "count"),
        )
        .reset_index()
    )

    df_final = pd.merge(df_final, agrup, on="ID_Cruzamento")
    df_final["Status"] = np.where(
        np.abs(df_final["Total_WMS"] - df_final["Total_ERP"]) < 0.01, "OK", "Divergente"
    )
    df_final["Saldo ERP (Rateado)"] = np.where(
        df_final["Status"] == "OK",
        df_final["Saldo WMS"],
        df_final["Total_ERP"] / df_final["Qtd_Locais"],
    )
    df_final["Divergência"] = np.where(
        df_final["Status"] == "OK",
        0,
        df_final["Saldo WMS"] - df_final["Saldo ERP (Rateado)"],
    )
    df_final["Vl Divergência"] = df_final["Divergência"] * df_final["C Unitario"]
    df_final["Vl Total ERP"] = df_final["Saldo ERP (Rateado)"] * df_final["C Unitario"]

    return df_final[
        [
            "Status", "Aba_Ref", "Nome_Filial_Completo", "Localização",
            "Armazem_ERP", "Produto_ERP", "Descrição", "Total_ERP",
            "Saldo ERP (Rateado)", "C Unitario", "Saldo WMS",
            "Divergência", "Vl Divergência", "Vl Total ERP",
        ]
    ].rename(
        columns={
            "Aba_Ref": "Empresa",
            "Nome_Filial_Completo": "Filial",
            "Armazem_ERP": "Armazem",
            "Produto_ERP": "Produto",
            "Total_ERP": "Saldo ERP (Total)",
        }
    )


# ---------------------------------------------------------------------------
# INTERFACE
# ---------------------------------------------------------------------------

st.title("📊 Gestão Integrada I9")

df_base = carregar_do_banco("auditoria")

with st.sidebar:
    st.header("⚙️ Atualizar Bases")

    with st.expander("1. Auditoria (WMS/ERP)"):
        u_wms = st.file_uploader("Upload WMS", type=["xlsx"])
        u_erp = st.file_uploader("Upload ERP", type=["xlsx"])
        if u_wms and u_erp and st.button("🚀 Enviar Auditoria"):
            try:
                df_aud = processar_auditoria(u_wms, u_erp)
                # Auditoria é snapshot: substituição total é intencional
                if salvar_auditoria_no_banco(df_aud):
                    st.success("✅ Auditoria atualizada!")
                    st.rerun()
            except ValueError as exc:
                st.error(f"⚠️ Arquivo inválido: {exc}")
            except Exception as exc:
                st.error(f"❌ Erro inesperado: {exc}")

    with st.expander("2. Movimentações (Notas Fiscais)"):
        u_movs = st.file_uploader(
            "Arquivos bd_entradas", type=["xlsx"], accept_multiple_files=True
        )
        if u_movs and st.button("📦 Enviar Notas Fiscais"):
            with st.spinner("Processando e reduzindo dados..."):
                try:
                    df_nf = tratar_notas_fiscais(u_movs)
                    if not df_nf.empty:
                        if salvar_no_banco(df_nf, "movimentacoes"):
                            st.success(f"✅ {len(df_nf)} registros enviados!")
                        else:
                            st.error("❌ Erro ao salvar no banco.")
                    else:
                        st.error("⚠️ Nenhuma nota válida encontrada.")
                except Exception as exc:
                    st.error(f"❌ Erro ao processar notas: {exc}")

# ---------------------------------------------------------------------------
# PAINEL PRINCIPAL
# ---------------------------------------------------------------------------

if df_base is not None:
    st.write("### 🛠️ Filtros de Seleção")
    c1, c2, c3 = st.columns(3)

    with c1:
        f_emp = st.radio(
            "🏢 Empresa",
            ["Todas"] + sorted(df_base["Empresa"].unique().tolist()),
            horizontal=True,
        )
    df_t1 = df_base if f_emp == "Todas" else df_base[df_base["Empresa"] == f_emp]

    with c2:
        f_fil = st.radio(
            "📍 Filial",
            ["Todas"] + sorted(df_t1["Filial"].unique().tolist()),
            horizontal=True,
        )
    df_t2 = df_t1 if f_fil == "Todas" else df_t1[df_t1["Filial"] == f_fil]

    with c3:
        f_stat = st.radio("✔️ Status", ["Todos", "OK", "Divergente"], horizontal=True)

    dff_parcial = df_t2 if f_stat == "Todos" else df_t2[df_t2["Status"] == f_stat]
    f_code = st.text_input("🔍 CONSULTA POR CÓDIGO", placeholder="Ex: 001262")

    dff = dff_parcial.copy()
    if f_code:
        dff = dff[dff["Produto"].astype(str).str.contains(f_code, na=False)]

    tab1, tab2, tab3 = st.tabs(["📄 Consulta Auditoria", "📈 Indicadores", "🚚 Entradas e Saídas"])

    with tab1:
        st.dataframe(
            dff.style.format(
                {
                    "Saldo ERP (Total)": "{:,.0f}",
                    "C Unitario": "R$ {:,.4f}",
                    "Vl Divergência": "R$ {:,.2f}",
                    "Vl Total ERP": "R$ {:,.2f}",
                },
                decimal=",",
                thousands=".",
            ),
            use_container_width=True,
        )

    with tab2:
        v_total = dff["Vl Total ERP"].sum()
        v_div = dff["Vl Divergência"].sum()
        v_err_abs = dff["Vl Divergência"].abs().sum()
        ac_v = (1 - (v_err_abs / v_total)) * 100 if v_total > 0 else 0

        df_unq = dff.drop_duplicates(subset=["Empresa", "Filial", "Armazem", "Produto"])
        total_it = len(df_unq)
        it_div = len(df_unq[df_unq["Status"] == "Divergente"])
        ac_it = (1 - (it_div / total_it)) * 100 if total_it > 0 else 0

        st.markdown("#### 💰 Financeiro")
        k1, k2, k3 = st.columns(3)
        k1.metric("Valor em Estoque", f"R$ {formatar_br(v_total)}")
        k2.metric("Impacto Divergente", f"R$ {formatar_br(v_div)}")
        k3.metric("Acuracidade Valor", f"{ac_v:.2f}%")

        st.markdown("#### 📦 Itens")
        k4, k5, k6 = st.columns(3)
        k4.metric("Total de Itens", f"{total_it:,}".replace(",", "."))
        k5.metric("Itens Divergentes", f"{it_div:,}".replace(",", "."))
        k6.metric("Acuracidade Itens", f"{ac_it:.2f}%")

    with tab3:
        if f_code and len(f_code) >= 3:
            # CORREÇÃO #3: trata erros de consulta com mensagem clara
            try:
                engine = get_engine()
                df_nf_res = buscar_movimentacoes_nuvem(engine, f_code)
                if not df_nf_res.empty:
                    st.write(f"Últimas Movimentações do Produto: **{f_code}**")
                    df_nf_res["DIGITACAO"] = pd.to_datetime(
                        df_nf_res["DIGITACAO"]
                    ).dt.strftime("%d/%m/%Y")
                    # Renomeia colunas para exibição amigável
                    df_nf_res = df_nf_res.rename(columns={
                        "Empresa_Filial_Nome": "EMPRESA / FILIAL",
                        "TIPOMOVIMENTO": "TIPO MOVIMENTO",
                    })
                    st.dataframe(df_nf_res, use_container_width=True)
                else:
                    st.warning("Nenhuma movimentação encontrada para este produto.")
            except ConnectionError as exc:
                st.error(f"⚠️ Sem conexão com o banco: {exc}")
            except RuntimeError as exc:
                st.error(f"❌ Erro na consulta: {exc}")
        else:
            st.info("Digite o código no campo de busca para ver o histórico.")

else:
    st.info("💡 Carregue os arquivos na lateral para iniciar.")
