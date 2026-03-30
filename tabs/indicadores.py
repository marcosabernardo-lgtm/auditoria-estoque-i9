import streamlit as st
import pandas as pd

def render(df_jlle, formatar_br_func):
    if df_jlle.empty:
        st.warning("Sem dados de Joinville para calcular indicadores.")
        return

    # ── Deduplica por produto único (Empresa + Filial + Armazem + Produto) ─────
    # Remove duplicatas de localização para não inflar valores financeiros
    # Vl Total ERP e Vl Divergência já estão calculados por produto no processador
    df_prod = df_jlle.drop_duplicates(
        subset=["Empresa", "Filial", "Armazem", "Produto"],
        keep="first"
    ).copy()

    # ── Financeiro ─────────────────────────────────────────────────────────────
    # Vl Total ERP = Saldo ERP (Total) × Vl Unit — valor real do produto no ERP
    if "Vl Total ERP" in df_prod.columns:
        v_total = pd.to_numeric(df_prod["Vl Total ERP"], errors="coerce").fillna(0).sum()
    else:
        v_total = 0

    # Vl Divergência: soma por produto único (já calculado por localização no processador)
    # Para indicador financeiro, agrupamos por produto e pegamos o total divergente
    if "Vl Divergência" in df_jlle.columns:
        v_err = pd.to_numeric(df_jlle["Vl Divergência"], errors="coerce").fillna(0).abs().sum()
    else:
        v_err = 0

    ac_v = (1 - (v_err / v_total)) * 100 if v_total > 0 else 0

    st.markdown("### 💰 Financeiro (Joinville)")
    k1, k2, k3 = st.columns(3)
    k1.metric("ESTOQUE TOTAL",     f"R$ {formatar_br_func(v_total)}")
    k2.metric("VALOR DIVERGENTE",  f"R$ {formatar_br_func(v_err)}")
    k3.metric("ACURACIDADE VALOR", f"{ac_v:.2f}%")

    # ── Itens ──────────────────────────────────────────────────────────────────
    # Conta produtos únicos (sem multiplicar por localização)
    total_itens = len(df_prod)
    div_itens   = len(df_prod[df_prod["Status"] == "Divergente"])
    ac_it = (1 - (div_itens / total_itens)) * 100 if total_itens > 0 else 100

    st.markdown("### 📦 Itens (Joinville)")
    k4, k5, k6 = st.columns(3)
    k4.metric("TOTAL DE ITENS",    f"{total_itens:,}".replace(",", "."))
    k5.metric("ITENS DIVERGENTES", f"{div_itens:,}".replace(",", "."))
    k6.metric("ACURACIDADE ITENS", f"{ac_it:.2f}%")
