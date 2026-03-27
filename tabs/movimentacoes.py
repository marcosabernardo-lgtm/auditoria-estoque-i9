import streamlit as st
import pandas as pd

def render(f_code_padded, engine, buscar_func, estilizar_func, to_float_func):
    if f_code_padded:
        df_nf = buscar_func(engine, f_code_padded)
        if not df_nf.empty:
            df_nf = df_nf.drop_duplicates()
            
            # Lógica de pegar o último de cada filial/tipo
            df_nf["DIGITACAO"] = pd.to_datetime(df_nf["DIGITACAO"])
            df_nf = df_nf.sort_values(by="DIGITACAO", ascending=False)
            df_nf = df_nf.drop_duplicates(subset=["Empresa_Filial_Nome", "TIPOMOVIMENTO"], keep="first")
            
            df_nf["DIGITACAO"] = df_nf["DIGITACAO"].dt.strftime("%d/%m/%Y")
            
            if "Empresa_Filial_Nome" in df_nf.columns:
                split = df_nf["Empresa_Filial_Nome"].str.split(" - ", n=1, expand=True)
                df_nf.insert(0, "Filial", split[1].fillna(""))
                df_nf.insert(0, "Empresa", split[0].fillna(""))
                df_nf = df_nf.drop(columns=["Empresa_Filial_Nome"])
            
            df_nf = df_nf.rename(columns={
                "TIPOMOVIMENTO": "Tipo Movimento", "DOCUMENTO": "Documento", "DIGITACAO": "Digitação",
                "NOTA_DEVOLUCAO": "Nota Devolução", "PRODUTO": "Produto", "DESCRICAO": "Descrição",
                "CENTRO_CUSTO": "Centro Custo", "RAZAO_SOCIAL": "Razão Social",
                "QUANTIDADE": "Qtd", "PRECO_UNITARIO": "Vl Unit", "TOTAL": "Vl Total"
            })

            if "Nota Devolução" in df_nf.columns:
                df_nf["Nota Devolução"] = df_nf["Nota Devolução"].astype(str).str.replace(".0", "", regex=False).replace(["None", "nan", "nan.0"], "")
                df_nf["Nota Devolução"] = df_nf["Nota Devolução"].apply(lambda x: x.zfill(9) if x != "" else "")
            
            if "Centro Custo" in df_nf.columns:
                df_nf["Centro Custo"] = df_nf["Centro Custo"].astype(str).str.replace(".0", "", regex=False).replace(["None", "nan"], "")
            
            for col in ["Vl Unit", "Vl Total"]:
                if col in df_nf.columns: df_nf[col] = to_float_func(df_nf[col])

            st.write(f"### 🕒 Últimas Movimentações (Entrada/Saída) por Filial: {f_code_padded}")
            st.dataframe(estilizar_func(df_nf), use_container_width=True, hide_index=True)
        else:
            st.warning("Nenhuma movimentação para o código informado.")