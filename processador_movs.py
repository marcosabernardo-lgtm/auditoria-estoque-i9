import pandas as pd
import numpy as np
import unicodedata
from sqlalchemy import text
import streamlit as st

# --- TABELA DE EMPRESAS (Mapeamento que estava no seu Código M) ---
def get_df_empresas():
    data = [
        ["Tools 00", "Tools - Matriz"], ["Tools 01", "Tools - Filial"],
        ["Maquinas 00", "Maquinas - Matriz"], ["Maquinas 01", "Maquinas - Filial"], ["Maquinas 02", "Maquinas - Jundiai"],
        ["Robotica 00", "Robotica", "Matriz"], ["Robotica 01", "Robotica", "Jaragua"],
        ["Service 01", "Service - Matriz"], ["Service 02", "Service - Filial"], ["Service 03", "Service - Caxias"], ["Service 04", "Service - Jundiai"]
    ]
    return pd.DataFrame(data, columns=['Empresa_Cod_Filial', 'Empresa_Filial_Nome'])

def remover_acentos(texto):
    if not isinstance(texto, str): return texto
    return "".join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def limpar_id_produto(serie):
    return serie.astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(6)

def limpar_id_geral(serie, digitos):
    return serie.astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(digitos)

# --- FUNÇÃO QUE TRADUZ SEU CÓDIGO M ---
def tratar_notas_fiscais(list_files):
    df_emp_ref = get_df_empresas()
    all_movs = []

    for file in list_files:
        # Pega o nome após o "_" (Ex: bd_Tools.xlsx -> Tools)
        nome_arquivo = file.name.replace(".xlsx", "")
        emp_origem = nome_arquivo.split("_")[-1] if "_" in nome_arquivo else nome_arquivo
        emp_origem = remover_acentos(emp_origem)

        xls = pd.ExcelFile(file)
        for sheet in xls.sheet_names:
            # Filtra abas que são Entrada ou Saída
            if sheet.upper() in ["ENTRADA", "SAIDA", "ENTRADAS", "SAIDAS"]:
                df_temp = pd.read_excel(file, sheet_name=sheet)
                df_temp.columns = [str(c).strip().upper() for c in df_temp.columns]
                
                # Filtros do seu código M: ESTOQUE == 'S' e QUANTIDADE != 0
                if 'ESTOQUE' in df_temp.columns and 'QUANTIDADE' in df_temp.columns:
                    # No pandas, convertemos QUANTIDADE para numérico antes de filtrar
                    df_temp['QUANTIDADE'] = pd.to_numeric(df_temp['QUANTIDADE'], errors='coerce').fillna(0)
                    df_temp = df_temp[(df_temp['ESTOQUE'] == 'S') & (df_temp['QUANTIDADE'] != 0)].copy()
                
                # Adiciona colunas de controle
                df_temp['TIPOMOVIMENTO'] = "Entrada" if "ENTRADA" in sheet.upper() else "Saída"
                df_temp['EMPRESA_ARQUIVO'] = emp_origem
                all_movs.append(df_temp)

    if not all_movs: return pd.DataFrame()

    df_final = pd.concat(all_movs, ignore_index=True)
    
    # Seleção de Colunas do código M
    cols_m = ["EMPRESA_ARQUIVO", "FILIAL", "DOCUMENTO", "DIGITACAO", "NOTA DEVOLUCAO", "PRODUTO", 
              "DESCRICAO", "CENTRO CUSTO", "RAZAO SOCIAL", "QUANTIDADE", "PRECO UNITARIO", "TOTAL", "TIPOMOVIMENTO"]
    
    # Filtra apenas as colunas que existem
    df_final = df_final[[c for c in cols_m if c in df_final.columns]]

    # Limpeza final de IDs para cruzamento
    df_final['PRODUTO'] = limpar_id_produto(df_final['PRODUTO'])
    df_final['FILIAL'] = limpar_id_geral(df_final['FILIAL'], 2)
    
    # Join com Empresas (Mapeamento Matriz/Filial)
    df_final['CHAVE_JOIN'] = df_final['EMPRESA_ARQUIVO'] + " " + df_final['FILIAL']
    df_final = pd.merge(df_final, df_emp_ref, left_on='CHAVE_JOIN', right_on='Empresa_Cod_Filial', how='left')
    
    # Retorna as colunas finais renomeadas
    return df_final.drop(columns=['CHAVE_JOIN', 'Empresa_Cod_Filial'])

# --- FUNÇÃO DE CONSULTA AO BANCO (Puxa só 1 produto por vez) ---
def buscar_movimentacoes_nuvem(engine, produto_cod):
    try:
        query = text("SELECT * FROM movimentacoes WHERE PRODUTO = :p")
        return pd.read_sql(query, engine, params={"p": produto_cod})
    except:
        return pd.DataFrame()
