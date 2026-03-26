import pandas as pd
import numpy as np
import unicodedata
from sqlalchemy import text

def remover_acentos(texto):
    if not isinstance(texto, str): return texto
    return "".join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def limpar_id_produto(serie):
    return serie.astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(6)

def limpar_id_geral(serie, digitos):
    return serie.astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.zfill(digitos)

def get_df_empresas():
    data = {
        'Empresa_Cod_Filial': [
            "Tools 00", "Tools 01", "Maquinas 00", "Maquinas 01", "Maquinas 02",
            "Robotica 00", "Robotica 01", "Service 01", "Service 02", "Service 03", "Service 04"
        ],
        'Empresa_Filial_Nome': [
            "Tools - Matriz", "Tools - Filial", "Maquinas - Matriz", "Maquinas - Filial", "Maquinas - Jundiai",
            "Robotica - Matriz", "Robotica - Jaragua", "Service - Matriz", "Service - Filial", "Service - Caxias", "Service - Jundiai"
        ]
    }
    return pd.DataFrame(data)

def tratar_notas_fiscais(list_files):
    df_emp_ref = get_df_empresas()
    all_movs = []

    for file in list_files:
        nome_arquivo = file.name.replace(".xlsx", "")
        emp_origem = nome_arquivo.split("_")[-1] if "_" in nome_arquivo else nome_arquivo
        emp_origem = remover_acentos(emp_origem)

        xls = pd.ExcelFile(file)
        for sheet in xls.sheet_names:
            if any(x in sheet.upper() for x in ["ENTRADA", "SAIDA"]):
                df_temp = pd.read_excel(file, sheet_name=sheet)
                df_temp.columns = [str(c).strip().upper() for c in df_temp.columns]
                
                if 'ESTOQUE' in df_temp.columns and 'QUANTIDADE' in df_temp.columns:
                    df_temp['QUANTIDADE'] = pd.to_numeric(df_temp['QUANTIDADE'], errors='coerce').fillna(0)
                    df_temp = df_temp[(df_temp['ESTOQUE'] == 'S') & (df_temp['QUANTIDADE'] != 0)].copy()
                
                if not df_temp.empty:
                    df_temp['TIPOMOVIMENTO'] = "Entrada" if "ENTRADA" in sheet.upper() else "Saída"
                    df_temp['EMPRESA_ARQUIVO'] = emp_origem
                    all_movs.append(df_temp)

    if not all_movs: return pd.DataFrame()

    df_final = pd.concat(all_movs, ignore_index=True)
    
    # Padronização de colunas
    df_final.columns = [c.strip().upper() for c in df_final.columns]
    
    # --- NOVA LÓGICA DE REDUÇÃO (ÚLTIMA ENTRADA E ÚLTIMA SAÍDA) ---
    
    # 1. Converter a coluna de data (DIGITACAO) para o formato de data real
    df_final['DIGITACAO'] = pd.to_datetime(df_final['DIGITACAO'], errors='coerce')
    
    # 2. Limpar IDs para garantir que o agrupamento funcione
    df_final['PRODUTO'] = limpar_id_produto(df_final['PRODUTO'])
    df_final['FILIAL'] = limpar_id_geral(df_final['FILIAL'], 2)
    
    # 3. Ordenar pela data (mais recente primeiro)
    df_final = df_final.sort_values(by=['DIGITACAO'], ascending=False)
    
    # 4. Remover duplicatas: Manter apenas a 1ª ocorrência (a mais recente) 
    # de cada tipo (Entrada/Saída) por Produto, Filial e Empresa.
    df_reduzido = df_final.drop_duplicates(
        subset=['EMPRESA_ARQUIVO', 'FILIAL', 'PRODUTO', 'TIPOMOVIMENTO'], 
        keep='first'
    ).copy()
    
    # --- FIM DA LÓGICA DE REDUÇÃO ---

    cols_m = ["EMPRESA_ARQUIVO", "FILIAL", "DOCUMENTO", "DIGITACAO", "NOTA DEVOLUCAO", "PRODUTO", 
              "DESCRICAO", "CENTRO CUSTO", "RAZAO SOCIAL", "QUANTIDADE", "PRECO UNITARIO", "TOTAL", "TIPOMOVIMENTO"]
    
    df_reduzido = df_reduzido[[c for c in cols_m if c in df_reduzido.columns]]

    df_reduzido['CHAVE_JOIN'] = df_reduzido['EMPRESA_ARQUIVO'] + " " + df_reduzido['FILIAL']
    df_reduzido = pd.merge(df_reduzido, df_emp_ref, left_on='CHAVE_JOIN', right_on='Empresa_Cod_Filial', how='left')
    
    return df_reduzido.drop(columns=['CHAVE_JOIN', 'Empresa_Cod_Filial'])

def buscar_movimentacoes_nuvem(engine, produto_cod):
    try:
        query = text("SELECT * FROM movimentacoes WHERE PRODUTO = :p ORDER BY DIGITACAO DESC")
        return pd.read_sql(query, engine, params={"p": produto_cod})
    except:
        return pd.DataFrame()
