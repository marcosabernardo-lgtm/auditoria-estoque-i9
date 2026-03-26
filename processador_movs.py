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
        # Extrair nome da empresa do nome do arquivo (ex: 01_Tools.xlsx -> Tools)
        nome_arquivo = file.name.replace(".xlsx", "")
        partes = nome_arquivo.split("_")
        emp_origem = partes[-1].strip() if len(partes) > 1 else nome_arquivo
        emp_origem = remover_acentos(emp_origem)

        # Usar pd.ExcelFile para ler as abas sem re-abrir o arquivo toda hora
        xls = pd.ExcelFile(file)
        for sheet in xls.sheet_names:
            aba_nome = sheet.upper().strip()
            # Procura por abas que contenham ENTRADA ou SAIDA
            if "ENTRADA" in aba_nome or "SAIDA" in aba_nome:
                df_temp = pd.read_excel(xls, sheet_name=sheet)
                
                # Limpar cabeçalhos (remover espaços e colocar em maiúsculo)
                df_temp.columns = [str(c).strip().upper() for c in df_temp.columns]
                
                # Tratamento de Quantidade (converte string "10,00" para número 10.00)
                if 'QUANTIDADE' in df_temp.columns:
                    df_temp['QUANTIDADE'] = df_temp['QUANTIDADE'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                    df_temp['QUANTIDADE'] = pd.to_numeric(df_temp['QUANTIDADE'], errors='coerce').fillna(0)

                # Filtros do Código M: ESTOQUE == 'S' e QUANTIDADE != 0
                if 'ESTOQUE' in df_temp.columns and 'QUANTIDADE' in df_temp.columns:
                    # Garante que 'S' não tenha espaços
                    df_temp['ESTOQUE'] = df_temp['ESTOQUE'].astype(str).str.strip().upper()
                    df_temp = df_temp[(df_temp['ESTOQUE'] == 'S') & (df_temp['QUANTIDADE'] != 0)].copy()
                
                if not df_temp.empty:
                    df_temp['TIPOMOVIMENTO'] = "Entrada" if "ENTRADA" in aba_nome else "Saída"
                    df_temp['EMPRESA_ARQUIVO'] = emp_origem
                    
                    # Converter DIGITACAO para data real para podermos filtrar a última
                    if 'DIGITACAO' in df_temp.columns:
                        df_temp['DIGITACAO'] = pd.to_datetime(df_temp['DIGITACAO'], errors='coerce')
                    
                    all_movs.append(df_temp)

    if not all_movs:
        return pd.DataFrame()

    df_final = pd.concat(all_movs, ignore_index=True)
    
    # --- LÓGICA DE REDUÇÃO: ÚLTIMA ENTRADA E ÚLTIMA SAÍDA ---
    df_final['PRODUTO'] = limpar_id_produto(df_final['PRODUTO'])
    df_final['FILIAL'] = limpar_id_geral(df_final['FILIAL'], 2)
    
    # Ordenar pela data (mais recente primeiro)
    if 'DIGITACAO' in df_final.columns:
        df_final = df_final.sort_values(by='DIGITACAO', ascending=False)
    
    # Manter apenas a primeira ocorrência (a mais nova) de cada tipo por produto/filial
    df_reduzido = df_final.drop_duplicates(
        subset=['EMPRESA_ARQUIVO', 'FILIAL', 'PRODUTO', 'TIPOMOVIMENTO'],
        keep='first'
    ).copy()

    # Seleção de Colunas Finais
    cols_desejadas = ["EMPRESA_ARQUIVO", "FILIAL", "DOCUMENTO", "DIGITACAO", "NOTA DEVOLUCAO", "PRODUTO", 
                      "DESCRICAO", "CENTRO CUSTO", "RAZAO SOCIAL", "QUANTIDADE", "PRECO UNITARIO", "TOTAL", "TIPOMOVIMENTO"]
    
    df_reduzido = df_reduzido[[c for c in cols_desejadas if c in df_reduzido.columns]]

    # Join com a tabela de Nomes de Filiais
    df_reduzido['CHAVE_JOIN'] = df_reduzido['EMPRESA_ARQUIVO'] + " " + df_reduzido['FILIAL']
    df_reduzido = pd.merge(df_reduzido, df_emp_ref, left_on='CHAVE_JOIN', right_on='Empresa_Cod_Filial', how='left')
    
    return df_reduzido.drop(columns=['CHAVE_JOIN', 'Empresa_Cod_Filial'])

def buscar_movimentacoes_nuvem(engine, produto_cod):
    try:
        # Busca ordenada por data
        query = text("SELECT * FROM movimentacoes WHERE PRODUTO = :p ORDER BY DIGITACAO DESC")
        return pd.read_sql(query, engine, params={"p": produto_cod})
    except Exception as e:
        return pd.DataFrame()
