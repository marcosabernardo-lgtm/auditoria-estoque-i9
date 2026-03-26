import pandas as pd
import numpy as np
import unicodedata
import logging
from sqlalchemy import text

# Configuração de log para registrar avisos sem interromper o fluxo
logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# UTILITÁRIOS
# ---------------------------------------------------------------------------

def remover_acentos(texto):
    if not isinstance(texto, str):
        return texto
    return "".join(
        c for c in unicodedata.normalize("NFD", texto)
        if unicodedata.category(c) != "Mn"
    )


def limpar_id_produto(serie):
    """
    Normaliza código de produto: remove '.0', espaços e aplica zero-fill.
    CORREÇÃO #6: zfill dinâmico — respeita códigos com mais de 6 dígitos.
    """
    serie_limpa = (
        serie.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
    )
    max_len = serie_limpa.str.len().max()
    pad = max(6, int(max_len)) if pd.notna(max_len) else 6
    return serie_limpa.str.zfill(pad)


def limpar_id_geral(serie, digitos):
    return (
        serie.astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
        .str.zfill(digitos)
    )


def get_df_empresas():
    data = {
        "Empresa_Cod_Filial": [
            "Tools 00", "Tools 01", "Maquinas 00", "Maquinas 01", "Maquinas 02",
            "Robotica 00", "Robotica 01", "Service 01", "Service 02", "Service 03", "Service 04",
        ],
        "Empresa_Filial_Nome": [
            "Tools - Matriz", "Tools - Filial", "Maquinas - Matriz", "Maquinas - Filial",
            "Maquinas - Jundiai", "Robotica - Matriz", "Robotica - Jaragua",
            "Service - Matriz", "Service - Filial", "Service - Caxias", "Service - Jundiai",
        ],
    }
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# PROCESSAMENTO DE NOTAS FISCAIS
# ---------------------------------------------------------------------------

def tratar_notas_fiscais(list_files):
    """
    Lê arquivos de entrada/saída, filtra, deduplica e retorna DataFrame pronto
    para persistência no banco.

    CORREÇÕES aplicadas:
    #5  – Coluna 'NOTA DEVOLUCAO' renomeada para 'NOTA_DEVOLUCAO' (sem espaço).
    #8  – Colunas ausentes são registradas em log em vez de descartadas silenciosamente.
    """
    df_emp_ref = get_df_empresas()
    all_movs = []

    # EMPRESA_ARQUIVO e FILIAL usadas apenas para join — excluídas do resultado final
    cols_finais = [
        "DOCUMENTO", "DIGITACAO", "NOTA_DEVOLUCAO", "PRODUTO",
        "DESCRICAO", "CENTRO_CUSTO", "RAZAO_SOCIAL",
        "QUANTIDADE", "PRECO_UNITARIO", "TOTAL", "TIPOMOVIMENTO",
    ]

    for file in list_files:
        nome_arquivo = file.name.replace(".xlsx", "")
        partes = nome_arquivo.split("_")
        emp_origem = remover_acentos(partes[-1].strip() if len(partes) > 1 else nome_arquivo)

        xls = pd.ExcelFile(file)
        for sheet in xls.sheet_names:
            aba_nome = sheet.upper().strip()
            if "ENTRADA" not in aba_nome and "SAIDA" not in aba_nome:
                continue

            df_temp = pd.read_excel(xls, sheet_name=sheet)

            # Normaliza cabeçalhos: remove espaços, acentos e padroniza separadores
            df_temp.columns = [
                remover_acentos(str(c).strip().upper()).replace(" ", "_")
                for c in df_temp.columns
            ]

            # Tratamento de QUANTIDADE
            if "QUANTIDADE" in df_temp.columns:
                df_temp["QUANTIDADE"] = (
                    df_temp["QUANTIDADE"]
                    .astype(str)
                    .str.replace(".", "", regex=False)
                    .str.replace(",", ".", regex=False)
                )
                df_temp["QUANTIDADE"] = pd.to_numeric(
                    df_temp["QUANTIDADE"], errors="coerce"
                ).fillna(0)

            # Filtros: ESTOQUE == 'S' e QUANTIDADE != 0
            if "ESTOQUE" in df_temp.columns and "QUANTIDADE" in df_temp.columns:
                df_temp["ESTOQUE"] = df_temp["ESTOQUE"].astype(str).str.strip().str.upper()
                df_temp = df_temp[
                    (df_temp["ESTOQUE"] == "S") & (df_temp["QUANTIDADE"] != 0)
                ].copy()

            if df_temp.empty:
                continue

            df_temp["TIPOMOVIMENTO"] = "Entrada" if "ENTRADA" in aba_nome else "Saída"
            df_temp["EMPRESA_ARQUIVO"] = emp_origem

            if "DIGITACAO" in df_temp.columns:
                df_temp["DIGITACAO"] = pd.to_datetime(df_temp["DIGITACAO"], errors="coerce")

            all_movs.append(df_temp)

    if not all_movs:
        logger.warning("tratar_notas_fiscais: nenhuma aba válida encontrada nos arquivos enviados.")
        return pd.DataFrame()

    df_final = pd.concat(all_movs, ignore_index=True)

    # Normaliza chaves de cruzamento
    df_final["PRODUTO"] = limpar_id_produto(df_final["PRODUTO"])
    df_final["FILIAL"] = limpar_id_geral(df_final["FILIAL"], 2)

    if "DIGITACAO" in df_final.columns:
        df_final = df_final.sort_values(by="DIGITACAO", ascending=False)

    # Mantém apenas a movimentação mais recente por produto/filial/tipo
    df_reduzido = df_final.drop_duplicates(
        subset=["EMPRESA_ARQUIVO", "FILIAL", "PRODUTO", "TIPOMOVIMENTO"],
        keep="first",
    ).copy()

    # CORREÇÃO #8: registra em log colunas esperadas que estão ausentes
    cols_presentes = [c for c in cols_finais if c in df_reduzido.columns]
    cols_ausentes = [c for c in cols_finais if c not in df_reduzido.columns]
    if cols_ausentes:
        logger.warning(
            "tratar_notas_fiscais: colunas esperadas não encontradas e ignoradas: %s",
            cols_ausentes,
        )

    df_reduzido = df_reduzido[cols_presentes]

    # Padroniza DOCUMENTO com zero-fill (9 digitos) para nao perder zeros a esquerda
    if "DOCUMENTO" in df_reduzido.columns:
        df_reduzido["DOCUMENTO"] = (
            df_reduzido["DOCUMENTO"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
            .str.zfill(9)
        )

    # Join com nomes de filiais (EMPRESA_ARQUIVO e FILIAL usados so para chave)
    df_reduzido["CHAVE_JOIN"] = df_reduzido["EMPRESA_ARQUIVO"] + " " + df_reduzido["FILIAL"]
    df_reduzido = pd.merge(
        df_reduzido,
        df_emp_ref,
        left_on="CHAVE_JOIN",
        right_on="Empresa_Cod_Filial",
        how="left",
    )
    df_reduzido = df_reduzido.drop(columns=["CHAVE_JOIN", "Empresa_Cod_Filial", "EMPRESA_ARQUIVO", "FILIAL"])

    # Empresa_Filial_Nome como primeira coluna, TIPOMOVIMENTO como segunda
    col_order = ["Empresa_Filial_Nome", "TIPOMOVIMENTO"] + [
        c for c in df_reduzido.columns if c not in ("Empresa_Filial_Nome", "TIPOMOVIMENTO")
    ]
    return df_reduzido[[c for c in col_order if c in df_reduzido.columns]]


# ---------------------------------------------------------------------------
# CONSULTA DE MOVIMENTAÇÕES NO BANCO
# ---------------------------------------------------------------------------

def buscar_movimentacoes_nuvem(engine, produto_cod):
    """
    CORREÇÃO #3: exceções explícitas — distingue erro de conexão de ausência
    de dados e propaga a mensagem correta para a camada de apresentação.
    """
    if engine is None:
        raise ConnectionError("Engine não inicializada. Verifique a conexão com o banco.")

    try:
        query = text(
            'SELECT * FROM movimentacoes WHERE "PRODUTO" = :p ORDER BY "DIGITACAO" DESC'
        )
        return pd.read_sql(query, engine, params={"p": produto_cod})
    except Exception as exc:
        # Re-lança com contexto para que app.py possa exibir mensagem útil
        raise RuntimeError(f"Erro ao consultar movimentações para '{produto_cod}': {exc}") from exc
