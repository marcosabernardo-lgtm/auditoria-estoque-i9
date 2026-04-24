import json
import logging
from datetime import date
from sqlalchemy import text

logger = logging.getLogger(__name__)

def get_now_fn(engine):
    """Compatibilidade de data/hora entre SQLite e PostgreSQL."""
    return "CURRENT_TIMESTAMP" if "sqlite" in str(engine.url) else "NOW()"

def garantir_tabelas(engine):
    """Cria tabelas necessárias se não existirem (Essencial para o banco de teste)."""
    if "sqlite" not in str(engine.url): return
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS inventario_ciclo_ativo (
                empresa TEXT, filial TEXT, num_ciclo TEXT, data_geracao TEXT,
                label TEXT, qtd_lista INTEGER, produtos_lista TEXT,
                uploads_json TEXT, status TEXT, atualizado_em TIMESTAMP,
                PRIMARY KEY (empresa, filial)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS inventario_erp_upload (
                empresa TEXT, filial TEXT, num_ciclo TEXT, documento TEXT,
                data_upload TEXT, dados_json TEXT, atualizado_em TIMESTAMP,
                PRIMARY KEY (empresa, filial, num_ciclo, documento)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS inventario_contados (
                empresa TEXT, filial TEXT, produto TEXT, data_contagem TEXT, num_ciclo TEXT,
                PRIMARY KEY (empresa, filial, produto)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS inventario_justificativas (
                empresa TEXT, filial TEXT, num_ciclo TEXT, produto TEXT,
                justificativa TEXT, documento TEXT, atualizado_em TIMESTAMP,
                PRIMARY KEY (empresa, filial, num_ciclo, produto)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS inventario_nf_ajuste (
                empresa TEXT, filial TEXT, num_ciclo TEXT, num_nf TEXT,
                data_nf TEXT, natureza TEXT, dados_json TEXT, atualizado_em TIMESTAMP,
                PRIMARY KEY (empresa, filial, num_ciclo, num_nf)
            )
        """))
        # CORREÇÃO: tabela de histórico de ciclos fechados
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS inventario_ciclos_historico (
                empresa TEXT, filial TEXT, num_ciclo TEXT, data_geracao TEXT,
                data_fechamento TEXT, responsavel TEXT, qtd_lista INTEGER, produtos_lista TEXT,
                uploads_json TEXT,
                PRIMARY KEY (empresa, filial, num_ciclo)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS inventario_ajustes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa TEXT, filial TEXT, num_nf TEXT, data_nf TEXT,
                natureza TEXT, justificativa TEXT, dados_json TEXT,
                operador TEXT, origem TEXT, num_ciclo TEXT,
                criado_em TIMESTAMP
            )
        """))
        _garantir_coluna(engine, "inventario_ciclo_ativo", "responsavel TEXT")
        _garantir_coluna(engine, "inventario_ciclos_historico", "responsavel TEXT")
        conn.commit()

def _garantir_coluna(engine, table, column_def):
    """Adiciona uma coluna no SQLite se ela não existir."""
    if "sqlite" not in str(engine.url):
        return
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
            cols = [r[1] for r in rows]
            col_name = column_def.split()[0]
            if col_name not in cols:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column_def}"))
                conn.commit()
    except Exception as e:
        logger.error(f"Erro ao garantir coluna {column_def} em {table}: {e}")


def _garantir_tabela_historico_postgres(engine):
    """Garante que a tabela de histórico existe no PostgreSQL."""
    if "sqlite" in str(engine.url): return
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS inventario_ciclos_historico (
                    empresa TEXT, filial TEXT, num_ciclo TEXT, data_geracao TEXT,
                    data_fechamento TEXT, responsavel TEXT, qtd_lista INTEGER, produtos_lista TEXT,
                    uploads_json TEXT,
                    PRIMARY KEY (empresa, filial, num_ciclo)
                )
            """))
            conn.commit()
    except Exception as e:
        logger.error(f"Erro ao garantir tabela histórico: {e}")

def garantir_tabela_ajustes(engine):
    """Cria inventario_ajustes se não existir (SQLite e PostgreSQL)."""
    if "sqlite" in str(engine.url):
        garantir_tabelas(engine)
        return
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS inventario_ajustes (
                    id SERIAL PRIMARY KEY,
                    empresa TEXT, filial TEXT, num_nf TEXT, data_nf DATE,
                    natureza TEXT, justificativa TEXT, dados_json TEXT,
                    operador TEXT, origem TEXT, num_ciclo TEXT,
                    criado_em TIMESTAMP
                )
            """))
            conn.commit()
    except Exception as e:
        logger.error(f"Erro ao garantir tabela ajustes: {e}")

# ── GERAÇÃO DE NUM_CICLO ÚNICO ────────────────────────────────────────────────

def db_gerar_num_ciclo(engine, empresa, filial):
    """Gera num_ciclo único para o dia. Se já existe no histórico ou no ativo, adiciona sufixo -2, -3..."""
    _garantir_tabela_historico_postgres(engine)
    garantir_tabelas(engine)
    base = f"{date.today().strftime('%Y%m%d')}-{empresa}-{filial}".replace(" ", "")
    try:
        with engine.connect() as conn:
            rows_hist = conn.execute(text(
                "SELECT num_ciclo FROM inventario_ciclos_historico WHERE empresa=:e AND filial=:f AND num_ciclo LIKE :pat"
            ), {"e": empresa, "f": filial, "pat": f"{base}%"}).fetchall()
            existentes = {r[0] for r in rows_hist}

            row_ativo = conn.execute(text(
                "SELECT num_ciclo FROM inventario_ciclo_ativo WHERE empresa=:e AND filial=:f"
            ), {"e": empresa, "f": filial}).fetchone()
            if row_ativo:
                existentes.add(row_ativo[0])

        if base not in existentes:
            return base
        counter = 2
        while f"{base}-{counter}" in existentes:
            counter += 1
        return f"{base}-{counter}"
    except Exception as e:
        logger.error(f"Erro ao gerar num_ciclo: {e}")
        return base

# ── CONTADOS ──────────────────────────────────────────────────────────────────

def db_obter_contados(engine, empresa, filial):
    garantir_tabelas(engine)
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT produto, data_contagem FROM inventario_contados WHERE empresa=:e AND filial=:f"), {"e": empresa, "f": filial}).fetchall()
        return {str(r[0]): r[1] for r in rows}
    except: return {}

def db_marcar_contados(engine, empresa, filial, produtos, data=None, num_ciclo=None):
    data_reg = data or date.today().isoformat()
    try:
        with engine.connect() as conn:
            for p in produtos:
                conn.execute(text("""
                    INSERT INTO inventario_contados (empresa, filial, produto, data_contagem, num_ciclo)
                    VALUES (:e,:f,:p,:d,:c)
                    ON CONFLICT (empresa, filial, produto) DO UPDATE SET data_contagem=EXCLUDED.data_contagem, num_ciclo=EXCLUDED.num_ciclo
                """), {"e":empresa,"f":filial,"p":str(p).zfill(6),"d":data_reg,"c":num_ciclo or ""})
            conn.commit()
    except: pass

# ── CICLO ATIVO ───────────────────────────────────────────────────────────────

def db_obter_ciclo_ativo(engine, empresa, filial):
    garantir_tabelas(engine)
    try:
        with engine.connect() as conn:
            row = conn.execute(text("SELECT num_ciclo, data_geracao, responsavel, qtd_lista, produtos_lista, uploads_json, status FROM inventario_ciclo_ativo WHERE empresa=:e AND filial=:f"), {"e":empresa,"f":filial}).fetchone()
        if not row: return None
        return {
            "num_ciclo": row[0],
            "data_geracao": row[1],
            "responsavel": row[2],
            "qtd_lista": row[3],
            "produtos_lista": json.loads(row[4] or "[]"),
            "uploads": json.loads(row[5] or "[]"),
            "status": row[6]
        }
    except: return None

def db_salvar_ciclo_ativo(engine, empresa, filial, ciclo):
    _garantir_tabela_historico_postgres(engine)
    garantir_tabelas(engine)
    now_fn = get_now_fn(engine)
    try:
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM inventario_ciclo_ativo WHERE empresa=:e AND filial=:f"), {"e":empresa, "f":filial})
            num_ciclo = ciclo.get("num_ciclo", "")
            if num_ciclo:
                conn.execute(text("DELETE FROM inventario_erp_upload WHERE empresa=:e AND filial=:f AND num_ciclo=:c"), {"e":empresa, "f":filial, "c":num_ciclo})
                conn.execute(text("DELETE FROM inventario_justificativas WHERE empresa=:e AND filial=:f AND num_ciclo=:c"), {"e":empresa, "f":filial, "c":num_ciclo})
                conn.execute(text("DELETE FROM inventario_nf_ajuste WHERE empresa=:e AND filial=:f AND num_ciclo=:c"), {"e":empresa, "f":filial, "c":num_ciclo})
                # Só apaga contados se este num_ciclo NÃO está no histórico fechado.
                # Evita apagar contados de um ciclo já concluído quando um novo é
                # iniciado no mesmo dia (num_ciclo gerado com a mesma data).
                ja_no_historico = conn.execute(text(
                    "SELECT 1 FROM inventario_ciclos_historico WHERE empresa=:e AND filial=:f AND num_ciclo=:c"
                ), {"e": empresa, "f": filial, "c": num_ciclo}).fetchone()
                if not ja_no_historico:
                    conn.execute(text("DELETE FROM inventario_contados WHERE empresa=:e AND filial=:f AND num_ciclo=:c"), {"e":empresa, "f":filial, "c":num_ciclo})
            conn.execute(text(f"INSERT INTO inventario_ciclo_ativo (empresa, filial, num_ciclo, data_geracao, responsavel, label, qtd_lista, produtos_lista, uploads_json, status, atualizado_em) VALUES (:e, :f, :num_ciclo, :data_geracao, :responsavel, :label, :qtd_lista, :produtos_lista, :uploads_json, :status, {now_fn})"),
                {"e":empresa, "f":filial, "num_ciclo": num_ciclo, "data_geracao": ciclo.get("data_geracao",""), "responsavel": ciclo.get("responsavel", ""), "label": f"{empresa}-{filial}", "qtd_lista": ciclo.get("qtd_lista", 0), "produtos_lista": json.dumps(ciclo.get("produtos_lista",[])), "uploads_json": json.dumps(ciclo.get("uploads",[])), "status": ciclo.get("status","Em andamento")})
            conn.commit()
    except Exception as e: logger.error(e)

def db_fechar_ciclo_ativo(engine, empresa, filial):
    """
    Antes de deletar o ciclo ativo, salva no histórico
    junto com todos os uploads do ciclo.
    Usa DELETE + INSERT para evitar falha de ON CONFLICT
    em bancos que não possuem o PRIMARY KEY na tabela histórico.
    """
    _garantir_tabela_historico_postgres(engine)
    garantir_tabelas(engine)
    try:
        with engine.connect() as conn:
            # Busca o ciclo ativo antes de deletar
            row = conn.execute(text(
                "SELECT num_ciclo, data_geracao, responsavel, qtd_lista, produtos_lista FROM inventario_ciclo_ativo WHERE empresa=:e AND filial=:f"
            ), {"e": empresa, "f": filial}).fetchone()

            if row:
                num_ciclo = row[0]
                data_geracao = row[1]
                responsavel = row[2]
                qtd_lista = row[3]
                produtos_lista = row[4]

                # Busca uploads do ciclo
                rows_erp = conn.execute(text(
                    "SELECT documento, data_upload, dados_json FROM inventario_erp_upload WHERE empresa=:e AND filial=:f AND num_ciclo=:c"
                ), {"e": empresa, "f": filial, "c": num_ciclo}).fetchall()
                uploads = [{"documento": r[0], "data_upload": str(r[1]), "dados": json.loads(r[2] or "[]")} for r in rows_erp]

                # Salva no histórico: DELETE + INSERT para compatibilidade com
                # bancos que não possuem PRIMARY KEY na tabela (versões antigas)
                conn.execute(text(
                    "DELETE FROM inventario_ciclos_historico WHERE empresa=:e AND filial=:f AND num_ciclo=:c"
                ), {"e": empresa, "f": filial, "c": num_ciclo})
                conn.execute(text("""
                    INSERT INTO inventario_ciclos_historico
                        (empresa, filial, num_ciclo, data_geracao, data_fechamento, responsavel, qtd_lista, produtos_lista, uploads_json)
                    VALUES (:e, :f, :c, :dg, :df, :resp, :ql, :pl, :uj)
                """), {
                    "e": empresa, "f": filial,
                    "c": num_ciclo,
                    "dg": data_geracao,
                    "df": date.today().isoformat(),
                    "resp": responsavel,
                    "ql": qtd_lista,
                    "pl": produtos_lista,
                    "uj": json.dumps(uploads)
                })

                # Itens da lista que não foram contados: remover de inventario_contados
                # para que apareçam com score máximo no próximo ciclo
                lista_prods = json.loads(produtos_lista or "[]")
                uploaded_set = set()
                for u in uploads:
                    for item in u.get("dados", []):
                        cod = str(item.get("Codigo", "")).strip().zfill(6)
                        if cod:
                            uploaded_set.add(cod)
                nao_contados = [str(p).zfill(6) for p in lista_prods if str(p).zfill(6) not in uploaded_set]
                for p in nao_contados:
                    conn.execute(text(
                        "DELETE FROM inventario_contados WHERE empresa=:e AND filial=:f AND produto=:p"
                    ), {"e": empresa, "f": filial, "p": p})

            # Deleta o ciclo ativo
            conn.execute(text("DELETE FROM inventario_ciclo_ativo WHERE empresa=:e AND filial=:f"), {"e": empresa, "f": filial})
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Erro ao fechar ciclo: {e}")
        return False

# ── HISTÓRICO DE CICLOS ───────────────────────────────────────────────────────

def db_obter_ciclos_historico(engine, empresa, filial):
    """
    CORREÇÃO: busca todos os ciclos fechados do histórico,
    incluindo uploads, justificativas e NFs de cada um.
    """
    _garantir_tabela_historico_postgres(engine)
    garantir_tabelas(engine)
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT num_ciclo, data_geracao, data_fechamento, responsavel, qtd_lista, produtos_lista, uploads_json
                FROM inventario_ciclos_historico
                WHERE empresa=:e AND filial=:f
                ORDER BY data_fechamento DESC
            """), {"e": empresa, "f": filial}).fetchall()

        ciclos = []
        for r in rows:
            num_ciclo = r[0]
            uploads = json.loads(r[6] or "[]")
            responsavel = r[3]

            # Busca justificativas e NFs para cada ciclo histórico
            with engine.connect() as conn:
                rows_j = conn.execute(text(
                    "SELECT produto, justificativa FROM inventario_justificativas WHERE empresa=:e AND filial=:f AND num_ciclo=:c"
                ), {"e": empresa, "f": filial, "c": num_ciclo}).fetchall()
                justs = {rj[0]: rj[1] for rj in rows_j}

                rows_nf = conn.execute(text(
                    "SELECT num_nf, data_nf, natureza, dados_json FROM inventario_nf_ajuste WHERE empresa=:e AND filial=:f AND num_ciclo=:c"
                ), {"e": empresa, "f": filial, "c": num_ciclo}).fetchall()
                nfs = [{"num_nf": rn[0], "data_nf": rn[1], "natureza": rn[2], "dados": json.loads(rn[3] or "[]")} for rn in rows_nf]

            ciclos.append({
                "num_ciclo":       r[0],
                "data_geracao":    r[1],
                "data_fechamento": r[2],
                "responsavel":     responsavel,
                "qtd_lista":       r[4],
                "produtos_lista":  json.loads(r[5] or "[]"),
                "uploads":         uploads,
                "justs":           justs,
                "nfs":             nfs,
            })
        return ciclos
    except Exception as e:
        logger.error(f"Erro ao obter histórico: {e}")
        return []

# ── CARGA COMPLETA ────────────────────────────────────────────────────────────

def db_carregar_tudo(engine, empresa, filial):
    garantir_tabelas(engine)
    ca = db_obter_ciclo_ativo(engine, empresa, filial)
    contados = db_obter_contados(engine, empresa, filial)
    erp_uploads, justs, nf_ajustes = [], {}, []
    if ca:
        with engine.connect() as conn:
            rows_erp = conn.execute(text("SELECT documento, data_upload, dados_json FROM inventario_erp_upload WHERE empresa=:e AND filial=:f AND num_ciclo=:c"), {"e":empresa,"f":filial,"c":ca['num_ciclo']}).fetchall()
            erp_uploads = [{"documento": r[0], "data_upload": str(r[1]), "dados": json.loads(r[2] or "[]")} for r in rows_erp]
            rows_j = conn.execute(text("SELECT produto, justificativa FROM inventario_justificativas WHERE empresa=:e AND filial=:f AND num_ciclo=:c"), {"e":empresa,"f":filial,"c":ca['num_ciclo']}).fetchall()
            justs = {r[0]: r[1] for r in rows_j}
            rows_nf = conn.execute(text("SELECT num_nf, data_nf FROM inventario_nf_ajuste WHERE empresa=:e AND filial=:f AND num_ciclo=:c"), {"e":empresa,"f":filial,"c":ca['num_ciclo']}).fetchall()
            nf_ajustes = [{"num_nf": r[0], "data_nf": r[1]} for r in rows_nf]

    # CORREÇÃO: busca ciclos históricos reais em vez de retornar lista vazia
    ciclos_historico = db_obter_ciclos_historico(engine, empresa, filial)

    return {
        "contados":    contados,
        "ciclo_ativo": ca,
        "erp_uploads": erp_uploads,
        "justs":       justs,
        "nf_ajustes":  nf_ajustes,
        "ciclos":      ciclos_historico,   # ← antes era sempre []
    }

# ── OPERAÇÕES ─────────────────────────────────────────────────────────────────

def db_salvar_erp_upload(engine, empresa, filial, num_ciclo, documento, data_upload, dados):
    garantir_tabelas(engine)
    now_fn = get_now_fn(engine)
    try:
        with engine.connect() as conn:
            conn.execute(text(f"INSERT INTO inventario_erp_upload (empresa, filial, num_ciclo, documento, data_upload, dados_json, atualizado_em) VALUES (:e,:f,:c,:doc,:data,:dados,{now_fn}) ON CONFLICT (empresa, filial, num_ciclo, documento) DO UPDATE SET data_upload=EXCLUDED.data_upload, dados_json=EXCLUDED.dados_json, atualizado_em={now_fn}"), {"e":empresa, "f":filial, "c":num_ciclo, "doc":documento, "data":data_upload, "dados":json.dumps(dados)})
            conn.commit()
    except: pass

def db_remover_erp_uploads(engine, empresa, filial, num_ciclo):
    try:
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM inventario_erp_upload WHERE empresa=:e AND filial=:f AND num_ciclo=:c"), {"e":empresa,"f":filial,"c":num_ciclo})
            conn.commit()
    except: pass

def db_cancelar_ciclo_ativo(engine, empresa, filial):
    try:
        with engine.connect() as conn:
            row = conn.execute(text("SELECT num_ciclo FROM inventario_ciclo_ativo WHERE empresa=:e AND filial=:f"), {"e":empresa, "f":filial}).fetchone()
            if row:
                num_ciclo = row[0]
                conn.execute(text("DELETE FROM inventario_erp_upload WHERE empresa=:e AND filial=:f AND num_ciclo=:c"), {"e":empresa, "f":filial, "c":num_ciclo})
                conn.execute(text("DELETE FROM inventario_justificativas WHERE empresa=:e AND filial=:f AND num_ciclo=:c"), {"e":empresa, "f":filial, "c":num_ciclo})
                conn.execute(text("DELETE FROM inventario_nf_ajuste WHERE empresa=:e AND filial=:f AND num_ciclo=:c"), {"e":empresa, "f":filial, "c":num_ciclo})
                conn.execute(text("DELETE FROM inventario_ciclo_ativo WHERE empresa=:e AND filial=:f"), {"e":empresa, "f":filial})
                conn.commit()
    except Exception as e:
        logger.error(f"Erro ao cancelar ciclo ativo: {e}")

def db_salvar_justificativas(engine, empresa, filial, num_ciclo, justificativas):
    now_fn = get_now_fn(engine)
    try:
        with engine.connect() as conn:
            for prod, just in justificativas.items():
                conn.execute(text(f"INSERT INTO inventario_justificativas (empresa, filial, num_ciclo, produto, justificativa, atualizado_em) VALUES (:e,:f,:c,:p,:j,{now_fn}) ON CONFLICT (empresa, filial, num_ciclo, produto) DO UPDATE SET justificativa=EXCLUDED.justificativa"), {"e":empresa, "f":filial, "c":num_ciclo, "p":prod, "j":just})
            conn.commit()
    except: pass

def db_salvar_nf_ajuste(engine, empresa, filial, num_ciclo, num_nf, data_nf, natureza, dados):
    now_fn = get_now_fn(engine)
    try:
        with engine.connect() as conn:
            conn.execute(text(f"INSERT INTO inventario_nf_ajuste (empresa, filial, num_ciclo, num_nf, data_nf, natureza, dados_json, atualizado_em) VALUES (:e,:f,:c,:nf,:d,:n,:dados,{now_fn}) ON CONFLICT (empresa, filial, num_ciclo, num_nf) DO UPDATE SET dados_json=EXCLUDED.dados_json"), {"e":empresa,"f":filial,"c":num_ciclo,"nf":num_nf,"d":data_nf,"n":natureza,"dados":json.dumps(dados)})
            conn.commit()
    except: pass

# Placeholders para evitar erros de importação
def db_gravar_ciclo(*args, **kwargs): pass

def db_obter_nf_ajustes(engine, empresa=None, filial=None, num_ciclo=None, *args, **kwargs):
    if not engine or not empresa or not filial or not num_ciclo:
        return {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT num_nf, data_nf, natureza, dados_json FROM inventario_nf_ajuste WHERE empresa=:e AND filial=:f AND num_ciclo=:c"
            ), {"e": empresa, "f": filial, "c": num_ciclo}).fetchall()
        return {r[0]: {"data_nf": r[1], "natureza": r[2], "dados": json.loads(r[3] or "[]")} for r in rows}
    except:
        return {}

def db_obter_justificativas(engine, empresa=None, filial=None, num_ciclo=None, *args, **kwargs):
    if not engine or not empresa or not filial or not num_ciclo:
        return {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT produto, justificativa FROM inventario_justificativas WHERE empresa=:e AND filial=:f AND num_ciclo=:c"
            ), {"e": empresa, "f": filial, "c": num_ciclo}).fetchall()
        return {r[0]: r[1] for r in rows}
    except:
        return {}