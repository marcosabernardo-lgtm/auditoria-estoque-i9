"""
inventario_db.py — Persistência do Inventário Cíclico no Supabase.
Engine é passada como parâmetro em todas as funções.
Usa engine.connect() + conn.commit() explícito para compatibilidade com Supabase.
"""
import json
import logging
from datetime import date
from sqlalchemy import text

logger = logging.getLogger(__name__)


# ── Contados ──────────────────────────────────────────────────────────────────

def db_obter_contados(engine, empresa, filial):
    if engine is None: return {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT produto, data_contagem FROM inventario_contados "
                "WHERE empresa=:e AND filial=:f"
            ), {"e": empresa, "f": filial}).fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception as ex:
        logger.warning("db_obter_contados: %s", ex)
        return {}


def db_marcar_contados(engine, empresa, filial, produtos, data=None, num_ciclo=None):
    if engine is None or not produtos: return
    data_reg = data or date.today().isoformat()
    try:
        with engine.connect() as conn:
            for p in produtos:
                conn.execute(text("""
                    INSERT INTO inventario_contados (empresa, filial, produto, data_contagem, num_ciclo)
                    VALUES (:e,:f,:p,:d,:c)
                    ON CONFLICT (empresa, filial, produto)
                    DO UPDATE SET data_contagem=EXCLUDED.data_contagem, num_ciclo=EXCLUDED.num_ciclo
                """), {"e":empresa,"f":filial,"p":str(p),"d":data_reg,"c":num_ciclo or ""})
            conn.commit()
    except Exception as ex:
        logger.warning("db_marcar_contados: %s", ex)


def db_resetar_contados(engine, empresa, filial):
    if engine is None: return
    try:
        with engine.connect() as conn:
            conn.execute(text(
                "DELETE FROM inventario_contados WHERE empresa=:e AND filial=:f"
            ), {"e":empresa,"f":filial})
            conn.commit()
    except Exception as ex:
        logger.warning("db_resetar_contados: %s", ex)


# ── Histórico de ciclos ───────────────────────────────────────────────────────

def db_obter_ciclos(engine, empresa, filial):
    if engine is None: return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT num_ciclo, data_geracao, data_contagem, responsavel,
                       num_inv, acuracidade, qtd_lista, qtd_contados,
                       cobertura_pct, status, uploads
                FROM inventario_ciclos
                WHERE empresa=:e AND filial=:f
                ORDER BY criado_em ASC
            """), {"e":empresa,"f":filial}).fetchall()
        return [{"num_ciclo":r[0],"data_geracao":r[1],"data":r[2],"responsavel":r[3],
                 "num_inv":r[4],"acuracidade":r[5],"qtd_lista":r[6],"qtd_contados":r[7],
                 "cobertura_pct":float(r[8] or 0),"status":r[9],"uploads":r[10]}
                for r in rows]
    except Exception as ex:
        logger.warning("db_obter_ciclos: %s", ex)
        return []


def db_gravar_ciclo(engine, empresa, filial, ciclo):
    if engine is None: return
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO inventario_ciclos
                    (empresa,filial,num_ciclo,data_geracao,data_contagem,responsavel,
                     num_inv,acuracidade,qtd_lista,qtd_contados,cobertura_pct,status,uploads)
                VALUES
                    (:empresa,:filial,:num_ciclo,:data_geracao,:data,:responsavel,
                     :num_inv,:acuracidade,:qtd_lista,:qtd_contados,:cobertura_pct,:status,:uploads)
            """), {
                "empresa":empresa,"filial":filial,
                "num_ciclo":ciclo.get("num_ciclo",""),
                "data_geracao":ciclo.get("data_geracao",""),
                "data":ciclo.get("data",""),
                "responsavel":ciclo.get("responsavel",""),
                "num_inv":ciclo.get("num_inv",""),
                "acuracidade":ciclo.get("acuracidade",""),
                "qtd_lista":ciclo.get("qtd_lista",0),
                "qtd_contados":len(ciclo.get("produtos_contados",[])),
                "cobertura_pct":ciclo.get("cobertura_pct",0),
                "status":ciclo.get("status","Concluído"),
                "uploads":ciclo.get("uploads",1),
            })
            conn.commit()
    except Exception as ex:
        logger.warning("db_gravar_ciclo: %s", ex)


def db_resetar_ciclos(engine, empresa, filial):
    if engine is None: return
    try:
        with engine.connect() as conn:
            conn.execute(text(
                "DELETE FROM inventario_ciclos WHERE empresa=:e AND filial=:f"
            ), {"e":empresa,"f":filial})
            conn.commit()
    except Exception as ex:
        logger.warning("db_resetar_ciclos: %s", ex)


# ── Ciclo ativo ───────────────────────────────────────────────────────────────

def db_obter_ciclo_ativo(engine, empresa, filial):
    if engine is None: return None
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT num_ciclo,data_geracao,qtd_lista,produtos_lista,uploads_json,status
                FROM inventario_ciclo_ativo
                WHERE empresa=:e AND filial=:f LIMIT 1
            """), {"e":empresa,"f":filial}).fetchone()
        if row is None: return None
        return {
            "num_ciclo":      row[0],
            "data_geracao":   row[1],
            "qtd_lista":      row[2],
            "produtos_lista": json.loads(row[3] or "[]"),
            "uploads":        json.loads(row[4] or "[]"),
            "status":         row[5],
            "label":          f"{empresa} — {filial}",
        }
    except Exception as ex:
        logger.warning("db_obter_ciclo_ativo: %s", ex)
        return None


def db_salvar_ciclo_ativo(engine, empresa, filial, ciclo):
    if engine is None: return
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO inventario_ciclo_ativo
                    (empresa,filial,num_ciclo,data_geracao,qtd_lista,
                     produtos_lista,uploads_json,status,atualizado_em)
                VALUES (:e,:f,:num_ciclo,:data_geracao,:qtd_lista,
                        :produtos_lista,:uploads_json,:status,NOW())
                ON CONFLICT (empresa,filial) DO UPDATE SET
                    num_ciclo=EXCLUDED.num_ciclo,
                    data_geracao=EXCLUDED.data_geracao,
                    qtd_lista=EXCLUDED.qtd_lista,
                    produtos_lista=EXCLUDED.produtos_lista,
                    uploads_json=EXCLUDED.uploads_json,
                    status=EXCLUDED.status,
                    atualizado_em=NOW()
            """), {
                "e":empresa,"f":filial,
                "num_ciclo":     ciclo.get("num_ciclo",""),
                "data_geracao":  ciclo.get("data_geracao",""),
                "qtd_lista":     ciclo.get("qtd_lista",0),
                "produtos_lista":json.dumps(ciclo.get("produtos_lista",[])),
                "uploads_json":  json.dumps(ciclo.get("uploads",[])),
                "status":        ciclo.get("status","Em andamento"),
            })
            conn.commit()
    except Exception as ex:
        logger.warning("db_salvar_ciclo_ativo: %s", ex)


def db_acumular_upload(engine, empresa, filial, upload_info):
    """Adiciona upload ao ciclo ativo via UPDATE direto no banco."""
    if engine is None: return
    ciclo = db_obter_ciclo_ativo(engine, empresa, filial)
    if ciclo is None:
        raise ValueError("Ciclo ativo não encontrado no banco")
    uploads = ciclo.get("uploads", [])
    uploads.append(upload_info)
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE inventario_ciclo_ativo
                SET uploads_json = :uploads_json, atualizado_em = NOW()
                WHERE empresa = :e AND filial = :f
            """), {
                "uploads_json": json.dumps(uploads),
                "e": empresa,
                "f": filial,
            })
            conn.commit()
    except Exception as ex:
        raise RuntimeError(f"db_acumular_upload falhou: {ex}")


def db_fechar_ciclo_ativo(engine, empresa, filial):
    if engine is None: return
    try:
        with engine.connect() as conn:
            conn.execute(text(
                "DELETE FROM inventario_ciclo_ativo WHERE empresa=:e AND filial=:f"
            ), {"e":empresa,"f":filial})
            conn.commit()
    except Exception as ex:
        logger.warning("db_fechar_ciclo_ativo: %s", ex)


def db_resetar_tudo(engine, empresa, filial):
    db_resetar_contados(engine, empresa, filial)
    db_resetar_ciclos(engine, empresa, filial)
    db_fechar_ciclo_ativo(engine, empresa, filial)
