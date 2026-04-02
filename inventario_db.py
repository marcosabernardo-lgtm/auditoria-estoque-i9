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
                       cobertura_pct, status, uploads,
                       relatorio_json, produtos_contados, erp_json
                FROM inventario_ciclos
                WHERE empresa=:e AND filial=:f
                ORDER BY criado_em ASC
            """), {"e":empresa,"f":filial}).fetchall()

        result = []
        for r in rows:
            # uploads pode estar gravado como JSON string (lista) ou int (legado)
            uploads_raw = r[10]
            if isinstance(uploads_raw, str):
                try:
                    uploads_val = json.loads(uploads_raw)
                except Exception:
                    uploads_val = []
            elif isinstance(uploads_raw, list):
                uploads_val = uploads_raw
            else:
                # int legado — não há como recuperar os dados dos uploads
                uploads_val = []

            result.append({
                "num_ciclo":        r[0],
                "data_geracao":     r[1],
                "data":             r[2],
                "responsavel":      r[3],
                "num_inv":          r[4],
                "acuracidade":      r[5],
                "qtd_lista":        r[6],
                "qtd_contados":     r[7],
                "cobertura_pct":    float(r[8] or 0),
                "status":           r[9],
                "uploads":          uploads_val,
                "relatorio_json":   r[11] or "[]",
                "produtos_contados":json.loads(r[12] or "[]"),
                "erp_json":         r[13] or "[]",
            })
        return result
    except Exception as ex:
        logger.error("db_obter_ciclos ERRO: %s", ex)
        return []


def db_gravar_ciclo(engine, empresa, filial, ciclo):
    if engine is None: return
    try:
        prods = ciclo.get("produtos_contados", [])

        # uploads pode ser lista (nova versão) ou int (legado) — sempre serializa como JSON string
        uploads_raw = ciclo.get("uploads", [])
        if isinstance(uploads_raw, list):
            uploads_json_str = json.dumps(uploads_raw, ensure_ascii=False)
            qtd_uploads = len(uploads_raw)
        else:
            # valor legado era int; preserva contagem mas não os dados
            uploads_json_str = "[]"
            qtd_uploads = int(uploads_raw) if uploads_raw else 0

        # qtd_uploads pode vir explicitamente do ciclo (campo separado)
        qtd_uploads = ciclo.get("qtd_uploads", qtd_uploads)

        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO inventario_ciclos
                    (empresa, filial, num_ciclo, data_geracao, data_contagem, responsavel,
                     num_inv, acuracidade, qtd_lista, qtd_contados, cobertura_pct, status,
                     uploads, relatorio_json, produtos_contados, erp_json)
                VALUES
                    (:empresa, :filial, :num_ciclo, :data_geracao, :data, :responsavel,
                     :num_inv, :acuracidade, :qtd_lista, :qtd_contados, :cobertura_pct, :status,
                     :uploads, :relatorio_json, :produtos_contados, :erp_json)
                ON CONFLICT (empresa, filial, num_ciclo) DO UPDATE SET
                    data_geracao      = EXCLUDED.data_geracao,
                    data_contagem     = EXCLUDED.data_contagem,
                    responsavel       = EXCLUDED.responsavel,
                    num_inv           = EXCLUDED.num_inv,
                    acuracidade       = EXCLUDED.acuracidade,
                    qtd_lista         = EXCLUDED.qtd_lista,
                    qtd_contados      = EXCLUDED.qtd_contados,
                    cobertura_pct     = EXCLUDED.cobertura_pct,
                    status            = EXCLUDED.status,
                    uploads           = EXCLUDED.uploads,
                    relatorio_json    = EXCLUDED.relatorio_json,
                    produtos_contados = EXCLUDED.produtos_contados,
                    erp_json          = EXCLUDED.erp_json
            """), {
                "empresa":           empresa,
                "filial":            filial,
                "num_ciclo":         ciclo.get("num_ciclo", ""),
                "data_geracao":      ciclo.get("data_geracao", ""),
                "data":              ciclo.get("data", ""),
                "responsavel":       ciclo.get("responsavel", ""),
                "num_inv":           ciclo.get("num_inv", ""),
                "acuracidade":       ciclo.get("acuracidade", ""),
                "qtd_lista":         ciclo.get("qtd_lista", 0),
                "qtd_contados":      len(prods),
                "cobertura_pct":     ciclo.get("cobertura_pct", 0),
                "status":            ciclo.get("status", "Concluído"),
                "uploads":           uploads_json_str,
                "relatorio_json":    ciclo.get("relatorio_json", "[]"),
                "produtos_contados": json.dumps(prods, ensure_ascii=False),
                "erp_json":          ciclo.get("erp_json", "[]"),
            })
            conn.commit()
    except Exception as ex:
        logger.error("db_gravar_ciclo ERRO: %s", ex)
        raise  # re-lança para que o chamador exiba o erro na UI


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
                    (empresa,filial,num_ciclo,data_geracao,label,qtd_lista,
                     produtos_lista,uploads_json,status,atualizado_em)
                VALUES (:e,:f,:num_ciclo,:data_geracao,:label,:qtd_lista,
                        :produtos_lista,:uploads_json,:status,NOW())
                ON CONFLICT (empresa,filial) DO UPDATE SET
                    num_ciclo=EXCLUDED.num_ciclo,
                    data_geracao=EXCLUDED.data_geracao,
                    label=EXCLUDED.label,
                    qtd_lista=EXCLUDED.qtd_lista,
                    produtos_lista=EXCLUDED.produtos_lista,
                    uploads_json=EXCLUDED.uploads_json,
                    status=EXCLUDED.status,
                    atualizado_em=NOW()
            """), {
                "e":empresa,"f":filial,
                "num_ciclo":     ciclo.get("num_ciclo",""),
                "data_geracao":  ciclo.get("data_geracao",""),
                "label":         ciclo.get("label",""),
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


# ── Justificativas ────────────────────────────────────────────────────────────

def db_obter_justificativas(engine, empresa, filial, num_ciclo):
    if engine is None: return {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT produto, justificativa FROM inventario_justificativas
                WHERE empresa=:e AND filial=:f AND num_ciclo=:c
            """), {"e":empresa,"f":filial,"c":num_ciclo}).fetchall()
        return {r[0]: r[1] for r in rows}
    except Exception as ex:
        logger.warning("db_obter_justificativas: %s", ex)
        return {}


def db_salvar_justificativas(engine, empresa, filial, num_ciclo, justificativas, documento=""):
    """justificativas = dict {produto: texto}. documento = nº do inventário ERP conferido."""
    if engine is None or not justificativas: return
    try:
        with engine.connect() as conn:
            for produto, texto in justificativas.items():
                conn.execute(text("""
                    INSERT INTO inventario_justificativas
                        (empresa, filial, num_ciclo, produto, justificativa, documento, atualizado_em)
                    VALUES (:e,:f,:c,:p,:j,:doc,NOW())
                    ON CONFLICT (empresa,filial,num_ciclo,produto)
                    DO UPDATE SET justificativa=EXCLUDED.justificativa,
                                  documento=EXCLUDED.documento, atualizado_em=NOW()
                """), {"e":empresa,"f":filial,"c":num_ciclo,"p":str(produto),"j":texto,"doc":documento})
            conn.commit()
    except Exception as ex:
        logger.warning("db_salvar_justificativas: %s", ex)


def db_obter_documentos_conferidos(engine, empresa, filial, num_ciclo):
    """Retorna set de documentos ERP já conferidos na etapa 4."""
    if engine is None: return set()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT documento FROM inventario_justificativas
                WHERE empresa=:e AND filial=:f AND num_ciclo=:c
                AND documento != '' AND documento IS NOT NULL
            """), {"e":empresa,"f":filial,"c":num_ciclo}).fetchall()
        return {r[0] for r in rows}
    except Exception as ex:
        logger.warning("db_obter_documentos_conferidos: %s", ex)
        return set()


# ── Upload ERP Protheus ───────────────────────────────────────────────────────

def db_obter_erp_uploads(engine, empresa, filial, num_ciclo):
    """Retorna lista de todos os uploads ERP do ciclo, ordenados por data."""
    if engine is None: return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT documento, data_upload, dados_json FROM inventario_erp_upload
                WHERE empresa=:e AND filial=:f AND num_ciclo=:c
                ORDER BY atualizado_em ASC
            """), {"e":empresa,"f":filial,"c":num_ciclo}).fetchall()
        return [{"documento": r[0], "data_upload": str(r[1]) if r[1] else "",
                 "dados": json.loads(r[2] or "[]")} for r in rows]
    except Exception as ex:
        logger.warning("db_obter_erp_uploads: %s", ex)
        return []


def db_obter_erp_upload(engine, empresa, filial, num_ciclo):
    """Compatibilidade: retorna primeiro upload ou None."""
    uploads = db_obter_erp_uploads(engine, empresa, filial, num_ciclo)
    return uploads[0] if uploads else None


def db_salvar_erp_upload(engine, empresa, filial, num_ciclo, documento, data_upload, dados):
    """Acumula upload ERP — cada documento é único por ciclo."""
    if engine is None: return
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO inventario_erp_upload
                    (empresa, filial, num_ciclo, documento, data_upload, dados_json, atualizado_em)
                VALUES (:e,:f,:c,:doc,:data,:dados,NOW())
                ON CONFLICT (empresa,filial,num_ciclo,documento)
                DO UPDATE SET data_upload=EXCLUDED.data_upload,
                              dados_json=EXCLUDED.dados_json, atualizado_em=NOW()
            """), {
                "e":empresa,"f":filial,"c":num_ciclo,
                "doc":documento,"data":data_upload,
                "dados":json.dumps(dados, ensure_ascii=False),
            })
            conn.commit()
    except Exception as ex:
        logger.warning("db_salvar_erp_upload: %s", ex)


# ── NF de Ajuste ─────────────────────────────────────────────────────────────

def db_obter_nf_ajustes(engine, empresa, filial, num_ciclo):
    """Retorna lista de NFs de ajuste do ciclo."""
    if engine is None: return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT num_nf, data_nf, natureza, dados_json
                FROM inventario_nf_ajuste
                WHERE empresa=:e AND filial=:f AND num_ciclo=:c
                ORDER BY atualizado_em ASC
            """), {"e":empresa,"f":filial,"c":num_ciclo}).fetchall()
        return [{"num_nf": r[0], "data_nf": str(r[1]) if r[1] else "",
                 "natureza": r[2], "dados": json.loads(r[3] or "[]")} for r in rows]
    except Exception as ex:
        logger.warning("db_obter_nf_ajustes: %s", ex)
        return []


def db_salvar_nf_ajuste(engine, empresa, filial, num_ciclo, num_nf, data_nf, natureza, dados):
    """Salva NF de ajuste — upsert por num_nf."""
    if engine is None: return
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO inventario_nf_ajuste
                    (empresa, filial, num_ciclo, num_nf, data_nf, natureza, dados_json, atualizado_em)
                VALUES (:e,:f,:c,:nf,:data,:nat,:dados,NOW())
                ON CONFLICT (empresa,filial,num_ciclo,num_nf)
                DO UPDATE SET data_nf=EXCLUDED.data_nf, natureza=EXCLUDED.natureza,
                              dados_json=EXCLUDED.dados_json, atualizado_em=NOW()
            """), {"e":empresa,"f":filial,"c":num_ciclo,"nf":num_nf,
                   "data":data_nf,"nat":natureza,
                   "dados":json.dumps(dados, ensure_ascii=False)})
            conn.commit()
    except Exception as ex:
        logger.warning("db_salvar_nf_ajuste: %s", ex)


# ── Carga consolidada (uma conexão só) ───────────────────────────────────────

def db_carregar_tudo(engine, empresa, filial):
    """Carrega todos os dados do ciclo ativo em uma única conexão."""
    if engine is None:
        return {"contados": {}, "ciclos": [], "ciclo_ativo": None,
                "erp_uploads": [], "nf_ajustes": [], "docs_conf": set(), "justs": {}}
    try:
        with engine.connect() as conn:
            # 1. Contados
            rows_cont = conn.execute(text("""
                SELECT produto, ultima_contagem FROM inventario_contados
                WHERE empresa=:e AND filial=:f
            """), {"e":empresa,"f":filial}).fetchall()
            contados = {r[0]: str(r[1]) for r in rows_cont}

            # 2. Ciclos fechados
            rows_ciclos = conn.execute(text("""
                SELECT num_ciclo, data_geracao, label, qtd_lista, produtos_lista,
                       uploads_json, status, relatorio_json, produtos_contados, erp_json
                FROM inventario_ciclos
                WHERE empresa=:e AND filial=:f
                ORDER BY num_ciclo ASC
            """), {"e":empresa,"f":filial}).fetchall()
            ciclos = []
            for r in rows_ciclos:
                try: pl = json.loads(r[4] or "[]")
                except: pl = []
                try: ups = json.loads(r[5] or "[]")
                except: ups = []
                try: pc = json.loads(r[8] or "[]")
                except: pc = []
                ciclos.append({
                    "num_ciclo": r[0], "data_geracao": r[1], "label": r[2],
                    "qtd_lista": r[3], "produtos_lista": pl, "uploads": ups,
                    "status": r[6], "relatorio_json": r[7] or "[]",
                    "produtos_contados": pc, "erp_json": r[9] or "[]",
                })

            # 3. Ciclo ativo
            row_ca = conn.execute(text("""
                SELECT num_ciclo, data_geracao, label, qtd_lista, produtos_lista, uploads_json, status
                FROM inventario_ciclo_ativo
                WHERE empresa=:e AND filial=:f LIMIT 1
            """), {"e":empresa,"f":filial}).fetchone()
            ciclo_ativo = None
            num_c = ""
            if row_ca:
                try: pl_a = json.loads(row_ca[4] or "[]")
                except: pl_a = []
                try: ups_a = json.loads(row_ca[5] or "[]")
                except: ups_a = []
                ciclo_ativo = {
                    "num_ciclo": row_ca[0], "data_geracao": row_ca[1],
                    "label": row_ca[2], "qtd_lista": row_ca[3],
                    "produtos_lista": pl_a, "uploads": ups_a, "status": row_ca[6],
                }
                num_c = row_ca[0]

            erp_uploads = []
            nf_ajustes  = []
            docs_conf   = set()
            justs       = {}

            if num_c:
                # 4. ERP uploads
                rows_erp = conn.execute(text("""
                    SELECT documento, data_upload, dados_json FROM inventario_erp_upload
                    WHERE empresa=:e AND filial=:f AND num_ciclo=:c ORDER BY atualizado_em ASC
                """), {"e":empresa,"f":filial,"c":num_c}).fetchall()
                erp_uploads = [{"documento": r[0], "data_upload": str(r[1]) if r[1] else "",
                                 "dados": json.loads(r[2] or "[]")} for r in rows_erp]

                # 5. NF ajustes
                rows_nf = conn.execute(text("""
                    SELECT num_nf, data_nf, natureza, dados_json FROM inventario_nf_ajuste
                    WHERE empresa=:e AND filial=:f AND num_ciclo=:c ORDER BY atualizado_em ASC
                """), {"e":empresa,"f":filial,"c":num_c}).fetchall()
                nf_ajustes = [{"num_nf": r[0], "data_nf": str(r[1]) if r[1] else "",
                                "natureza": r[2], "dados": json.loads(r[3] or "[]")} for r in rows_nf]

                # 6. Documentos conferidos + justificativas
                rows_j = conn.execute(text("""
                    SELECT produto, justificativa, documento FROM inventario_justificativas
                    WHERE empresa=:e AND filial=:f AND num_ciclo=:c
                """), {"e":empresa,"f":filial,"c":num_c}).fetchall()
                for rj in rows_j:
                    justs[rj[0]] = rj[1]
                    if rj[2]: docs_conf.add(rj[2])

        return {"contados": contados, "ciclos": ciclos, "ciclo_ativo": ciclo_ativo,
                "erp_uploads": erp_uploads, "nf_ajustes": nf_ajustes,
                "docs_conf": docs_conf, "justs": justs}
    except Exception as ex:
        logger.warning("db_carregar_tudo: %s", ex)
        return {"contados": {}, "ciclos": [], "ciclo_ativo": None,
                "erp_uploads": [], "nf_ajustes": [], "docs_conf": set(), "justs": {}}
