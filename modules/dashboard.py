"""
Module 4 - Dashboard Flask
Interface web de monitoring pour suivre l'avancement du crawling,
les statistiques et les résultats.
"""
import json
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request

from config.settings import DASHBOARD
from utils.database import DatabaseManager
from utils.logger import setup_logger

logger = setup_logger("dashboard", "dashboard.log")

app = Flask(__name__,
            template_folder="../templates",
            static_folder="../static")
app.secret_key = DASHBOARD["secret_key"]


def get_overview_stats() -> dict:
    queries = {
        "total_domains": "SELECT COUNT(*) as cnt FROM APP_domaine WHERE deleted=0",
        "http_checked": "SELECT COUNT(*) as cnt FROM APP_domaine WHERE deleted=0 AND http_at IS NOT NULL",
        "online": "SELECT COUNT(*) as cnt FROM APP_domaine WHERE deleted=0 AND http_statut=1",
        "offline": "SELECT COUNT(*) as cnt FROM APP_domaine WHERE deleted=0 AND http_statut=0 AND http_at IS NOT NULL",
        "parked": "SELECT COUNT(*) as cnt FROM APP_domaine WHERE deleted=0 AND flag_parking=1",
        "crawled": "SELECT COUNT(*) as cnt FROM APP_domaine WHERE deleted=0 AND flag_data_collected=1",
        "crawling_now": "SELECT COUNT(*) as cnt FROM APP_domaine WHERE deleted=0 AND flag_crawling=1",
        "with_email": "SELECT COUNT(DISTINCT id_domaine) as cnt FROM APP_email WHERE deleted=0",
        "with_phone": "SELECT COUNT(DISTINCT id_domaine) as cnt FROM APP_telephone WHERE deleted=0",
        "with_siret": "SELECT COUNT(DISTINCT id_domaine) as cnt FROM APP_siret WHERE deleted=0",
        "siretised": "SELECT COUNT(*) as cnt FROM APP_domaine_SIRETISATION WHERE ThG_score > 0",
        "total_emails": "SELECT COUNT(*) as cnt FROM APP_email WHERE deleted=0",
        "total_phones": "SELECT COUNT(*) as cnt FROM APP_telephone WHERE deleted=0",
        "total_keywords": "SELECT COUNT(*) as cnt FROM APP_mot_cle WHERE deleted=0",
    }

    stats = {}
    for key, query in queries.items():
        try:
            result = DatabaseManager.execute_query(query, fetch="one")
            stats[key] = result["cnt"] if result else 0
        except Exception as e:
            logger.error(f"Error getting stat {key}: {e}")
            stats[key] = 0

    return stats


def get_recent_activity(hours: int = 24) -> dict:
    cutoff = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    queries = {
        "http_checked_recent": f"SELECT COUNT(*) as cnt FROM APP_domaine WHERE http_at >= '{cutoff}'",
        "crawled_recent": f"SELECT COUNT(*) as cnt FROM APP_domaine WHERE crawled_at >= '{cutoff}'",
        "emails_recent": f"SELECT COUNT(*) as cnt FROM APP_email WHERE created_at >= '{cutoff}'",
        "phones_recent": f"SELECT COUNT(*) as cnt FROM APP_telephone WHERE created_at >= '{cutoff}'",
    }

    stats = {}
    for key, query in queries.items():
        try:
            result = DatabaseManager.execute_query(query, fetch="one")
            stats[key] = result["cnt"] if result else 0
        except Exception:
            stats[key] = 0
    return stats


def get_hourly_stats(days: int = 7) -> list:
    query = """
        SELECT DATE_FORMAT(crawled_at, '%Y-%m-%d %H:00') as hour,
               COUNT(*) as count
        FROM APP_domaine
        WHERE crawled_at >= DATE_SUB(NOW(), INTERVAL %s DAY)
          AND crawled_at IS NOT NULL
        GROUP BY hour
        ORDER BY hour ASC
    """
    try:
        return DatabaseManager.execute_query(query, (days,))
    except Exception:
        return []


def get_extension_stats() -> list:
    query = """
        SELECT extension, COUNT(*) as count
        FROM APP_domaine
        WHERE deleted=0 AND extension IS NOT NULL AND extension != ''
        GROUP BY extension
        ORDER BY count DESC
        LIMIT 20
    """
    try:
        return DatabaseManager.execute_query(query)
    except Exception:
        return []


def get_top_siretisation(limit: int = 20) -> list:
    query = """
        SELECT s.ID as domain_id, d.domaine, s.nom_societe,
               s.ThG_siret, s.ThG_score, s.code_postal, s.ville
        FROM APP_domaine_SIRETISATION s
        JOIN APP_domaine d ON s.ID = d.ID
        WHERE s.ThG_score > 50
        ORDER BY s.ThG_score DESC
        LIMIT %s
    """
    try:
        return DatabaseManager.execute_query(query, (limit,))
    except Exception:
        return []


def search_domain(query_str: str, limit: int = 50) -> list:
    query = """
        SELECT d.ID, d.domaine, d.http_statut, d.http_code, d.http_at,
               d.flag_parking, d.flag_data_collected, d.crawled_at,
               d.description
        FROM APP_domaine d
        WHERE d.deleted = 0 AND d.domaine LIKE %s
        ORDER BY d.domaine ASC
        LIMIT %s
    """
    return DatabaseManager.execute_query(query, (f"%{query_str}%", limit))


def get_domain_details(domain_id: int) -> dict:
    domain = DatabaseManager.execute_query(
        "SELECT * FROM APP_domaine WHERE ID = %s", (domain_id,), fetch="one"
    )
    if not domain:
        return None

    details = {"domain": domain}

    details["emails"] = DatabaseManager.execute_query(
        "SELECT email FROM APP_email WHERE id_domaine = %s AND deleted=0", (domain_id,)
    )
    details["phones"] = DatabaseManager.execute_query(
        "SELECT numero FROM APP_telephone WHERE id_domaine = %s AND deleted=0", (domain_id,)
    )
    details["addresses"] = DatabaseManager.execute_query(
        "SELECT numero, voie, code_postal, ville FROM APP_adresse WHERE id_domaine = %s AND deleted=0",
        (domain_id,)
    )
    details["social"] = DatabaseManager.execute_query(
        "SELECT url, type FROM APP_reseau_sociaux WHERE id_domaine = %s AND deleted=0", (domain_id,)
    )
    details["sirets"] = DatabaseManager.execute_query(
        "SELECT siret, siren FROM APP_siret WHERE id_domaine = %s AND deleted=0", (domain_id,)
    )
    details["sirens"] = DatabaseManager.execute_query(
        "SELECT siren FROM APP_siren WHERE id_domaine = %s AND deleted=0", (domain_id,)
    )
    details["siretisation"] = DatabaseManager.execute_query(
        "SELECT * FROM APP_domaine_SIRETISATION WHERE ID = %s", (domain_id,), fetch="one"
    )
    details["keywords"] = DatabaseManager.execute_query(
        "SELECT mot_cle, repetition FROM APP_mot_cle WHERE id_domaine = %s AND deleted=0 ORDER BY repetition DESC LIMIT 30",
        (domain_id,)
    )
    return details


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/overview")
def api_overview():
    stats = get_overview_stats()
    recent = get_recent_activity()
    return jsonify({"stats": stats, "recent": recent})


@app.route("/api/hourly")
def api_hourly():
    days = request.args.get("days", 7, type=int)
    data = get_hourly_stats(days)
    return jsonify(data)


@app.route("/api/extensions")
def api_extensions():
    data = get_extension_stats()
    return jsonify(data)


@app.route("/api/top-siretisation")
def api_top_siretisation():
    limit = request.args.get("limit", 20, type=int)
    data = get_top_siretisation(limit)
    return jsonify(data)


@app.route("/api/search")
def api_search():
    q = request.args.get("q", "")
    if len(q) < 2:
        return jsonify([])
    results = search_domain(q)
    return jsonify(results)


@app.route("/api/domain/<int:domain_id>")
def api_domain(domain_id):
    details = get_domain_details(domain_id)
    if not details:
        return jsonify({"error": "Domain not found"}), 404

    def serialize(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="ignore")
        return str(obj)

    return app.response_class(
        response=json.dumps(details, default=serialize, ensure_ascii=False),
        mimetype="application/json",
    )


@app.route("/api/logs")
def api_logs():
    logs = DatabaseManager.execute_query(
        "SELECT * FROM APP_crawling_logs ORDER BY id DESC LIMIT 50"
    )
    return jsonify(logs)


def main():
    logger.info(f"Dashboard starting on {DASHBOARD['host']}:{DASHBOARD['port']}")
    app.run(
        host=DASHBOARD["host"],
        port=DASHBOARD["port"],
        debug=DASHBOARD["debug"],
    )


if __name__ == "__main__":
    main()
