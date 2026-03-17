"""
Module 4 - Dashboard Flask
Interface web de monitoring pour suivre l'avancement du crawling,
les statistiques et les résultats.
"""
import json
import subprocess
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



def get_pm2_processes() -> list:
    """Get PM2 process list with status info."""
    try:
        result = subprocess.run(
            ['/home/netit972/.npm-global/bin/pm2', 'jlist'],
            capture_output=True, text=True, timeout=10,
            cwd='/home/netit972/crawling-app'
        )
        if result.returncode == 0 and result.stdout.strip():
            processes = json.loads(result.stdout)
            pm2_list = []
            for p in processes:
                env = p.get('pm2_env', {})
                monit = p.get('monit', {})
                pm2_list.append({
                    'name': p.get('name', 'unknown'),
                    'pid': p.get('pid', 0),
                    'status': env.get('status', 'unknown'),
                    'cpu': monit.get('cpu', 0),
                    'memory': round(monit.get('memory', 0) / 1024 / 1024, 1),
                    'uptime': env.get('pm_uptime', 0),
                    'restarts': env.get('restart_time', 0),
                    'created_at': env.get('created_at', 0),
                })
            return pm2_list
        return []
    except Exception as e:
        logger.error(f"Error getting PM2 processes: {e}")
        return []

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
        "siretised": "SELECT COUNT(*) as cnt FROM APP_domaine_SIRETISATION WHERE ThG_MR_score > 0",
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
        SELECT SUBSTRING_INDEX(domaine, '.', -1) as extension, COUNT(*) as count
        FROM APP_domaine
        WHERE deleted=0 AND domaine IS NOT NULL AND domaine != ''
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
               s.ThG_siret, s.ThG_MR_score, s.code_postal, s.ville
        FROM APP_domaine_SIRETISATION s
        JOIN APP_domaine d ON s.ID = d.ID
        WHERE s.ThG_MR_score > 50
        ORDER BY s.ThG_MR_score DESC
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


@app.route("/api/recent-siretisation")
def api_recent_siretisation():
    """Get the 100 most recent siretisations with timestamps."""
    try:
        limit = request.args.get("limit", 100, type=int)
        data = DatabaseManager.execute_query("""
            SELECT s.ID as domain_id, d.domaine, s.nom_societe, s.ThG_siret,
                   s.ThG_MR_score, s.code_postal, s.ville, s.adresse,
                   s.created_at, s.updated_at
            FROM APP_domaine_SIRETISATION s
            JOIN APP_domaine d ON s.ID = d.ID
            WHERE s.ThG_MR_score > 0
            ORDER BY s.updated_at DESC
            LIMIT %s
        """, (limit,))
        result = []
        for row in data:
            r = dict(row)
            if r.get('created_at'):
                r['created_at'] = r['created_at'].strftime('%Y-%m-%d %H:%M:%S')
            if r.get('updated_at'):
                r['updated_at'] = r['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
            result.append(r)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Recent siretisation error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/diag-siretisation")
def api_diag_siretisation():
    """Diagnostic: check siretisation status."""
    try:
        pending = DatabaseManager.execute_query("""
            SELECT COUNT(*) as cnt FROM APP_domaine d
            WHERE d.deleted = 0
              AND d.flag_data_collected = 1
              AND d.ID NOT IN (
                  SELECT DISTINCT ID FROM APP_domaine_SIRETISATION
                  WHERE ThG_MR_score IS NOT NULL AND ThG_MR_score > 0
              )
        """)
        recent = DatabaseManager.execute_query("""
            SELECT MAX(updated_at) as last_updated, MAX(created_at) as last_created,
                   COUNT(*) as total_siretised
            FROM APP_domaine_SIRETISATION WHERE ThG_MR_score > 0
        """)
        daily = DatabaseManager.execute_query("""
            SELECT DATE(updated_at) as dt, COUNT(*) as cnt
            FROM APP_domaine_SIRETISATION
            WHERE updated_at >= DATE_SUB(NOW(), INTERVAL 7 DAY)
              AND ThG_MR_score > 0
            GROUP BY DATE(updated_at)
            ORDER BY dt DESC
        """)
        daily_result = []
        for row in daily:
            r = dict(row)
            if r.get('dt'):
                r['dt'] = r['dt'].strftime('%Y-%m-%d')
            daily_result.append(r)
        return jsonify({
            "pending_domains": pending[0]['cnt'] if pending else 0,
            "last_updated": recent[0]['last_updated'].strftime('%Y-%m-%d %H:%M:%S') if recent and recent[0].get('last_updated') else None,
            "last_created": recent[0]['last_created'].strftime('%Y-%m-%d %H:%M:%S') if recent and recent[0].get('last_created') else None,
            "total_siretised": recent[0]['total_siretised'] if recent else 0,
            "daily_activity": daily_result
        })
    except Exception as e:
        logger.error(f"Diag siretisation error: {e}")
        return jsonify({"error": str(e)}), 500


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


@app.route("/api/pm2")
def api_pm2():
    processes = get_pm2_processes()
    return jsonify(processes)

@app.route("/api/pm2/<action>/<name>", methods=["POST"])
def api_pm2_action(action, name):
    if action not in ('restart', 'stop', 'start'):
        return jsonify({"error": "Invalid action"}), 400
    try:
        result = subprocess.run(
            ['/home/netit972/.npm-global/bin/pm2', action, name],
            capture_output=True, text=True, timeout=15,
            cwd='/home/netit972/crawling-app'
        )
        return jsonify({"success": result.returncode == 0, "output": result.stdout})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/logs")
def api_logs():
    logs = DatabaseManager.execute_query(
        "SELECT * FROM APP_crawling_logs ORDER BY id DESC LIMIT 50"
    )
    return jsonify(logs)


@app.route("/api/db-check")
def api_db_check():
    """Diagnostic endpoint to check DB schema."""
    try:
        tables = DatabaseManager.execute_query("SHOW TABLES")
        siret_cols = DatabaseManager.execute_query("DESCRIBE APP_domaine_SIRETISATION")
        domaine_cols = DatabaseManager.execute_query("DESCRIBE APP_domaine")
        return jsonify({
            "tables": tables,
            "siretisation_columns": siret_cols,
            "domaine_columns": domaine_cols
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/service-health")
def api_service_health():
    """Health check for all services with error logs."""
    import os
    health = {}
    log_dir = "/home/netit972/crawling-app/logs"
    services = ["http-checker", "crawler", "siretisation", "dashboard"]
    for svc in services:
        err_file = os.path.join(log_dir, f"pm2-{svc}.error.log")
        try:
            if os.path.exists(err_file):
                with open(err_file, "r") as f:
                    lines = f.readlines()
                    health[svc] = {
                        "error_lines": len(lines),
                        "last_errors": [l.strip() for l in lines[-10:]]
                    }
            else:
                health[svc] = {"error_lines": 0, "last_errors": []}
        except Exception as e:
            health[svc] = {"error": str(e)}
    return jsonify(health)



# ===== Reporting Routes =====

@app.route("/api/recent-activity")
def api_recent_activity():
    """Get daily crawling activity for the last 30 days."""
    try:
        data = DatabaseManager.execute_query("""
            SELECT DATE(crawled_at) as date,
                   COUNT(*) as crawled,
                   COUNT(DISTINCT CASE WHEN http_statut=1 THEN ID END) as online,
                   SUM(CASE WHEN flag_data_collected=1 THEN 1 ELSE 0 END) as collected
            FROM APP_domaine
            WHERE crawled_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
              AND crawled_at IS NOT NULL
            GROUP BY DATE(crawled_at)
            ORDER BY date DESC
            LIMIT 30
        """)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============================================================
# REPORTING
# ============================================================
from modules.reporting import get_full_report

@app.route('/reporting')
def reporting_page():
    """Render the reporting dashboard."""
    try:
        report = get_full_report()
        return render_template('reporting.html', report=report)
    except Exception as e:
        logger.error(f'Error in reporting_page: {e}')
        return f'Error generating report: {e}', 500

@app.route('/api/reporting')
def api_reporting():
    """API endpoint for reporting data."""
    try:
        report = get_full_report()
        return jsonify(report)
    except Exception as e:
        logger.error(f'Error in api_reporting: {e}')
        return jsonify({'error': str(e)}), 500


def main():
    logger.info(f"Dashboard starting on {DASHBOARD['host']}:{DASHBOARD['port']}")
    app.run(
        host=DASHBOARD["host"],
        port=DASHBOARD["port"],
        debug=DASHBOARD["debug"],
    )


if __name__ == "__main__":
    main()
