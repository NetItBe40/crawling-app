"""
Reporting module - Comprehensive statistics and monitoring dashboard
"""
import json
import subprocess
from datetime import datetime, timedelta
from utils.database import DatabaseManager
from utils.logger import setup_logger

logger = setup_logger("reporting")


def get_global_stats() -> dict:
    """Get global statistics from the database."""
    try:
        stats = {}

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_domaine WHERE deleted=0",
            fetch="one"
        )
        stats["total_domaines"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_domaine WHERE deleted=0 AND crawled_at IS NOT NULL",
            fetch="one"
        )
        stats["domaines_crawles"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_domaine WHERE deleted=0 AND crawled_at IS NULL",
            fetch="one"
        )
        stats["domaines_en_cours"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_domaine WHERE deleted=0 AND http_at IS NOT NULL",
            fetch="one"
        )
        stats["http_verifies"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_domaine WHERE deleted=0 AND http_statut=0 AND http_at IS NOT NULL",
            fetch="one"
        )
        stats["http_erreurs"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_email",
            fetch="one"
        )
        stats["total_emails"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_telephone",
            fetch="one"
        )
        stats["total_telephones"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_adresse",
            fetch="one"
        )
        stats["total_adresses"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_crawling_social_network",
            fetch="one"
        )
        stats["total_reseaux_sociaux"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_siret",
            fetch="one"
        )
        stats["total_siret"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_siren",
            fetch="one"
        )
        stats["total_siren"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_domaine WHERE deleted=0 AND flag_parking=1",
            fetch="one"
        )
        stats["domaines_parking"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_domaine WHERE deleted=0 AND flag_relocation=1",
            fetch="one"
        )
        stats["domaines_relocation"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_domaine WHERE deleted=0 AND flag_production=1",
            fetch="one"
        )
        stats["domaines_production"] = result["count"] if result else 0

        return stats
    except Exception as e:
        logger.error(f"Error in get_global_stats: {e}")
        return {}


def get_crawling_progress() -> dict:
    """Get crawling progress statistics."""
    try:
        stats = get_global_stats()
        total = stats.get("total_domaines", 0)
        crawled = stats.get("domaines_crawles", 0)
        remaining = stats.get("domaines_en_cours", 0)
        percentage = (crawled / total * 100) if total > 0 else 0

        return {
            "total": total,
            "crawled": crawled,
            "remaining": remaining,
            "percentage": round(percentage, 2)
        }
    except Exception as e:
        logger.error(f"Error in get_crawling_progress: {e}")
        return {}


def get_hourly_activity(days=7) -> list:
    """Get hourly crawling activity for the last N days."""
    try:
        since = datetime.now() - timedelta(days=days)
        query = """
            SELECT
                CONCAT(DATE(crawled_at), ' ', LPAD(HOUR(crawled_at), 2, '0'), ':00:00') as hour,
                COUNT(*) as count
            FROM APP_domaine
            WHERE crawled_at IS NOT NULL AND crawled_at >= %s
            GROUP BY CONCAT(DATE(crawled_at), ' ', LPAD(HOUR(crawled_at), 2, '0'), ':00:00')
            ORDER BY hour ASC
        """
        results = DatabaseManager.execute_query(query, params=(since.strftime('%Y-%m-%d %H:%M:%S'),), fetch="all")
        return results if results else []
    except Exception as e:
        logger.error(f"Error in get_hourly_activity: {e}")
        return []


def get_daily_activity(days=30) -> list:
    """Get daily crawling and data collection activity for the last N days."""
    try:
        since = datetime.now() - timedelta(days=days)
        query = """
            SELECT
                DATE(crawled_at) as day,
                COUNT(*) as crawled,
                SUM(CASE WHEN flag_data_collected=1 THEN 1 ELSE 0 END) as collected
            FROM APP_domaine
            WHERE crawled_at IS NOT NULL AND crawled_at >= %s
            GROUP BY DATE(crawled_at)
            ORDER BY day ASC
        """
        results = DatabaseManager.execute_query(query, params=(since.strftime('%Y-%m-%d %H:%M:%S'),), fetch="all")
        return results if results else []
    except Exception as e:
        logger.error(f"Error in get_daily_activity: {e}")
        return []


def get_http_stats() -> dict:
    """Get HTTP checking statistics."""
    try:
        stats = {}

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_domaine WHERE http_at IS NOT NULL",
            fetch="one"
        )
        stats["total_checked"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_domaine WHERE http_statut=1",
            fetch="one"
        )
        stats["ok"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_domaine WHERE http_statut=0 AND http_at IS NOT NULL",
            fetch="one"
        )
        stats["error"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_domaine WHERE http_at IS NULL",
            fetch="one"
        )
        stats["pending"] = result["count"] if result else 0

        return stats
    except Exception as e:
        logger.error(f"Error in get_http_stats: {e}")
        return {}


def get_http_daily(days=30) -> list:
    """Get daily HTTP checking statistics for the last N days."""
    try:
        since = datetime.now() - timedelta(days=days)
        query = """
            SELECT
                DATE(http_at) as day,
                SUM(CASE WHEN http_statut=1 THEN 1 ELSE 0 END) as ok,
                SUM(CASE WHEN http_statut=0 THEN 1 ELSE 0 END) as error
            FROM APP_domaine
            WHERE http_at IS NOT NULL AND http_at >= %s
            GROUP BY DATE(http_at)
            ORDER BY day ASC
        """
        results = DatabaseManager.execute_query(query, params=(since.strftime('%Y-%m-%d %H:%M:%S'),), fetch="all")
        return results if results else []
    except Exception as e:
        logger.error(f"Error in get_http_daily: {e}")
        return []


def get_siretisation_stats() -> dict:
    """Get SIRET/SIREN extraction statistics."""
    try:
        stats = {}

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_siretisation",
            fetch="one"
        )
        stats["total_siretisations"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_siret",
            fetch="one"
        )
        stats["total_siret"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_siren",
            fetch="one"
        )
        stats["total_siren"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_tva",
            fetch="one"
        )
        stats["total_tva"] = result["count"] if result else 0

        return stats
    except Exception as e:
        logger.error(f"Error in get_siretisation_stats: {e}")
        return {}


def get_data_extraction_stats() -> dict:
    """Get data extraction statistics."""
    try:
        stats = {}

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_email",
            fetch="one"
        )
        stats["total_emails"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_telephone",
            fetch="one"
        )
        stats["total_telephones"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_adresse",
            fetch="one"
        )
        stats["total_adresses"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_crawling_social_network",
            fetch="one"
        )
        stats["total_reseaux_sociaux"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_mot_cle",
            fetch="one"
        )
        stats["total_mots_cles"] = result["count"] if result else 0

        result = DatabaseManager.execute_query(
            "SELECT COUNT(*) as count FROM APP_dirigeant",
            fetch="one"
        )
        stats["total_dirigeants"] = result["count"] if result else 0

        return stats
    except Exception as e:
        logger.error(f"Error in get_data_extraction_stats: {e}")
        return {}


def get_pm2_status() -> list:
    """Get PM2 process status."""
    try:
        result = subprocess.run(
            ["pm2", "jlist"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode == 0:
            processes = json.loads(result.stdout)
            pm2_status = []
            for proc in processes:
                mem_bytes = proc.get("monit", {}).get("memory", 0)
                pm2_status.append({
                    "name": proc.get("name"),
                    "status": proc.get("pm2_env", {}).get("status"),
                    "cpu": proc.get("monit", {}).get("cpu"),
                    "memory_mb": round(mem_bytes / (1024 * 1024), 1),
                    "restarts": proc.get("pm2_env", {}).get("restart_time", 0),
                    "uptime": proc.get("pm2_env", {}).get("pm_uptime")
                })
            return pm2_status
        else:
            logger.error(f"PM2 error: {result.stderr}")
            return []
    except Exception as e:
        logger.error(f"Error in get_pm2_status: {e}")
        return []


def get_recent_errors(limit=20) -> list:
    """Get recent domains with HTTP errors."""
    try:
        query = """
            SELECT
                domaine,
                http_statut,
                http_code,
                http_at
            FROM APP_domaine
            WHERE http_statut=0 AND http_at IS NOT NULL
            ORDER BY http_at DESC
            LIMIT %s
        """
        results = DatabaseManager.execute_query(query, params=(limit,), fetch="all")
        return results if results else []
    except Exception as e:
        logger.error(f"Error in get_recent_errors: {e}")
        return []


def get_extension_distribution() -> list:
    """Get top 15 domain extensions by count."""
    try:
        query = """
            SELECT
                SUBSTRING_INDEX(domaine, '.', -1) as extension,
                COUNT(*) as count
            FROM APP_domaine
            WHERE deleted=0 AND domaine IS NOT NULL AND domaine != ''
            GROUP BY SUBSTRING_INDEX(domaine, '.', -1)
            ORDER BY count DESC
            LIMIT 15
        """
        results = DatabaseManager.execute_query(query, fetch="all")
        return results if results else []
    except Exception as e:
        logger.error(f"Error in get_extension_distribution: {e}")
        return []


def get_full_report() -> dict:
    """Generate a full report with all statistics."""
    try:
        report = {
            "generated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "global_stats": get_global_stats(),
            "crawling_progress": get_crawling_progress(),
            "hourly_activity": get_hourly_activity(),
            "daily_activity": get_daily_activity(),
            "http_stats": get_http_stats(),
            "http_daily": get_http_daily(),
            "siretisation_stats": get_siretisation_stats(),
            "data_extraction_stats": get_data_extraction_stats(),
            "pm2_status": get_pm2_status(),
            "recent_errors": get_recent_errors(),
            "extension_distribution": get_extension_distribution()
        }
        return report
    except Exception as e:
        logger.error(f"Error in get_full_report: {e}")
        return {}
