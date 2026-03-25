"""
Module de nettoyage des emails parasites.
API endpoints pour le dashboard de reporting.
"""
import subprocess
import os
from flask import jsonify, request
from utils.logger import setup_logger

logger = setup_logger("email_cleanup", "dashboard.log")

SCRIPT_PATH = os.path.join(os.path.dirname(__file__), '..', 'scripts', 'nettoyage_emails.sh')

def get_email_cleanup_stats():
    """Get current email stats from the cleanup script --stats mode."""
    try:
        result = subprocess.run(
            ['bash', SCRIPT_PATH, '--stats'],
            capture_output=True, text=True, timeout=30,
            cwd=os.path.dirname(SCRIPT_PATH)
        )
        output = result.stdout.strip()
        # Parse stats output
        stats = {'raw_output': output}
        for line in output.split('\n'):
            if 'ACTIFS=' in line:
                parts = line.split('|')
                for p in parts:
                    p = p.strip()
                    if 'ACTIFS=' in p:
                        stats['actifs'] = int(p.split('=')[1].strip())
                    if 'SUPPRIMES=' in p:
                        stats['supprimes'] = int(p.split('=')[1].strip())
            if 'Total:' in line and 'dry-run' not in line:
                try:
                    stats['distinct_emails'] = int(line.split(':')[1].strip())
                except:
                    pass
        return stats
    except Exception as e:
        logger.error(f"Error getting email cleanup stats: {e}")
        return {"error": str(e)}

def run_email_cleanup(mode):
    """Run the email cleanup script with specified mode."""
    allowed_modes = {
        'dry-run': '--dry-run',
        'marquage': '--marquage',
        'lowercase': '--lowercase',
        'full': '--full'
    }
    if mode not in allowed_modes:
        return {"error": f"Mode invalide: {mode}. Modes: {list(allowed_modes.keys())}"}
    try:
        result = subprocess.run(
            ['bash', SCRIPT_PATH, allowed_modes[mode]],
            capture_output=True, text=True, timeout=120,
            cwd=os.path.dirname(SCRIPT_PATH)
        )
        output = result.stdout.strip()
        # Parse results
        data = {
            'success': result.returncode == 0,
            'mode': mode,
            'output': output,
            'categories': {}
        }
        for line in output.split('\n'):
            if ' : ' in line and not line.startswith('=') and not line.startswith('-'):
                parts = line.strip().split(' : ')
                if len(parts) == 2:
                    try:
                        data['categories'][parts[0].strip()] = int(parts[1].strip())
                    except ValueError:
                        pass
            if 'AVANT:' in line:
                data['avant'] = line.strip()
            if 'APRES:' in line or 'APRÈS:' in line:
                data['apres'] = line.strip()
            if 'Total:' in line:
                data['total_line'] = line.strip()
        return data
    except subprocess.TimeoutExpired:
        return {"error": "Timeout: le script a pris plus de 2 minutes"}
    except Exception as e:
        logger.error(f"Error running email cleanup: {e}")
        return {"error": str(e)}

def register_email_cleanup_routes(app):
    """Register email cleanup routes on the Flask app."""

    @app.route("/api/email-cleanup/stats")
    def api_email_cleanup_stats():
        return jsonify(get_email_cleanup_stats())

    @app.route("/api/email-cleanup/<mode>", methods=["POST"])
    def api_email_cleanup_run(mode):
        result = run_email_cleanup(mode)
        if "error" in result:
            return jsonify(result), 400 if "invalide" in result.get("error","") else 500
        return jsonify(result)
