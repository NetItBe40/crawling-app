#!/usr/bin/env python3
"""
Bootstrap script - Creates the entire crawling-app project structure.
Run on the server: python3 bootstrap.py
"""
import os
import base64
import zlib

APP_DIR = os.path.expanduser("~/crawling-app")

FILES = {}

# === config/__init__.py ===
FILES["config/__init__.py"] = ""

# === config/settings.py ===
FILES["config/settings.py"] = '''"""
Configuration globale du projet Crawling Website
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

DB_CONFIG = {
    "crawling": {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 3306)),
        "user": os.getenv("DB_USER", "netit972_crawling_website"),
        "password": os.getenv("DB_PASSWORD", "Qkrevc49RqRhMPubaauv"),
        "database": os.getenv("DB_NAME", "netit972_crawling_website"),
        "charset": "utf8mb4",
        "autocommit": True,
        "pool_name": "crawling_pool",
        "pool_size": 10,
    },
    "sirene": {
        "host": os.getenv("SIRENE_DB_HOST", "localhost"),
        "port": int(os.getenv("SIRENE_DB_PORT", 3306)),
        "user": os.getenv("SIRENE_DB_USER", "netit972_crawling_website"),
        "password": os.getenv("SIRENE_DB_PASSWORD", "Qkrevc49RqRhMPubaauv"),
        "database": os.getenv("SIRENE_DB_NAME", "netit972_netit972_sirene_db"),
        "charset": "utf8mb4",
        "autocommit": True,
        "pool_name": "sirene_pool",
        "pool_size": 5,
    },
}

HTTP_CHECKER = {
    "batch_size": 500,
    "timeout": 10,
    "max_concurrent": 50,
    "retry_count": 2,
    "delay_between_batches": 0.5,
    "user_agent": "Mozilla/5.0 (compatible; CrawlingBot/1.0)",
    "recheck_days": 30,
}

CRAWLER = {
    "batch_size": 100,
    "max_pages_per_domain": 10,
    "page_timeout": 15000,
    "navigation_timeout": 30000,
    "delay_min": 1.0,
    "delay_max": 2.0,
    "max_concurrent_browsers": 3,
    "user_agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "contact_page_patterns": [
        "contact", "nous-contacter", "contactez-nous", "about", "a-propos",
        "qui-sommes-nous", "mentions-legales", "legal", "imprint",
        "cgv", "cgu", "conditions-generales"
    ],
}

KEYWORDS = {
    "min_length": 3,
    "use_soundex": True,
    "stop_words_file": BASE_DIR / "config" / "french_stop_words.txt",
}

SIRETISATION = {
    "batch_size": 200,
    "score_threshold": 50,
    "weights": {
        "siret_exact": 100,
        "siren_exact": 80,
        "denomination": 40,
        "enseigne": 30,
        "code_postal": 20,
        "ville": 15,
        "adresse": 15,
        "telephone": 10,
        "naf_keyword": 5,
    },
}

PARKING_PATTERNS = {
    "registrars": [
        "godaddy", "namecheap", "ovh", "gandi", "ionos", "1and1",
        "sedo", "dan.com", "afternic", "hugedomains",
    ],
    "title_patterns": [
        "domain parking", "parked domain", "this domain is for sale",
        "buy this domain", "domain name for sale", "coming soon",
        "under construction", "site en construction",
        "ce domaine est a vendre", "nom de domaine a vendre",
    ],
}

DASHBOARD = {
    "host": "0.0.0.0",
    "port": int(os.getenv("DASHBOARD_PORT", 5050)),
    "debug": os.getenv("DASHBOARD_DEBUG", "false").lower() == "true",
    "secret_key": os.getenv("DASHBOARD_SECRET", "crawling-dashboard-secret-2024"),
}

SOCIAL_PATTERNS = {
    1: {"name": "Facebook", "patterns": [r"facebook\\.com/[\\w.\\-]+", r"fb\\.com/[\\w.\\-]+"]},
    2: {"name": "LinkedIn", "patterns": [r"linkedin\\.com/(?:company|in)/[\\w.\\-]+"]},
    3: {"name": "Twitter", "patterns": [r"(?:twitter|x)\\.com/[\\w.\\-]+"]},
    4: {"name": "Societe", "patterns": [r"societe\\.com/societe/[\\w.\\-]+"]},
}

EXTRACT_PATTERNS = {
    "email": r"[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}",
    "phone_fr": r"(?:(?:\\+33|0033|0)\\s*[1-9])(?:[\\s.\\-]?\\d{2}){4}",
    "siret": r"\\b\\d{3}\\s?\\d{3}\\s?\\d{3}\\s?\\d{5}\\b",
    "siren": r"\\b\\d{3}\\s?\\d{3}\\s?\\d{3}\\b",
    "postal_code_fr": r"\\b(?:0[1-9]|[1-8]\\d|9[0-5]|97[1-6])\\d{3}\\b",
}
'''

# === config/french_stop_words.txt ===
FILES["config/french_stop_words.txt"] = """le
la
les
un
une
des
du
de
au
aux
ce
ces
cet
cette
mon
ma
mes
ton
ta
tes
son
sa
ses
notre
nos
votre
vos
leur
leurs
je
tu
il
elle
nous
vous
ils
elles
me
te
se
en
et
ou
mais
donc
car
ni
que
qui
quoi
dont
est
sont
fait
sur
pour
par
dans
avec
sans
sous
entre
vers
chez
plus
moins
tout
tous
toute
toutes
pas
aussi
bien
comme
peu
tres
trop
assez
encore
tant
quel
quelle
quels
quelles
chaque
avant
apres
depuis
pendant
quand
comment
pourquoi
etre
avoir
faire
dire
aller
voir
venir
pouvoir
vouloir
devoir
savoir
bon
mal
grand
petit
nouveau
vieux
premier
dernier
seul
meme
page
site
web
www
http
https
com
org
net
accueil
menu
navigation
footer
header
copyright
droits
reserved
click
cliquez
lire
suite
recherche
search
home
contact
about
the
and
for
this
that
with
from
your
are
was
not
but
all
can
you
will
one
also
than
any
only
new
some
just
these
other
which
when
what
there
their
non
oui
alors
aucun
rien
jamais
personne
seulement
cela
voici
voila"""

# === utils/__init__.py ===
FILES["utils/__init__.py"] = ""

# === utils/database.py ===
FILES["utils/database.py"] = '''"""
Database connection pool manager
"""
import logging
from contextlib import contextmanager
from mysql.connector import pooling, Error as MySQLError
from config.settings import DB_CONFIG

logger = logging.getLogger(__name__)

class DatabaseManager:
    _pools = {}

    @classmethod
    def get_pool(cls, db_name="crawling"):
        if db_name not in cls._pools:
            config = DB_CONFIG[db_name].copy()
            pool_name = config.pop("pool_name", f"{db_name}_pool")
            pool_size = config.pop("pool_size", 5)
            try:
                cls._pools[db_name] = pooling.MySQLConnectionPool(
                    pool_name=pool_name, pool_size=pool_size,
                    pool_reset_session=True, **config)
                logger.info(f"Pool \\'{pool_name}\\' created (size={pool_size})")
            except MySQLError as e:
                logger.error(f"Failed to create pool: {e}")
                raise
        return cls._pools[db_name]

    @classmethod
    @contextmanager
    def get_connection(cls, db_name="crawling"):
        pool = cls.get_pool(db_name)
        conn = None
        try:
            conn = pool.get_connection()
            yield conn
        except MySQLError as e:
            logger.error(f"DB error on \\'{db_name}\\': {e}")
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()

    @classmethod
    @contextmanager
    def get_cursor(cls, db_name="crawling", dictionary=True, buffered=True):
        with cls.get_connection(db_name) as conn:
            cursor = conn.cursor(dictionary=dictionary, buffered=buffered)
            try:
                yield cursor
                conn.commit()
            except MySQLError as e:
                conn.rollback()
                logger.error(f"Query error: {e}")
                raise
            finally:
                cursor.close()

    @classmethod
    def execute_many(cls, query, data, db_name="crawling", batch_size=1000):
        total = len(data)
        inserted = 0
        with cls.get_connection(db_name) as conn:
            cursor = conn.cursor()
            try:
                for i in range(0, total, batch_size):
                    batch = data[i:i + batch_size]
                    cursor.executemany(query, batch)
                    conn.commit()
                    inserted += len(batch)
            except MySQLError as e:
                conn.rollback()
                logger.error(f"Batch insert error: {e}")
                raise
            finally:
                cursor.close()
        return inserted

    @classmethod
    def execute_query(cls, query, params=None, db_name="crawling", dictionary=True, fetch="all"):
        with cls.get_cursor(db_name, dictionary=dictionary) as cursor:
            cursor.execute(query, params)
            if fetch == "all": return cursor.fetchall()
            elif fetch == "one": return cursor.fetchone()
            elif fetch == "none": return cursor.rowcount
            return cursor.fetchall()

    @classmethod
    def close_all(cls):
        cls._pools.clear()
'''

# === utils/logger.py ===
FILES["utils/logger.py"] = '''"""
Logging configuration
"""
import logging
import sys
from logging.handlers import RotatingFileHandler
from config.settings import LOG_DIR

def setup_logger(name, log_file=None, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    logger.addHandler(console)

    if log_file:
        fh = RotatingFileHandler(
            LOG_DIR / log_file, maxBytes=50*1024*1024, backupCount=5, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger
'''

# === modules/__init__.py ===
FILES["modules/__init__.py"] = ""

# === requirements.txt ===
FILES["requirements.txt"] = """aiohttp>=3.9.0
mysql-connector-python>=8.2.0
playwright>=1.40.0
flask>=3.0.0
gunicorn>=21.2.0
"""

# === ecosystem.config.js ===
FILES["ecosystem.config.js"] = '''module.exports = {
  apps: [
    {
      name: "http-checker",
      script: "python3",
      args: "-m modules.http_checker",
      cwd: "/home/netit972/crawling-app",
      interpreter: "none",
      autorestart: true, watch: false, max_restarts: 10, restart_delay: 5000,
      env: { PYTHONPATH: "/home/netit972/crawling-app", PYTHONUNBUFFERED: "1" },
      log_file: "/home/netit972/crawling-app/logs/pm2-http-checker.log",
      error_file: "/home/netit972/crawling-app/logs/pm2-http-checker-error.log",
      out_file: "/home/netit972/crawling-app/logs/pm2-http-checker-out.log",
    },
    {
      name: "crawler",
      script: "python3",
      args: "-m modules.crawler",
      cwd: "/home/netit972/crawling-app",
      interpreter: "none",
      autorestart: true, watch: false, max_restarts: 10, restart_delay: 10000,
      env: { PYTHONPATH: "/home/netit972/crawling-app", PYTHONUNBUFFERED: "1" },
      log_file: "/home/netit972/crawling-app/logs/pm2-crawler.log",
      error_file: "/home/netit972/crawling-app/logs/pm2-crawler-error.log",
      out_file: "/home/netit972/crawling-app/logs/pm2-crawler-out.log",
    },
    {
      name: "siretisation",
      script: "python3",
      args: "-m modules.siretisation",
      cwd: "/home/netit972/crawling-app",
      interpreter: "none",
      autorestart: true, watch: false, max_restarts: 10, restart_delay: 10000,
      env: { PYTHONPATH: "/home/netit972/crawling-app", PYTHONUNBUFFERED: "1" },
      log_file: "/home/netit972/crawling-app/logs/pm2-siretisation.log",
      error_file: "/home/netit972/crawling-app/logs/pm2-siretisation-error.log",
      out_file: "/home/netit972/crawling-app/logs/pm2-siretisation-out.log",
    },
    {
      name: "dashboard",
      script: "gunicorn",
      args: "--bind 0.0.0.0:5050 --workers 2 --timeout 120 modules.dashboard:app",
      cwd: "/home/netit972/crawling-app",
      interpreter: "none",
      autorestart: true, watch: false, max_restarts: 10, restart_delay: 5000,
      env: { PYTHONPATH: "/home/netit972/crawling-app", PYTHONUNBUFFERED: "1" },
      log_file: "/home/netit972/crawling-app/logs/pm2-dashboard.log",
      error_file: "/home/netit972/crawling-app/logs/pm2-dashboard-error.log",
      out_file: "/home/netit972/crawling-app/logs/pm2-dashboard-out.log",
    },
  ],
};
'''

# === static/.gitkeep ===
FILES["static/.gitkeep"] = ""

# === templates/.gitkeep ===
FILES["templates/.gitkeep"] = ""

# === logs/.gitkeep ===
FILES["logs/.gitkeep"] = ""


def create_project():
    print(f"Creating project in {APP_DIR}...")

    for rel_path, content in FILES.items():
        full_path = os.path.join(APP_DIR, rel_path)
        dir_name = os.path.dirname(full_path)
        os.makedirs(dir_name, exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"  Created: {ing-app"LError aspy-rinrs, t===
FILpret] # Trade namo
}t===
Ftf-8")
r aspy"Now me].n on 120 mot===
FI(les.http_cheit_, pm2-crawt_, pm2-siretisatpquery2-dashbopy)tf-8")
r aspy"onn  on ry2-dashb === temp
dery, telyR}...
   ger(__naetch __e do__"as f:
def create_proje.2.0
