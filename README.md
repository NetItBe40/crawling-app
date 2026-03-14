# Crawling App - Business Intelligence France

Application de crawling web pour collecter les donnees de contact des entreprises francaises (~6.5M domaines) et les croiser avec le registre SIRENE.

## Architecture

- **Crawler** : Playwright + Chromium headless, extraction d'emails, telephones, adresses, reseaux sociaux, SIRET/SIREN
- **Dashboard** : Flask + Gunicorn (port 5050), monitoring en temps reel
- **HTTP Checker** : aiohttp, verification asynchrone de la disponibilite des domaines
- **Sirene API** : API REST pour interroger la base SIRENE (29.3M entreprises)
- **Siretisation** : Croisement automatique domaines crawles / base SIRENE

## Stack technique

- Python 3.13, Playwright 1.58, MariaDB 10.11, PM2
- Serveur : Debian 13 (trixie) / DirectAdmin Evolution
- Connexion DB via Unix socket `/var/lib/mysql/mysql.sock`

## Installation

1. `cp config/settings.py.example config/settings.py` et renseigner les identifiants DB
2. `python -m venv venv && source venv/bin/activate`
3. `pip install -r requirements.txt`
4. `playwright install chromium`
5. `pm2 start ecosystem.config.js`

## Services PM2

| Service | Description |
|---------|-------------|
| crawler | Crawling par lots de 100 domaines |
| dashboard | Interface web de monitoring |
| http-checker | Verification HTTP asynchrone |
| sirene-api | API de recherche SIRENE |
| siretisation | Croisement domaines/SIRENE |
