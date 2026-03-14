# Changelog

## [1.0.0] - 2026-03-14

### Corrections critiques

- **Fix connexion MySQL** : Ajout de `unix_socket: /var/lib/mysql/mysql.sock` dans `config/settings.py` pour les deux bases (crawling + sirene). Resout l'erreur `Can't connect to MySQL server on 'localhost:3306' (111)` qui survenait de maniere intermittente.

- **Reset flag_crawling** : 177 domaines etaient bloques avec `flag_crawling=1` suite a des crashs precedents. Correction via `UPDATE APP_domaine SET flag_crawling = 0 WHERE flag_crawling = 1 AND flag_data_collected = 0`.

- **Stabilisation siretisation** : Le service avait accumule 582 restarts en boucle a cause des erreurs MySQL. Stabilise apres le fix unix_socket.

### Documentation

- Creation de `README.md` avec architecture, stack technique, installation et services PM2
- Creation de `config/settings.py.example` (template sans credentials)
- Creation de `.gitignore` adapte au projet

### Infrastructure

- Gestion des 5 services via PM2 (`ecosystem.config.js`)
- Dashboard Flask/Gunicorn sur port 5050
- Crawler Playwright/Chromium fonctionnel (lots de 100 domaines)
