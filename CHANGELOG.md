# Changelog

## [1.1.0] - 2026-03-16

### Performance - Optimisation SIRETisation (x5)

- **4 workers PM2 paralleles** : Nouveau fichier `ecosystem.siretisation.config.js` avec partitionnement par `MOD(d.ID, 4)` pour repartir la charge sur 4 processus independants
- **batch_size augmente** : De 200 a 500 domaines par cycle (+150%)
- **Sleep reduit** : De 120s a 30s entre les cycles (reactivite x4)
- **Requete SQL optimisee** : Remplacement de `NOT IN (subquery)` par `LEFT JOIN ... IS NULL` + fonction `MOD()` pour MariaDB
- **Resultat mesure** : Debit passe de ~30/h (~720/jour) a ~147/h (~3522/jour)

### Corrections

- **Dashboard recent-siretisation** : Correction ORDER BY `created_at` -> `updated_at` pour afficher les SIRETisations les plus recentes
- **Dashboard reporting** : Ajout graphiques Chart.js et statistiques detaillees
- **Crawler** : Ameliorations diverses du module de crawling
- **HTTP Checker** : Corrections mineures
- **Database** : Ameliorations du module utilitaire database

### Infrastructure

- Ajout `ecosystem.siretisation.config.js` pour gerer les 4 workers PM2 siretisation
- Variables d'environnement `WORKER_ID` et `NUM_WORKERS` pour le partitionnement
- Utilisation du virtualenv Python + PYTHONPATH dans la config PM2


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
