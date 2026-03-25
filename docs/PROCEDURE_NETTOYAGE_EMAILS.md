# Procedure de Nettoyage des Emails Parasites

**Base :** `netit972_crawling_website` — **Table :** `APP_email`

---

## Principe

Les crawlers collectent des emails depuis les sites web. Parmi eux, beaucoup sont parasites (emails systeme, placeholders, domaines invalides). Cette procedure les marque en **soft-delete** (`deleted=1, deleted_at=NOW()`) pour les exclure sans perte de donnees.

**Ce qui est conserve :** les emails @gmail.com, @yahoo, @hotmail, @outlook, ainsi que les emails generiques utiles comme contact@, info@, commercial@.

---

## Fichiers

| Fichier | Role |
|---|---|
| `scripts/nettoyage_emails.sh` | Script principal |
| `scripts/.env.example` | Template des credentials |
| `.env` | Credentials (non versionne) |

---

## Installation

```bash
# Copier le template et renseigner le mot de passe
cp scripts/.env.example .env
nano .env
```

---

## Utilisation

```bash
# Nettoyage complet (marquage + lowercase)
bash scripts/nettoyage_emails.sh

# Simulation sans modifier (dry-run)
bash scripts/nettoyage_emails.sh --dry-run

# Statistiques uniquement
bash scripts/nettoyage_emails.sh --stats

# Marquage seul
bash scripts/nettoyage_emails.sh --marquage

# Lowercase seul
bash scripts/nettoyage_emails.sh --lowercase
```

---

## Categories d'emails marques

### Vague 1 — Emails systeme / techniques
- noreply, no-reply, ne-pas-repondre
- wordpress@, wp@
- webmaster@, postmaster@, root@, admin@, administrator@
- abuse@, hostmaster@, security@
- test@, example@, exemple@
- newsletter@, unsubscribe@, bounce@
- privacy@, dpo@, rgpd@, gdpr@
- daemon@, mailer-daemon@
- cron@, system@, server@, devops@, sysadmin@

### Vague 2 — Emails fictifs / placeholder / services
- Prefixes vous@, votre@, nom@, prenom@, email@, user@, name@
- Emails avec hash hexadecimal (>=20 caracteres avant le @)
- Services : @sentry, @wixpress, @contact.gandi.net

### Vague 3 — Domaines exemple/example + support
- Tout domaine contenant "exemple" ou "example"
- Tous les support@

### Vague 4 — Malformes + domaines etrangers
- Emails contenant ".." (malformes)
- TLD fantaisistes (.aaa, .comsite, .frformulaire, .xyz, etc.)
- TLD de pays etrangers (.de, .uk, .it, .es, .ch, .be, .nl, etc.)

---

## Normalisation lowercase

Le script convertit automatiquement les emails en minuscules et gere les doublons :
1. Suppression temporaire de l'index unique
2. `UPDATE SET email = LOWER(email)`
3. Soft-delete des doublons actifs (garde le MIN(ID))
4. Hard-delete des doublons deja supprimes
5. Recreation de l'index unique

---

## Historique

| Date | Action | Resultat |
|---|---|---|
| 2026-03-25 | Vague 1-4 | 317 346 emails parasites marques |
| 2026-03-25 | Lowercase + dedup | 2 391 690 actifs / 315 171 supprimes |

---

## Automatisation (crontab)

```bash
# Tous les jours a 3h du matin
0 3 * * * bash /home/netit972/crawling-app/scripts/nettoyage_emails.sh >> /tmp/nettoyage_emails_logs/cron.log 2>&1
```

## Notes

- Le script est **idempotent** : relancer ne modifie que les nouveaux emails (`WHERE deleted=0`)
- Les crawlers tournent en continu (3 instances) → les nouveaux emails sont nettoyes au prochain lancement
