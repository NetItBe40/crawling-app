#!/bin/bash
# Charger les credentials depuis .env
ENV_FILE="$(dirname "$0")/../.env"
[ -f "$ENV_FILE" ] && source "$ENV_FILE"
DB_USER="${DB_USER:-netit972_crawling_website}"
DB_PASS="${DB_PASS:?Erreur: DB_PASS non defini. Creer un fichier .env}"
DB_NAME="${DB_NAME:-netit972_crawling_website}"
LOG="/tmp/nettoyage_$(date +%Y%m%d_%H%M%S).log"
run_sql() { mysql -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" -N -e "$1" 2>/dev/null; }
count_a() { run_sql "SELECT COUNT(*) FROM APP_email WHERE deleted=0;"; }
count_d() { run_sql "SELECT COUNT(*) FROM APP_email WHERE deleted=1;"; }
show() { echo "ACTIFS=$(count_a) | SUPPRIMES=$(count_d)"; }
do_marquage() {
  local m="$1" t=0
  echo "=== MARQUAGE ($m) ==="
  local -a P=(
"noreply|email LIKE '%noreply%' OR email LIKE '%no-reply%' OR email LIKE '%no_reply%' OR email LIKE '%ne-pas-repondre%'"
"wordpress|email LIKE 'wordpress@%' OR email LIKE 'wp@%' OR email LIKE '%@wordpress.com' OR email LIKE '%@wordpress.org'"
"webmaster/admin|email LIKE 'webmaster@%' OR email LIKE 'postmaster@%' OR email LIKE 'root@%' OR email LIKE 'admin@%' OR email LIKE 'administrator@%'"
"abuse/host/sec|email LIKE 'abuse@%' OR email LIKE 'hostmaster@%' OR email LIKE 'security@%'"
"test/example|email LIKE 'test@%' OR email LIKE '%@test.com' OR email LIKE '%@test.fr' OR email LIKE 'example@%' OR email LIKE 'exemple@%' OR email LIKE '%@example.com' OR email LIKE '%@example.org'"
"newsletter/bounce|email LIKE 'newsletter@%' OR email LIKE 'unsubscribe@%' OR email LIKE '%unsubscribe%' OR email LIKE 'bounce@%' OR email LIKE 'bounce-%'"
"privacy/rgpd|email LIKE 'privacy@%' OR email LIKE 'dpo@%' OR email LIKE 'rgpd@%' OR email LIKE 'gdpr@%' OR email LIKE 'donnees-personnelles@%'"
"daemon|email LIKE 'daemon@%' OR email LIKE 'mailer-daemon@%' OR email LIKE 'mailerdaemon@%'"
"system/cron|email LIKE 'cron@%' OR email LIKE 'system@%' OR email LIKE 'server@%' OR email LIKE 'devops@%' OR email LIKE 'sysadmin@%'"
  )
  local -a P2=(
"placeholders|email LIKE 'votre-courriel@%' OR email LIKE 'vous@%' OR email LIKE 'votre@%' OR email LIKE 'nom@%' OR email LIKE 'prenom@%' OR email LIKE 'email@%' OR email LIKE 'user@%' OR email LIKE 'name@%'"
"hash hex|email REGEXP '^[0-9a-f]{20,}[^@]*@'"
"sentry/wix/gandi|email LIKE '%@%sentry%' OR email LIKE '%@%wixpress%' OR email LIKE '%@contact.gandi.net'"
"domaines exemple|email LIKE '%@%exemple%' OR email LIKE '%@%example%'"
"support@|email LIKE 'support@%'"
"malformes ..|email LIKE '%..%'"
"TLD fantaisistes|email REGEXP '\\\\.(aaa|ajd|aro|asn|atom|avif|bai|beer|berlin|bl|bou|byq|bzh|casino|cex|clickservices|comconsulter|comdirecteur|comfroggy|comjanuary|coml|comle|comnum|compar|comsiret|comsite|comt|comtumblr|coupon|dev|digitalsite|direct|dyh|earth|eirelav|eiriam|elleanewg|enirak|enitram|erg|eurcs|fet|flour|fradresse|frakemekem|frc|frcrombouts|frdirecteur|frforme|frformulaire|frh|frhonoraires|frif|frimmatriculation|frivergier|frlrosa|frn|frnotre|frnum|frou|frpieux|frplus|frpour|frr|frraison|frrepr|frsas|frscordonnier|frsi|frsite|frt|frtel|frveuillez|frvous|games|gidiag|group|gx|gy|hed|icns|imprimerie|jh|jhf|jpeg|jsd|jxa|kc|kjq|kwy|ldi|leclerc|lg|lo|love|morineau|mua|network|odb|oe|oembed|omk|orglinkedin|orgtable|orgw|pbz|pdf|pga|plou|pro|pub|pya|qei|qiu|reivilo|rn|rq|rxo|ryc|skq|sne|social|some|tld|tq|ty|uc|webp|website|xn|xx|xyz|yr|zhp|zv|zy)$'"
"TLD etrangers|email REGEXP '\\\\.(ae|ag|ar|au|be|bf|bz|ca|cc|ch|cn|co|cz|de|dk|ec|edu|es|eu|fi|gr|hk|hr|hu|il|io|it|jp|kr|li|lt|lu|ly|ma|me|media|mg|mp|mr|mt|mu|mx|nc|net|nl|pf|pl|pm|pt|re|ru|se|sk|th|tn|tr|ua|uk|us|ve|yt)$'"
  )
  for p in "${P[@]}" "${P2[@]}"; do
    IFS='|' read -r d c <<< "$p"
    local n=$(run_sql "SELECT COUNT(*) FROM APP_email WHERE deleted=0 AND ($c);")
    if [ "$n" -gt 0 ]; then
      [ "$m" = "execute" ] && run_sql "UPDATE APP_email SET deleted=1, deleted_at=NOW() WHERE deleted=0 AND ($c);"
      echo "  $d : $n"; t=$((t+n))
    fi
  done
  echo "Total: $t ($m)" | tee -a "$LOG"
}
do_lowercase() {
  echo "=== LOWERCASE ==="
  local u=$(run_sql "SELECT COUNT(*) FROM APP_email WHERE email != LOWER(email);")
  echo "Emails avec majuscules: $u"
  [ "$u" -eq 0 ] && echo "Rien a faire." && return 0
  echo "Drop index..."; run_sql "ALTER TABLE APP_email DROP INDEX id_domaine_email;" 2>/dev/null
  echo "LOWER..."; run_sql "UPDATE APP_email SET email = LOWER(email) WHERE email != LOWER(email);"
  echo "Dedup actifs..."
  run_sql "UPDATE APP_email e1 INNER JOIN (SELECT id_domaine, email, MIN(ID) AS min_id FROM APP_email WHERE deleted=0 GROUP BY id_domaine, email HAVING COUNT(*) > 1) dups ON e1.id_domaine = dups.id_domaine AND e1.email = dups.email AND e1.ID != dups.min_id SET e1.deleted = 1, e1.deleted_at = NOW() WHERE e1.deleted = 0;"
  echo "Hard delete doublons deleted..."
  run_sql "DELETE e1 FROM APP_email e1 INNER JOIN APP_email e2 ON e1.id_domaine = e2.id_domaine AND e1.email = e2.email AND e1.ID != e2.ID WHERE e1.deleted = 1 AND e2.deleted = 0;"
  run_sql "DELETE e1 FROM APP_email e1 INNER JOIN (SELECT id_domaine, email, MIN(ID) AS min_id FROM APP_email GROUP BY id_domaine, email HAVING COUNT(*) > 1) dups ON e1.id_domaine = dups.id_domaine AND e1.email = dups.email AND e1.ID != dups.min_id WHERE e1.deleted = 1;"
  local r=$(run_sql "SELECT COUNT(*) FROM (SELECT id_domaine, email FROM APP_email GROUP BY id_domaine, email HAVING COUNT(*) > 1) t;")
  if [ "$r" -eq 0 ]; then
    echo "Recreate index..."; run_sql "ALTER TABLE APP_email ADD UNIQUE INDEX id_domaine_email (id_domaine, email);"
    echo "Lowercase OK"
  else echo "ERREUR: $r doublons restants!"; return 1; fi
}
echo "=== NETTOYAGE EMAILS $(date) ===" | tee -a "$LOG"
echo "AVANT: $(show)"
case "${1:-}" in
  --dry-run) do_marquage "dry-run";;
  --stats) show; run_sql "SELECT COUNT(DISTINCT email) FROM APP_email WHERE deleted=0;";;
  --marquage) do_marquage "execute"; echo "APRES: $(show)";;
  --lowercase) do_lowercase; echo "APRES: $(show)";;
  ""|-f|--full) do_marquage "execute"; do_lowercase; echo "APRES: $(show)";;
  *) echo "Usage: $0 [--dry-run|--stats|--marquage|--lowercase|--full]"; exit 1;;
esac
echo "Termine. Log: $LOG"
