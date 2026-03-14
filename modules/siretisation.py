"""
Module 3 - SIRETisation
Algorithme de scoring multi-critères pour associer les domaines crawlés
aux entreprises du registre SIRENE.
"""
import asyncio
import re
import signal
import time
from datetime import datetime
from difflib import SequenceMatcher

from config.settings import SIRETISATION
from utils.database import DatabaseManager
from utils.logger import setup_logger

logger = setup_logger("siretisation", "siretisation.log")

shutdown_event = asyncio.Event()


def signal_handler(sig, frame):
    logger.info("Shutdown signal received...")
    shutdown_event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def string_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    a = normalize_text(a)
    b = normalize_text(b)
    if a == b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


class SIRETMatcher:
    def __init__(self):
        self.config = SIRETISATION
        self.weights = self.config["weights"]
        self.stats = {
            "processed": 0,
            "matched": 0,
            "no_match": 0,
            "errors": 0,
            "start_time": None,
        }

    def get_domains_to_match(self, batch_size: int = None) -> list:
        size = batch_size or self.config["batch_size"]
        query = """
            SELECT d.ID, d.domaine, d.description
            FROM APP_domaine d
            WHERE d.deleted = 0
              AND d.flag_data_collected = 1
              AND d.ID NOT IN (
                  SELECT DISTINCT ID FROM APP_domaine_SIRETISATION
                  WHERE ThG_MR_score IS NOT NULL AND ThG_MR_score > 0
              )
            ORDER BY d.crawled_at DESC
            LIMIT %s
        """
        return DatabaseManager.execute_query(query, (size,))

    def get_domain_data(self, domain_id: int) -> dict:
        data = {"id": domain_id}

        data["emails"] = DatabaseManager.execute_query(
            "SELECT email FROM APP_email WHERE id_domaine = %s AND deleted = 0",
            (domain_id,)
        )

        data["phones"] = DatabaseManager.execute_query(
            "SELECT numero FROM APP_telephone WHERE id_domaine = %s AND deleted = 0",
            (domain_id,)
        )

        data["addresses"] = DatabaseManager.execute_query(
            "SELECT numero, voie, code_postal, ville FROM APP_adresse WHERE id_domaine = %s AND deleted = 0",
            (domain_id,)
        )

        data["sirets"] = DatabaseManager.execute_query(
            "SELECT siret, siren FROM APP_siret WHERE id_domaine = %s AND deleted = 0",
            (domain_id,)
        )
        data["sirens"] = DatabaseManager.execute_query(
            "SELECT siren FROM APP_siren WHERE id_domaine = %s AND deleted = 0",
            (domain_id,)
        )

        data["social"] = DatabaseManager.execute_query(
            "SELECT url, type FROM APP_reseau_sociaux WHERE id_domaine = %s AND deleted = 0",
            (domain_id,)
        )

        data["keywords"] = DatabaseManager.execute_query(
            "SELECT mot_cle, repetition FROM APP_mot_cle WHERE id_domaine = %s AND deleted = 0 ORDER BY repetition DESC LIMIT 50",
            (domain_id,)
        )

        return data

    def search_by_siret(self, siret: str) -> list:
        return DatabaseManager.execute_query(
            """SELECT e.siret, e.siren, e.enseigne_1, e.activite_principale,
                      e.libelle_voie, e.code_postal, e.libelle_commune,
                      ent.denomination, ent.sigle, ent.categorie_entreprise,
                      ent.activite_principale as activite_ent
               FROM etablissements e
               JOIN entreprises ent ON e.siren = ent.siren
               WHERE e.siret = %s AND e.etat_administratif = 'A'
               LIMIT 1""",
            (siret,), db_name="sirene"
        )

    def search_by_siren(self, siren: str) -> list:
        return DatabaseManager.execute_query(
            """SELECT e.siret, e.siren, e.enseigne_1, e.activite_principale,
                      e.libelle_voie, e.code_postal, e.libelle_commune,
                      e.etablissement_siege,
                      ent.denomination, ent.sigle, ent.categorie_entreprise
               FROM etablissements e
               JOIN entreprises ent ON e.siren = ent.siren
               WHERE e.siren = %s AND e.etat_administratif = 'A'
               ORDER BY e.etablissement_siege DESC
               LIMIT 10""",
            (siren,), db_name="sirene"
        )

    def search_by_name_postal(self, name: str, postal_code: str = None) -> list:
        name_normalized = normalize_text(name)
        if not name_normalized or len(name_normalized) < 3:
            return []

        words = [w for w in name_normalized.split() if len(w) >= 3]
        if not words:
            return []

        search_term = f"%{words[0]}%"

        if postal_code and len(postal_code) == 5:
            query = """
                SELECT e.siret, e.siren, e.enseigne_1, e.activite_principale,
                       e.libelle_voie, e.code_postal, e.libelle_commune,
                       e.etablissement_siege,
                       ent.denomination, ent.sigle
                FROM etablissements e
                JOIN entreprises ent ON e.siren = ent.siren
                WHERE e.etat_administratif = 'A'
                  AND e.code_postal = %s
                  AND (ent.denomination LIKE %s OR e.enseigne_1 LIKE %s)
                LIMIT 50
            """
            return DatabaseManager.execute_query(
                query, (postal_code, search_term, search_term), db_name="sirene"
            )
        else:
            query = """
                SELECT e.siret, e.siren, e.enseigne_1, e.activite_principale,
                       e.libelle_voie, e.code_postal, e.libelle_commune,
                       e.etablissement_siege,
                       ent.denomination, ent.sigle
                FROM entreprises ent
                JOIN etablissements e ON e.siren = ent.siren AND e.etablissement_siege = 1
                WHERE ent.etat_administratif = 'A'
                  AND ent.denomination LIKE %s
                LIMIT 50
            """
            return DatabaseManager.execute_query(
                query, (search_term,), db_name="sirene"
            )

    def score_candidate(self, candidate: dict, domain_data: dict, domain_info: dict) -> dict:
        score = 0
        details = {}

        candidate_siret = candidate.get("siret", "")
        website_sirets = [s["siret"] for s in domain_data.get("sirets", [])]
        if candidate_siret in website_sirets:
            score += self.weights["siret_exact"]
            details["siret_exact"] = self.weights["siret_exact"]

        candidate_siren = candidate.get("siren", "")
        website_sirens = [s["siren"] for s in domain_data.get("sirens", [])]
        website_sirens += [s.get("siren", "") for s in domain_data.get("sirets", [])]
        if candidate_siren in website_sirens:
            score += self.weights["siren_exact"]
            details["siren_exact"] = self.weights["siren_exact"]

        denom = candidate.get("denomination", "")
        enseigne = candidate.get("enseigne_1", "")

        domain_name = domain_info.get("domaine", "")
        domain_base = domain_name.split(".")[0] if domain_name else ""

        best_denom_score = 0
        if denom:
            sim = string_similarity(domain_base, denom)
            best_denom_score = max(best_denom_score, sim)

            for kw in domain_data.get("keywords", [])[:20]:
                sim = string_similarity(kw["mot_cle"], normalize_text(denom))
                best_denom_score = max(best_denom_score, sim)

            desc = domain_info.get("description", "")
            if desc and denom:
                if normalize_text(denom) in normalize_text(desc):
                    best_denom_score = max(best_denom_score, 0.9)

        if best_denom_score > 0.5:
            denom_points = int(self.weights["denomination"] * best_denom_score)
            score += denom_points
            details["denomination"] = denom_points

        if enseigne:
            best_ens_score = string_similarity(domain_base, enseigne)
            if best_ens_score > 0.5:
                ens_points = int(self.weights["enseigne"] * best_ens_score)
                score += ens_points
                details["enseigne"] = ens_points

        candidate_cp = candidate.get("code_postal", "")
        for addr in domain_data.get("addresses", []):
            if addr.get("code_postal") and addr["code_postal"] == candidate_cp:
                score += self.weights["code_postal"]
                details["code_postal"] = self.weights["code_postal"]
                break

        candidate_ville = normalize_text(candidate.get("libelle_commune", ""))
        for addr in domain_data.get("addresses", []):
            ville = normalize_text(addr.get("ville", ""))
            if ville and candidate_ville and string_similarity(ville, candidate_ville) > 0.8:
                score += self.weights["ville"]
                details["ville"] = self.weights["ville"]
                break

        candidate_voie = normalize_text(candidate.get("libelle_voie", ""))
        for addr in domain_data.get("addresses", []):
            voie = normalize_text(addr.get("voie", ""))
            if voie and candidate_voie and string_similarity(voie, candidate_voie) > 0.6:
                score += self.weights["adresse"]
                details["adresse"] = self.weights["adresse"]
                break

        return {
            "siret": candidate_siret,
            "siren": candidate_siren,
            "denomination": denom,
            "enseigne": enseigne or "",
            "code_postal": candidate_cp,
            "ville": candidate.get("libelle_commune", ""),
            "adresse": candidate.get("libelle_voie", ""),
            "score": score,
            "details": details,
            "etablissement_siege": candidate.get("etablissement_siege", 0),
        }

    def match_domain(self, domain: dict) -> dict:
        domain_id = domain["ID"]
        domain_data = self.get_domain_data(domain_id)

        candidates = []

        for siret_rec in domain_data.get("sirets", []):
            results = self.search_by_siret(siret_rec["siret"])
            candidates.extend(results)

        all_sirens = set()
        for s in domain_data.get("sirens", []):
            all_sirens.add(s["siren"])
        for s in domain_data.get("sirets", []):
            all_sirens.add(s.get("siren", s.get("siret", "")[:9]))

        for siren in all_sirens:
            results = self.search_by_siren(siren)
            candidates.extend(results)

        if domain_data.get("addresses"):
            for addr in domain_data["addresses"][:3]:
                domain_base = domain["domaine"].split(".")[0]
                results = self.search_by_name_postal(domain_base, addr.get("code_postal"))
                candidates.extend(results)

        if not candidates:
            domain_base = domain["domaine"].split(".")[0]
            if len(domain_base) >= 3:
                results = self.search_by_name_postal(domain_base)
                candidates.extend(results)

        seen = set()
        unique_candidates = []
        for c in candidates:
            siret = c.get("siret", "")
            if siret and siret not in seen:
                seen.add(siret)
                unique_candidates.append(c)

        scored = []
        for candidate in unique_candidates:
            scored_result = self.score_candidate(candidate, domain_data, domain)
            if scored_result["score"] > 0:
                scored.append(scored_result)

        scored.sort(key=lambda x: x["score"], reverse=True)

        return {
            "domain_id": domain_id,
            "domain_name": domain["domaine"],
            "best_match": scored[0] if scored else None,
            "all_matches": scored[:7],
            "total_candidates": len(unique_candidates),
        }

    def save_match_result(self, match_result: dict):
        domain_id = match_result["domain_id"]
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        best = match_result["best_match"]
        if not best:
            self.stats["no_match"] += 1
            return

        domain_data = self.get_domain_data(domain_id)
        addr = domain_data["addresses"][0] if domain_data.get("addresses") else {}

        import json

        all_matches = match_result["all_matches"]
        match_jsons = {}
        for i, m in enumerate(all_matches[:7], 1):
            match_jsons[f"ThG_json_MR_{i}"] = json.dumps({
                "siret": m["siret"],
                "siren": m["siren"],
                "denomination": m["denomination"],
                "enseigne": m["enseigne"],
                "score": m["score"],
                "details": m["details"],
                "code_postal": m["code_postal"],
                "ville": m["ville"],
            }, ensure_ascii=False)

        query = """
            INSERT INTO APP_domaine_SIRETISATION
                (ID, nom_societe, adresse, code_postal, ville,
                 ThG_siret, ThG_denom, ThG_MR_score,
                 ThG_json_MR_1, ThG_json_MR_2, ThG_json_MR_3,
                 ThG_json_MR_4, ThG_json_MR_5, ThG_json_MR_6, ThG_json_MR_7,
                 created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                nom_societe = VALUES(nom_societe),
                adresse = VALUES(adresse),
                code_postal = VALUES(code_postal),
                ville = VALUES(ville),
                ThG_siret = VALUES(ThG_siret),
                ThG_denom = VALUES(ThG_denom),
                ThG_MR_score = VALUES(ThG_MR_score),
                ThG_json_MR_1 = VALUES(ThG_json_MR_1),
                ThG_json_MR_2 = VALUES(ThG_json_MR_2),
                ThG_json_MR_3 = VALUES(ThG_json_MR_3),
                ThG_json_MR_4 = VALUES(ThG_json_MR_4),
                ThG_json_MR_5 = VALUES(ThG_json_MR_5),
                ThG_json_MR_6 = VALUES(ThG_json_MR_6),
                ThG_json_MR_7 = VALUES(ThG_json_MR_7),
                updated_at = VALUES(updated_at)
        """

        params = (
            domain_id,
            best["denomination"][:255] if best["denomination"] else "",
            f"{addr.get('numero', '')} {addr.get('voie', '')}".strip()[:255] if addr else "",
            addr.get("code_postal", "")[:10] if addr else "",
            addr.get("ville", "")[:100] if addr else "",
            best["siret"],
            best["denomination"][:255] if best["denomination"] else "",
            best["score"],
            match_jsons.get("ThG_json_MR_1", ""),
            match_jsons.get("ThG_json_MR_2", ""),
            match_jsons.get("ThG_json_MR_3", ""),
            match_jsons.get("ThG_json_MR_4", ""),
            match_jsons.get("ThG_json_MR_5", ""),
            match_jsons.get("ThG_json_MR_6", ""),
            match_jsons.get("ThG_json_MR_7", ""),
            now, now,
        )

        DatabaseManager.execute_query(query, params, fetch="none")

        if best["score"] >= self.config["score_threshold"]:
            self.stats["matched"] += 1
        else:
            self.stats["no_match"] += 1

    async def run(self):
        self.stats["start_time"] = time.time()
        logger.info("=== SIRETisation started ===")

        cycle = 0
        while not shutdown_event.is_set():
            domains = self.get_domains_to_match()

            if not domains:
                logger.info("No domains to match, sleeping 120s...")
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=120)
                except asyncio.TimeoutError:
                    pass
                continue

            cycle += 1
            logger.info(f"Cycle {cycle}: matching {len(domains)} domains...")

            for domain in domains:
                if shutdown_event.is_set():
                    break

                try:
                    result = self.match_domain(domain)
                    self.save_match_result(result)
                    self.stats["processed"] += 1

                    if self.stats["processed"] % 100 == 0:
                        logger.info(
                            f"Progress: processed={self.stats['processed']} | "
                            f"matched={self.stats['matched']} | "
                            f"no_match={self.stats['no_match']}"
                        )
                except Exception as e:
                    logger.error(f"Error matching {domain['domaine']}: {e}")
                    self.stats["errors"] += 1

        logger.info(f"=== SIRETisation stopped. Total: {self.stats['processed']} ===")


def main():
    matcher = SIRETMatcher()
    asyncio.run(matcher.run())


if __name__ == "__main__":
    main()
