"""
Module 1 - HTTP Status Checker
Vérifie le statut HTTP de tous les domaines en batch asynchrone.
Met à jour APP_domaine.http_statut, http_code, http_at, http_iteration
et SERV_domain_check_http_status.
"""
import asyncio
import time
import signal
import sys
from datetime import datetime, timedelta

import aiohttp

from config.settings import HTTP_CHECKER
from utils.database import DatabaseManager
from utils.logger import setup_logger

logger = setup_logger("http_checker", "http_checker.log")

# Graceful shutdown
shutdown_event = asyncio.Event()


def signal_handler(sig, frame):
    logger.info("Shutdown signal received, finishing current batch...")
    shutdown_event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class HTTPChecker:
    def __init__(self):
        self.config = HTTP_CHECKER
        self.stats = {
            "checked": 0,
            "online": 0,
            "offline": 0,
            "errors": 0,
            "redirects": 0,
            "parking": 0,
            "start_time": None,
        }

    def get_domains_to_check(self, batch_size: int = None) -> list:
        """Get domains that need HTTP status check."""
        size = batch_size or self.config["batch_size"]
        recheck_days = self.config["recheck_days"]
        cutoff_date = (datetime.now() - timedelta(days=recheck_days)).strftime("%Y-%m-%d %H:%M:%S")

        query = """
            SELECT d.ID, d.domaine, d.url_full, d.http_iteration
            FROM APP_domaine d
            WHERE d.deleted = 0
              AND (
                d.http_at IS NULL
                OR d.http_at < %s
              )
            ORDER BY d.http_at ASC, d.ID ASC
            LIMIT %s
        """
        return DatabaseManager.execute_query(query, (cutoff_date, size))

    async def check_single_domain(self, session: aiohttp.ClientSession, domain: dict) -> dict:
        """Check HTTP status of a single domain."""
        domain_name = domain["domaine"]
        url = domain.get("url_full") or f"https://{domain_name}"

        if not url.startswith(("http://", "https://")):
            url = f"https://{url}"

        result = {
            "id": domain["ID"],
            "domaine": domain_name,
            "http_code": 0,
            "http_statut": 0,
            "redirect_url": None,
            "is_parking": False,
            "iteration": (domain.get("http_iteration") or 0) + 1,
        }

        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=self.config["timeout"]),
                allow_redirects=True,
                ssl=False,
            ) as response:
                result["http_code"] = response.status
                final_url = str(response.url)

                if 200 <= response.status < 400:
                    result["http_statut"] = 1  # Online

                    # Check for redirect to different domain
                    if domain_name not in final_url:
                        result["redirect_url"] = final_url

                    # Quick parking check on title
                    try:
                        content = await response.text(encoding="utf-8", errors="ignore")
                        title = self._extract_title(content)
                        if title and self._is_parking(title, content[:2000]):
                            result["is_parking"] = True
                    except Exception:
                        pass
                else:
                    result["http_statut"] = 0  # Offline

        except asyncio.TimeoutError:
            result["http_code"] = 408
            result["http_statut"] = 0
        except aiohttp.ClientError:
            result["http_code"] = 0
            result["http_statut"] = 0
        except Exception as e:
            logger.debug(f"Error checking {domain_name}: {e}")
            result["http_code"] = 0
            result["http_statut"] = 0

        return result

    def _extract_title(self, html: str) -> str:
        """Extract <title> from HTML."""
        import re
        match = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
        return match.group(1).strip() if match else ""

    def _is_parking(self, title: str, content_snippet: str) -> bool:
        """Check if domain appears to be parked."""
        from config.settings import PARKING_PATTERNS
        text = (title + " " + content_snippet).lower()
        for pattern in PARKING_PATTERNS["title_patterns"]:
            if pattern.lower() in text:
                return True
        return False

    def update_results(self, results: list):
        """Update database with check results."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Update APP_domaine
        update_domain = """
            UPDATE APP_domaine
            SET http_statut = %s,
                http_code = %s,
                http_at = %s,
                http_iteration = %s,
                flag_parking = %s,
                flag_relocation = %s,
                domaine_relocation = %s,
                updated_at = %s
            WHERE ID = %s
        """

        # Update SERV_domain_check_http_status
        upsert_serv = """
            INSERT INTO SERV_domain_check_http_status
                (domaine, http_flag, http_flag_date, http_response_code, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                http_flag = VALUES(http_flag),
                http_flag_date = VALUES(http_flag_date),
                http_response_code = VALUES(http_response_code),
                updated_at = VALUES(updated_at)
        """

        domain_data = []
        serv_data = []

        for r in results:
            is_reloc = 1 if r["redirect_url"] else 0
            is_park = 1 if r["is_parking"] else 0

            domain_data.append((
                r["http_statut"], r["http_code"], now, r["iteration"],
                is_park, is_reloc, r["redirect_url"], now, r["id"]
            ))
            serv_data.append((
                r["domaine"], r["http_statut"], now, r["http_code"], now, now
            ))

            # Update stats
            if r["http_statut"] == 1:
                self.stats["online"] += 1
            else:
                self.stats["offline"] += 1
            if r["is_parking"]:
                self.stats["parking"] += 1
            if r["redirect_url"]:
                self.stats["redirects"] += 1

        DatabaseManager.execute_many(update_domain, domain_data, batch_size=500)
        DatabaseManager.execute_many(upsert_serv, serv_data, batch_size=500)

    async def run_batch(self, domains: list) -> list:
        """Check a batch of domains concurrently."""
        connector = aiohttp.TCPConnector(
            limit=self.config["max_concurrent"],
            ttl_dns_cache=300,
            ssl=False,
        )
        headers = {"User-Agent": self.config["user_agent"]}

        async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
            tasks = [self.check_single_domain(session, d) for d in domains]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_results = []
        for r in results:
            if isinstance(r, Exception):
                self.stats["errors"] += 1
                logger.error(f"Batch exception: {r}")
            else:
                valid_results.append(r)

        return valid_results

    def log_stats(self):
        """Log current statistics to APP_crawling_logs."""
        try:
            remaining = DatabaseManager.execute_query(
                "SELECT COUNT(*) as cnt FROM APP_domaine WHERE deleted=0 AND (http_at IS NULL OR http_at < DATE_SUB(NOW(), INTERVAL %s DAY))",
                (self.config["recheck_days"],),
                fetch="one"
            )

            total = DatabaseManager.execute_query(
                "SELECT COUNT(*) as cnt FROM APP_domaine WHERE deleted=0", fetch="one"
            )

            logger.info(
                f"Stats: checked={self.stats['checked']} | online={self.stats['online']} | "
                f"offline={self.stats['offline']} | parking={self.stats['parking']} | "
                f"remaining={remaining['cnt'] if remaining else '?'}"
            )
        except Exception as e:
            logger.error(f"Error logging stats: {e}")

    async def run(self):
        """Main loop - continuously check domains."""
        self.stats["start_time"] = time.time()
        logger.info("=== HTTP Checker started ===")

        cycle = 0
        while not shutdown_event.is_set():
            domains = self.get_domains_to_check()

            if not domains:
                logger.info("No domains to check, sleeping 60s...")
                try:
                    await asyncio.wait_for(shutdown_event.wait(), timeout=60)
                except asyncio.TimeoutError:
                    pass
                continue

            cycle += 1
            logger.info(f"Cycle {cycle}: checking {len(domains)} domains...")

            results = await self.run_batch(domains)
            self.stats["checked"] += len(results)

            if results:
                self.update_results(results)

            if cycle % 10 == 0:
                self.log_stats()

            # Small delay between batches
            await asyncio.sleep(self.config["delay_between_batches"])

        logger.info(f"=== HTTP Checker stopped. Total checked: {self.stats['checked']} ===")


def main():
    checker = HTTPChecker()
    asyncio.run(checker.run())


if __name__ == "__main__":
    main()
