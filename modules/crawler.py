"""
Module 2 - Web Crawler (Playwright)
Crawle les domaines actifs, extrait emails, téléphones, adresses,
réseaux sociaux et mots-clés depuis les pages HTML.
"""
import asyncio
import re
import signal
import time
import random
from datetime import datetime
from urllib.parse import urlparse, urljoin
from collections import Counter

from config.settings import CRAWLER, EXTRACT_PATTERNS, SOCIAL_PATTERNS, KEYWORDS
from utils.database import DatabaseManager
from utils.logger import setup_logger

logger = setup_logger("crawler", "crawler.log")

shutdown_event = asyncio.Event()


def signal_handler(sig, frame):
    logger.info("Shutdown signal received...")
    shutdown_event.set()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class StopWordsManager:
    """Manages French stop words for keyword filtering."""
    _instance = None
    _stop_words = set()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
            cls._load_stop_words()
        return cls._instance

    @classmethod
    def _load_stop_words(cls):
        try:
            with open(KEYWORDS["stop_words_file"], "r", encoding="utf-8") as f:
                cls._stop_words = {line.strip().lower() for line in f if line.strip()}
            logger.info(f"Loaded {len(cls._stop_words)} stop words")
        except FileNotFoundError:
            logger.warning("Stop words file not found, using empty set")
            cls._stop_words = set()

    @classmethod
    def is_stop_word(cls, word: str) -> bool:
        return word.lower() in cls._stop_words


def french_soundex(word: str) -> str:
    """Simple French soundex implementation."""
    if not word:
        return ""
    word = word.upper().strip()
    if len(word) < 2:
        return word

    # Keep first letter
    result = word[0]

    # French phonetic mappings
    replacements = {
        "B": "1", "P": "1",
        "C": "2", "G": "2", "J": "2", "K": "2", "Q": "2", "S": "2", "X": "2", "Z": "2",
        "D": "3", "T": "3",
        "L": "4",
        "M": "5", "N": "5",
        "R": "6",
        "F": "7", "V": "7", "W": "7",
    }

    prev = replacements.get(word[0], "0")
    for ch in word[1:]:
        code = replacements.get(ch, "0")
        if code != "0" and code != prev:
            result += code
        prev = code
        if len(result) >= 4:
            break

    return result.ljust(4, "0")[:4]


class DataExtractor:
    """Extracts structured data from HTML content."""

    EMAIL_RE = re.compile(EXTRACT_PATTERNS["email"], re.IGNORECASE)
    PHONE_RE = re.compile(EXTRACT_PATTERNS["phone_fr"])
    SIRET_RE = re.compile(EXTRACT_PATTERNS["siret"])
    SIREN_RE = re.compile(EXTRACT_PATTERNS["siren"])
    POSTAL_RE = re.compile(EXTRACT_PATTERNS["postal_code_fr"])

    # Patterns to ignore in emails
    EMAIL_BLACKLIST = {
        "example.com", "domain.com", "email.com", "test.com",
        "sentry.io", "wixpress.com", "gravatar.com",
    }

    @classmethod
    def extract_emails(cls, text: str, domain: str) -> list:
        """Extract valid email addresses."""
        emails = set()
        for match in cls.EMAIL_RE.finditer(text):
            email = match.group(0).lower().strip(".")
            email_domain = email.split("@")[1] if "@" in email else ""
            if (
                email_domain not in cls.EMAIL_BLACKLIST
                and not email.endswith((".png", ".jpg", ".gif", ".svg", ".css", ".js"))
                and len(email) < 200
            ):
                emails.add(email)
        return list(emails)

    @classmethod
    def extract_phones(cls, text: str) -> list:
        """Extract French phone numbers."""
        phones = set()
        for match in cls.PHONE_RE.finditer(text):
            phone = re.sub(r"[\s.\-]", "", match.group(0))
            if phone.startswith("+33"):
                phone = "0" + phone[3:]
            elif phone.startswith("0033"):
                phone = "0" + phone[4:]
            if len(phone) == 10 and phone.startswith("0"):
                phones.add(phone)
        return list(phones)

    @classmethod
    def extract_social_networks(cls, text: str, base_domain: str) -> list:
        """Extract social network URLs."""
        results = []
        for type_id, config in SOCIAL_PATTERNS.items():
            for pattern in config["patterns"]:
                for match in re.finditer(pattern, text, re.IGNORECASE):
                    url = match.group(0)
                    if base_domain not in url.lower():
                        # Ensure full URL
                        if not url.startswith("http"):
                            url = "https://" + url
                        results.append({"type": type_id, "url": url})
        return results

    @classmethod
    def extract_siret_siren(cls, text: str) -> dict:
        """Extract SIRET and SIREN numbers."""
        result = {"siret": set(), "siren": set()}

        # Look for SIRET (14 digits)
        for match in cls.SIRET_RE.finditer(text):
            siret = re.sub(r"\s", "", match.group(0))
            if len(siret) == 14:
                result["siret"].add(siret)
                result["siren"].add(siret[:9])

        # Look for SIREN (9 digits) - only those not part of a SIRET
        for match in cls.SIREN_RE.finditer(text):
            siren = re.sub(r"\s", "", match.group(0))
            if len(siren) == 9 and siren not in {s[:9] for s in result["siret"]}:
                # Avoid false positives (phone numbers, postal codes, etc)
                context = text[max(0, match.start() - 50):match.end() + 50].lower()
                if any(kw in context for kw in ["siren", "siret", "rcs", "immatricul", "registr"]):
                    result["siren"].add(siren)

        return {k: list(v) for k, v in result.items()}

    @classmethod
    def extract_address(cls, text: str) -> list:
        """Extract French addresses (simplified)."""
        addresses = []
        # Pattern: number + street type + name + postal code + city
        addr_pattern = re.compile(
            r"(\d{1,4})\s*,?\s*(rue|avenue|boulevard|allée|impasse|place|chemin|route|passage|cours|quai|"
            r"av\.|bd\.|bl\.|r\.|pl\.)\s+([^,\n]{3,60})\s*,?\s*(\d{5})\s+([A-ZÀ-Ü][a-zà-ü\-\s]{2,40})",
            re.IGNORECASE,
        )
        for match in addr_pattern.finditer(text):
            addresses.append({
                "numero": match.group(1),
                "voie": f"{match.group(2)} {match.group(3)}".strip(),
                "code_postal": match.group(4),
                "ville": match.group(5).strip(),
            })
        return addresses

    @classmethod
    def extract_keywords(cls, text: str) -> list:
        """Extract and count keywords from visible text."""
        stop_mgr = StopWordsManager.get_instance()
        min_len = KEYWORDS["min_length"]

        # Clean text: remove HTML tags, scripts, styles
        clean = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.DOTALL | re.IGNORECASE)
        clean = re.sub(r"<style[^>]*>.*?</style>", " ", clean, flags=re.DOTALL | re.IGNORECASE)
        clean = re.sub(r"<[^>]+>", " ", clean)
        clean = re.sub(r"&\w+;", " ", clean)
        clean = re.sub(r"[^\w\sàâäéèêëïîôùûüÿçœæ\-]", " ", clean, flags=re.UNICODE)

        words = clean.lower().split()
        # Filter
        filtered = [
            w for w in words
            if len(w) >= min_len
            and not w.isdigit()
            and not stop_mgr.is_stop_word(w)
            and not re.match(r"^[\d\-_.]+$", w)
        ]

        # Count occurrences
        counter = Counter(filtered)

        # Return top keywords with soundex
        keywords = []
        for word, count in counter.most_common(500):
            sdx = french_soundex(word) if KEYWORDS["use_soundex"] else ""
            keywords.append({
                "mot_cle": word[:100],
                "soundex": sdx,
                "repetition": count,
            })
        return keywords


class WebCrawler:
    """Main crawler using Playwright."""

    def __init__(self):
        self.config = CRAWLER
        self.extractor = DataExtractor()
        self.stats = {
            "crawled": 0,
            "data_collected": 0,
            "errors": 0,
            "start_time": None,
        }

    def get_domains_to_crawl(self, batch_size: int = None) -> list:
        """Get domains ready for crawling."""
        size = batch_size or self.config["batch_size"]
        query = """
            SELECT d.ID, d.domaine, d.url_full, d.crawled_iteration
            FROM APP_domaine d
            WHERE d.deleted = 0
              AND d.http_statut = 1
              AND d.flag_parking = 0
              AND d.flag_crawling = 0
              AND (d.flag_data_collected = 0 OR d.flag_data_collected IS NULL)
                AND (d.crawled_iteration IS NULL OR d.crawled_iteration < 3)
            ORDER BY d.crawled_at ASC, d.ID ASC
            LIMIT %s
        """
        return DatabaseManager.execute_query(query, (size,))

    def mark_crawling(self, domain_ids: list, flag: int = 1):
        """Mark domains as currently being crawled."""
        if not domain_ids:
            return
        placeholders = ",".join(["%s"] * len(domain_ids))
        query = f"UPDATE APP_domaine SET flag_crawling = %s, updated_at = NOW() WHERE ID IN ({placeholders})"
        DatabaseManager.execute_query(query, (flag, *domain_ids), fetch="none")

    async def crawl_domain(self, browser, domain: dict) -> dict:
        """Crawl a single domain and extract all data."""
        domain_name = domain["domaine"]
        domain_id = domain["ID"]
        base_url = domain.get("url_full") or f"https://{domain_name}"
        if not base_url.startswith("http"):
            base_url = f"https://{base_url}"

        result = {
            "id": domain_id,
            "domaine": domain_name,
            "emails": [],
            "phones": [],
            "addresses": [],
            "social": [],
            "keywords": [],
            "siret": [],
            "siren": [],
            "success": False,
            "description": "",
        }

        page = None
        try:
            page = await browser.new_page()
            await page.set_extra_http_headers({"User-Agent": self.config["user_agent"]})

            # Visit homepage
            await page.goto(base_url, wait_until="domcontentloaded",
                            timeout=self.config["navigation_timeout"])
            await page.wait_for_timeout(1000)

            # Get homepage content
            html = await page.content()
            text = await page.evaluate("() => document.body ? document.body.innerText : ''")

            # Extract description (meta description or first meaningful text)
            desc = await page.evaluate("""() => {
                const meta = document.querySelector('meta[name="description"]');
                return meta ? meta.getAttribute('content') : '';
            }""")
            result["description"] = (desc or "")[:500]

            # Collect all data from homepage
            self._extract_from_content(result, html, text, domain_name)

            # Find internal links
            links = await self._get_internal_links(page, domain_name)

            # Visit contact page first if found
            contact_links = [l for l in links if self._is_contact_page(l)]
            other_links = [l for l in links if l not in contact_links]

            # Prioritize contact pages, then other pages
            pages_to_visit = contact_links + other_links
            pages_to_visit = pages_to_visit[:self.config["max_pages_per_domain"] - 1]

            visited = {base_url}
            for link in pages_to_visit:
                if shutdown_event.is_set():
                    break
                if link in visited:
                    continue
                visited.add(link)

                try:
                    # Adaptive delay
                    delay = random.uniform(self.config["delay_min"], self.config["delay_max"])
                    await asyncio.sleep(delay)

                    await page.goto(link, wait_until="domcontentloaded",
                                    timeout=self.config["page_timeout"])
                    await page.wait_for_timeout(500)

                    sub_html = await page.content()
                    sub_text = await page.evaluate("() => document.body ? document.body.innerText : ''")
                    self._extract_from_content(result, sub_html, sub_text, domain_name)
                except Exception as e:
                    logger.debug(f"Error visiting {link}: {e}")

            result["success"] = True

        except Exception as e:
            logger.debug(f"Error crawling {domain_name}: {e}")
            self.stats["errors"] += 1
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass

        return result

    def _extract_from_content(self, result: dict, html: str, text: str, domain: str):
        """Extract all data types from page content."""
        full_content = html + " " + text

        # Emails
        new_emails = self.extractor.extract_emails(full_content, domain)
        result["emails"].extend(new_emails)

        # Phones
        new_phones = self.extractor.extract_phones(full_content)
        result["phones"].extend(new_phones)

        # Addresses
        new_addrs = self.extractor.extract_address(text)
        result["addresses"].extend(new_addrs)

        # Social networks
        new_social = self.extractor.extract_social_networks(html, domain)
        result["social"].extend(new_social)

        # SIRET/SIREN
        siret_siren = self.extractor.extract_siret_siren(full_content)
        result["siret"].extend(siret_siren["siret"])
        result["siren"].extend(siret_siren["siren"])

        # Keywords (aggregate from text)
        new_kw = self.extractor.extract_keywords(html)
        result["keywords"].extend(new_kw)

    async def _get_internal_links(self, page, domain: str) -> list:
        """Get all internal links from the current page."""
        try:
            links = await page.evaluate("""(domain) => {
                const anchors = document.querySelectorAll('a[href]');
                const links = [];
                for (const a of anchors) {
                    const href = a.href;
                    if (href && href.includes(domain) && !href.includes('#')
                        && !href.match(/\.(pdf|jpg|png|gif|svg|css|js|zip|doc|xlsx?)$/i)) {
                        links.push(href);
                    }
                }
                return [...new Set(links)];
            }""", domain)
            return links[:50]  # Limit to 50 links
        except Exception:
            return []

    def _is_contact_page(self, url: str) -> bool:
        """Check if URL looks like a contact page."""
        url_lower = url.lower()
        return any(p in url_lower for p in self.config["contact_page_patterns"])

    def save_results(self, result: dict):
        """Save crawled data to database."""
        domain_id = result["id"]
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        has_data = False

        # Deduplicate
        emails = list(set(result["emails"]))
        phones = list(set(result["phones"]))
        sirets = list(set(result["siret"]))
        sirens = list(set(result["siren"]))

        # Social: deduplicate by type+url
        social_seen = set()
        social = []
        for s in result["social"]:
            key = (s["type"], s["url"][:200])
            if key not in social_seen:
                social_seen.add(key)
                social.append(s)

        # Keywords: merge counts
        kw_merged = {}
        for kw in result["keywords"]:
            word = kw["mot_cle"]
            if word in kw_merged:
                kw_merged[word]["repetition"] += kw["repetition"]
            else:
                kw_merged[word] = kw

        # Save emails
        if emails:
            has_data = True
            data = [(domain_id, e, now, now) for e in emails[:100]]
            DatabaseManager.execute_many(
                "INSERT IGNORE INTO APP_email (id_domaine, email, created_at, updated_at) VALUES (%s, %s, %s, %s)",
                data
            )

        # Save phones
        if phones:
            has_data = True
            data = [(domain_id, p, now, now) for p in phones[:50]]
            DatabaseManager.execute_many(
                "INSERT IGNORE INTO APP_telephone (id_domaine, numero, created_at, updated_at) VALUES (%s, %s, %s, %s)",
                data
            )

        # Save addresses
        if result["addresses"]:
            has_data = True
            addrs = result["addresses"][:20]
            data = [(domain_id, a["numero"], a["voie"], a["code_postal"], a["ville"], now, now)
                    for a in addrs]
            DatabaseManager.execute_many(
                "INSERT IGNORE INTO APP_adresse (id_domaine, numero, voie, code_postal, ville, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                data
            )

        # Save social networks
        if social:
            has_data = True
            data = [(domain_id, s["url"][:1000], s["type"], now, now) for s in social[:30]]
            DatabaseManager.execute_many(
                "INSERT IGNORE INTO APP_reseau_sociaux (id_domaine, url, type, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
                data
            )

        # Save SIRET
        if sirets:
            has_data = True
            data = [(domain_id, s, s[:9], now, now) for s in sirets[:10]]
            DatabaseManager.execute_many(
                "INSERT IGNORE INTO APP_siret (id_domaine, siret, siren, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
                data
            )

        # Save SIREN
        if sirens:
            has_data = True
            data = [(domain_id, s, now, now) for s in sirens[:10]]
            DatabaseManager.execute_many(
                "INSERT IGNORE INTO APP_siren (id_domaine, siren, created_at, updated_at) VALUES (%s, %s, %s, %s)",
                data
            )

            # Save TVA
            tva_list = result.get("tva", [])
            if tva_list:
                has_data = True
                tva_data = [(domain_id, t, now, now) for t in tva_list[:5]]
                DatabaseManager.execute_many(
                    "INSERT IGNORE INTO APP_tva (id_domaine, tva, created_at, updated_at) VALUES (%s, %s, %s, %s)",
                    tva_data
                )

        # Save keywords (delete old + insert new)
        keywords = list(kw_merged.values())[:500]
        if keywords:
            has_data = True
            # Delete old keywords for this domain
            DatabaseManager.execute_query(
                "DELETE FROM APP_mot_cle WHERE id_domaine = %s", (domain_id,), fetch="none"
            )
            data = [(domain_id, kw["soundex"], kw["repetition"], kw["mot_cle"], now, now)
                    for kw in keywords]
            DatabaseManager.execute_many(
                "INSERT INTO APP_mot_cle (id_domaine, soundex, repetition, mot_cle, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)",
                data
            )

        # Update APP_domaine status
        desc = result.get("description", "")[:500]
        iteration = 1  # Will increment
        DatabaseManager.execute_query(
            """UPDATE APP_domaine
               SET flag_crawling = 0,
                   flag_data_collected = %s,
                   flag_production = %s,
                   crawled_at = %s,
                   crawled_iteration = COALESCE(crawled_iteration, 0) + 1,
                   description = %s,
                   updated_at = %s
               WHERE ID = %s""",
            (1 if has_data else 0, 1 if has_data else 0, now, desc, now, domain_id),
            fetch="none",
        )

        if has_data:
            self.stats["data_collected"] += 1

    async def run(self):
        """Main crawler loop with browser resilience."""
        from playwright.async_api import async_playwright

        self.stats["start_time"] = time.time()
        logger.info("=== Crawler started ===")

        # Initialize stop words
        StopWordsManager.get_instance()

        DOMAIN_TIMEOUT = 120
        BROWSER_MAX_DOMAINS = 50
        browser = None
        pw_context = None
        pw = None

        async def launch_browser():
            nonlocal browser, pw_context, pw
            try:
                if browser:
                    try:
                        await browser.close()
                    except Exception:
                        pass
                if pw_context:
                    try:
                        await pw_context.__aexit__(None, None, None)
                    except Exception:
                        pass
                pw_context = async_playwright()
                pw = await pw_context.__aenter__()
                browser = await pw.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
                          "--disable-extensions", "--disable-background-networking",
                          "--disable-default-apps", "--disable-sync", "--no-first-run"],
                )
                logger.info("Browser launched successfully")
                return True
            except Exception as e:
                logger.error(f"Failed to launch browser: {e}")
                browser = None
                return False

        try:
            if not await launch_browser():
                logger.error("Cannot start browser, exiting")
                return

            cycle = 0
            domains_since_restart = 0

            while not shutdown_event.is_set():
                domains = self.get_domains_to_crawl()

                if not domains:
                    logger.info("No domains to crawl, sleeping 60s...")
                    try:
                        await asyncio.wait_for(shutdown_event.wait(), timeout=60)
                    except asyncio.TimeoutError:
                        pass
                    continue

                cycle += 1
                domain_ids = [d["ID"] for d in domains]
                self.mark_crawling(domain_ids, flag=1)
                logger.info(f"Cycle {cycle}: crawling {len(domains)} domains...")

                for domain in domains:
                    if shutdown_event.is_set():
                        break

                    # Restart browser periodically to prevent memory leaks
                    if domains_since_restart >= BROWSER_MAX_DOMAINS:
                        logger.info(f"Restarting browser after {domains_since_restart} domains...")
                        if not await launch_browser():
                            logger.error("Browser restart failed, retrying...")
                            await asyncio.sleep(5)
                            if not await launch_browser():
                                logger.error("Browser restart failed twice, stopping cycle")
                                break
                        domains_since_restart = 0

                    try:
                        result = await asyncio.wait_for(
                            self.crawl_domain(browser, domain),
                            timeout=DOMAIN_TIMEOUT
                        )
                        self.stats["crawled"] += 1
                        domains_since_restart += 1

                        if result["success"]:
                            self.save_results(result)
                        else:
                            DatabaseManager.execute_query(
                                """UPDATE APP_domaine
                                   SET flag_crawling = 0,
                                       crawled_at = NOW(),
                                       crawled_iteration = COALESCE(crawled_iteration, 0) + 1,
                                       updated_at = NOW()
                                   WHERE ID = %s""",
                                (domain["ID"],), fetch="none",
                            )

                    except asyncio.TimeoutError:
                        logger.warning(f"Timeout ({DOMAIN_TIMEOUT}s) on {domain['domaine']}")
                        self.stats["errors"] += 1
                        self.stats["crawled"] += 1
                        domains_since_restart += 1
                        DatabaseManager.execute_query(
                            """UPDATE APP_domaine
                               SET flag_crawling = 0,
                                   crawled_at = NOW(),
                                   crawled_iteration = COALESCE(crawled_iteration, 0) + 1,
                                   updated_at = NOW()
                               WHERE ID = %s""",
                            (domain["ID"],), fetch="none",
                        )
                        # Check if browser is still alive
                        try:
                            p = await browser.new_page()
                            await p.close()
                        except Exception:
                            logger.warning("Browser dead after timeout, relaunching...")
                            if not await launch_browser():
                                break
                            domains_since_restart = 0

                    except Exception as e:
                        err_str = str(e).lower()
                        self.stats["errors"] += 1
                        self.stats["crawled"] += 1
                        logger.error(f"Fatal error on {domain['domaine']}: {e}")
                        self.mark_crawling([domain["ID"]], flag=0)

                        if "browser" in err_str or "closed" in err_str or "connection" in err_str or "target" in err_str:
                            logger.warning("Browser appears crashed, relaunching...")
                            if not await launch_browser():
                                logger.error("Cannot relaunch browser, stopping")
                                break
                            domains_since_restart = 0

                    if self.stats["crawled"] % 50 == 0:
                        logger.info(
                            f"Progress: crawled={self.stats['crawled']} | "
                            f"data_collected={self.stats['data_collected']} | "
                            f"errors={self.stats['errors']}"
                        )

        finally:
            try:
                if browser:
                    await browser.close()
            except Exception:
                pass
            try:
                if pw_context:
                    await pw_context.__aexit__(None, None, None)
            except Exception:
                pass

        logger.info(f"=== Crawler stopped. Total: {self.stats['crawled']} ===")


def main():
    crawler = WebCrawler()
    asyncio.run(crawler.run())


if __name__ == "__main__":
    main()
