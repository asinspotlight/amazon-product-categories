"""
Amazon Product Categories — Complete Tree

Discovers all Amazon product categories by recursively crawling
Best Sellers pages via the ASINSpotlight Scraping API.

Supports multiple marketplaces (US, UK, CA, DE, etc.) via the
MARKETPLACE environment variable or --marketplace CLI flag.
"""

import argparse
import csv
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin

import httpx
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["ASINSPOTLIGHT_API_KEY"]
API_URL = os.environ.get("ASINSPOTLIGHT_API_URL", "https://api.asinspotlight.com")

MARKETPLACES = {
    "us": "www.amazon.com",
    "uk": "www.amazon.co.uk",
    "ca": "www.amazon.ca",
    "de": "www.amazon.de",
    "fr": "www.amazon.fr",
    "it": "www.amazon.it",
    "es": "www.amazon.es",
    "jp": "www.amazon.co.jp",
    "au": "www.amazon.com.au",
    "in": "www.amazon.in",
    "mx": "www.amazon.com.mx",
    "br": "www.amazon.com.br",
    "nl": "www.amazon.nl",
    "se": "www.amazon.se",
    "pl": "www.amazon.pl",
    "be": "www.amazon.com.be",
    "sg": "www.amazon.sg",
    "sa": "www.amazon.sa",
    "ae": "www.amazon.ae",
    "tr": "www.amazon.com.tr",
}

# Marketplaces where the Best Sellers landing page differs from the default
SEED_PATHS = {
    "de": "/gp/bestsellers",
}

MARKETPLACE = os.environ.get("MARKETPLACE", "us").lower()
DOMAIN = MARKETPLACES[MARKETPLACE]
SEED_URL = f"https://{DOMAIN}{SEED_PATHS.get(MARKETPLACE, '/Best-Sellers/zgbs')}"
OUTPUT_DIR = Path("output")
STATE_FILE = OUTPUT_DIR / f"categories_{MARKETPLACE}.csv"
WORKERS = int(os.environ.get("CRAWL_WORKERS", "5"))

CSV_FIELDS = ["category_id", "category_name", "parent_path", "url", "status"]


def load_state() -> dict[str, dict]:
    """Load crawl state from CSV. Returns {url: row_dict}."""
    if not STATE_FILE.exists():
        return {}
    with open(STATE_FILE) as f:
        return {row["url"]: row for row in csv.DictReader(f)}


def save_state(state: dict[str, dict]) -> None:
    """Persist crawl state to CSV."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    with open(STATE_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(state.values())


def extract_category_id(url: str) -> str:
    """Extract category identifier from a Best Sellers URL path."""
    # e.g. /zgbs/automotive/ref=... -> automotive
    # e.g. /zgbs/electronics/1266092011/ref=... -> 1266092011
    parts = [p for p in url.rstrip("/").split("/") if not p.startswith("ref=")]
    return parts[-1] if parts else ""


def fetch_and_parse(url: str) -> dict:
    """Send a URL to the ASINSpotlight API and return parsed result."""
    resp = httpx.post(
        f"{API_URL}/v1/scrape",
        json={"url": url, "marketplace": MARKETPLACE},
        headers={"X-Api-Key": API_KEY},
        timeout=90,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    body = resp.json()

    if not body.get("success"):
        error = body.get("error", {})
        code = error.get("code", "UNKNOWN")
        if code == "PAGE_NOT_FOUND":
            return {"category_name": "", "departments": [], "sub_departments": []}
        raise RuntimeError(f"{code}: {error.get('message', 'Request failed')}")

    data = body["data"]
    return {
        "category_name": clean_category_name(data.get("title", "")),
        "departments": data.get("departments", []),
        "sub_departments": data.get("sub_departments", []),
    }


def clean_category_name(title: str) -> str:
    """Strip Amazon boilerplate from page titles."""
    # "Amazon Best Sellers: Best Electronics" -> "Electronics"
    # "Amazon.com Best Sellers: The most popular items on Amazon" -> ""
    cleaned = re.sub(
        r"^Amazon(?:\.[a-z.]+)? Best Sellers:?\s*(?:Best\s+|The most popular items (?:on Amazon|in ))?",
        "", title,
    ).strip()
    cleaned = re.sub(r"^The most popular items in ", "", cleaned)
    return cleaned


def normalize_url(url: str) -> str:
    """Strip ref= tracking segments from Amazon URLs for deduplication."""
    absolute = urljoin(f"https://{DOMAIN}", url)
    parts = absolute.split("/")
    return "/".join(p for p in parts if not p.startswith("ref="))


def enqueue(state: dict, url: str, parent_path: str = "") -> None:
    """Add a URL to state if not already tracked."""
    absolute = normalize_url(url)
    if absolute not in state:
        state[absolute] = {
            "category_id": "",
            "category_name": "",
            "parent_path": parent_path,
            "url": absolute,
            "status": "pending",
        }


def crawl() -> None:
    """Main crawl loop: resume from state, process pending URLs."""
    state = load_state()
    lock = threading.Lock()
    save_counter = 0

    # Reset any in_progress rows from a previous interrupted run
    for row in state.values():
        if row["status"] == "in_progress":
            row["status"] = "pending"

    if not state:
        enqueue(state, SEED_URL)

    def pick_url() -> str | None:
        """Grab the next pending URL and mark it in_progress."""
        for url, row in state.items():
            if row["status"] == "pending":
                row["status"] = "in_progress"
                return url
        return None

    def process(url: str) -> None:
        nonlocal save_counter
        try:
            result = fetch_and_parse(url)
        except Exception as e:
            with lock:
                state[url]["status"] = "pending"
            print(f"  error: {e}")
            return

        with lock:
            row = state[url]
            row["category_id"] = extract_category_id(url)
            row["category_name"] = result.get("category_name", row["category_id"])
            row["status"] = "done"

            parent = row["parent_path"]
            child_prefix = f"{parent} > {row['category_name']}" if parent else row["category_name"]

            for dept in result.get("departments", []) + result.get("sub_departments", []):
                enqueue(state, dept["link"], child_prefix)

            save_counter += 1
            if save_counter % WORKERS == 0:
                save_state(state)

    done = sum(1 for r in state.values() if r["status"] == "done")
    total = len(state)
    print(f"Resuming: {done} done, {total - done} remaining, {WORKERS} workers")

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        while True:
            with lock:
                urls = []
                for _ in range(WORKERS):
                    url = pick_url()
                    if url:
                        urls.append(url)

            if not urls:
                break

            done = sum(1 for r in state.values() if r["status"] == "done")
            print(f"[{done}/{len(state)}] dispatching {len(urls)} URLs")

            futures = [pool.submit(process, url) for url in urls]
            for f in as_completed(futures):
                f.result()

        with lock:
            save_state(state)

    done = sum(1 for r in state.values() if r["status"] == "done")
    print(f"Done. {done} categories crawled, {len(state)} total URLs.")


def main():
    global MARKETPLACE, DOMAIN, SEED_URL, STATE_FILE

    parser = argparse.ArgumentParser(description="Crawl Amazon product categories")
    parser.add_argument(
        "-m", "--marketplace",
        choices=sorted(MARKETPLACES),
        default=MARKETPLACE,
        help=f"Amazon marketplace to crawl (default: {MARKETPLACE})",
    )
    args = parser.parse_args()

    MARKETPLACE = args.marketplace
    DOMAIN = MARKETPLACES[MARKETPLACE]
    SEED_URL = f"https://{DOMAIN}{SEED_PATHS.get(MARKETPLACE, '/Best-Sellers/zgbs')}"
    STATE_FILE = OUTPUT_DIR / f"categories_{MARKETPLACE}.csv"

    print(f"Marketplace: {MARKETPLACE} ({DOMAIN})")
    crawl()


if __name__ == "__main__":
    main()
