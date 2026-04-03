"""
Amazon Product Categories — Complete Tree

Discovers all Amazon US product categories by recursively crawling
Best Sellers pages via the ASINSpotlight Scraping API.
"""

import csv
import os
from pathlib import Path
from urllib.parse import urljoin

import httpx
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["ASINSPOTLIGHT_API_KEY"]
API_URL = os.environ.get("ASINSPOTLIGHT_API_URL", "https://api.asinspotlight.com")

SEED_URL = "https://www.amazon.com/Best-Sellers/zgbs"
OUTPUT_DIR = Path("output")
STATE_FILE = OUTPUT_DIR / "categories.csv"

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
    # e.g. /zgbs/automotive/ -> automotive
    # e.g. /zgbs/electronics/1266092011 -> 1266092011
    parts = url.rstrip("/").split("/")
    return parts[-1] if parts else ""


def fetch_and_parse(url: str) -> dict:
    """Send a URL to the ASINSpotlight API and return parsed result."""
    # TODO: implement API call — send URL, receive departments + sub_departments
    raise NotImplementedError


def enqueue(state: dict, url: str, parent_path: str = "") -> None:
    """Add a URL to state if not already tracked."""
    absolute = urljoin("https://www.amazon.com", url)
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

    if not state:
        enqueue(state, SEED_URL)

    while pending := [u for u, r in state.items() if r["status"] == "pending"]:
        url = pending[0]
        print(f"[{len(state) - len(pending)}/{len(state)}] {url}")

        try:
            result = fetch_and_parse(url)
        except Exception as e:
            print(f"  error: {e}")
            continue

        row = state[url]
        row["category_id"] = extract_category_id(url)
        row["category_name"] = result.get("category_name", row["category_id"])
        row["status"] = "done"

        parent = row["parent_path"]
        child_prefix = f"{parent} > {row['category_name']}" if parent else row["category_name"]

        for dept in result.get("departments", []) + result.get("sub_departments", []):
            enqueue(state, dept["link"], child_prefix)

        save_state(state)

    print(f"Done. {len(state)} categories discovered.")


def main():
    crawl()


if __name__ == "__main__":
    main()
