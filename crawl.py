"""
Amazon Product Categories - Best Sellers tree crawler.

The crawler follows Amazon Best Sellers category navigation and writes a CSV of
category placements. A placement is one occurrence of an Amazon category in the
tree. This matters because Amazon can expose the same category_id under multiple
parents, and a valid hierarchy must preserve those separate branches.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

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

# Marketplaces where the Best Sellers landing page differs from the default.
SEED_PATHS = {
    "de": "/gp/bestsellers",
}

MARKETPLACE = os.environ.get("MARKETPLACE", "us").lower()
DOMAIN = MARKETPLACES[MARKETPLACE]
SEED_URL = f"https://{DOMAIN}{SEED_PATHS.get(MARKETPLACE, '/Best-Sellers/zgbs')}"
OUTPUT_DIR = Path("output")
STATE_FILE = OUTPUT_DIR / f"categories_{MARKETPLACE}.csv"
WORKERS = int(os.environ.get("CRAWL_WORKERS", "5"))

ROOT_PARENT_ID = ""
ROOT_PLACEMENT_ID = "root"
PATH_SEP = " > "

CSV_FIELDS = [
    "placement_id",
    "parent_placement_id",
    "category_id",
    "category_name",
    "parent_category_id",
    "depth",
    "category_path",
    "category_name_path",
    "url",
    "status",
]

BEST_SELLERS_MARKERS = ("zgbs", "bestsellers")
NAV_LEVEL_RE = re.compile(r"zg_bs_nav_[^_]+_(\d+)(?:_|$)")
UUID_SEGMENT_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(_\d+)?$")


def load_state() -> dict[str, dict]:
    """Load placement crawl state from CSV. Returns {placement_id: row_dict}."""
    if not STATE_FILE.exists():
        return {}

    with open(STATE_FILE, newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return {}

        missing_fields = set(CSV_FIELDS) - set(reader.fieldnames)
        if missing_fields:
            raise RuntimeError(
                f"{STATE_FILE} uses the old flat schema and cannot be resumed as "
                "a valid placement tree. Move it aside or pass --fresh to replace it."
            )

        rows = [{field: row.get(field, "") for field in CSV_FIELDS} for row in reader]
    return {row["placement_id"]: row for row in rows}


def save_state(state: dict[str, dict]) -> None:
    """Persist crawl state to CSV."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    rows = sorted(state.values(), key=lambda row: (int(row["depth"]), row["category_name_path"], row["placement_id"]))
    with open(STATE_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def normalize_url(url: str) -> str:
    """Build an absolute Amazon URL and remove tracking-only parts."""
    absolute = urljoin(f"https://{DOMAIN}", url)
    parsed = urlsplit(absolute)
    path_parts = [part for part in parsed.path.split("/") if part and not part.startswith("ref=")]
    normalized_path = "/" + "/".join(path_parts)
    return urlunsplit((parsed.scheme, parsed.netloc, normalized_path, "", ""))


def best_sellers_tail(url: str) -> list[str]:
    """Return path segments after /zgbs/ or /gp/bestsellers/."""
    path_parts = [part for part in urlsplit(normalize_url(url)).path.split("/") if part]
    for marker in BEST_SELLERS_MARKERS:
        if marker in path_parts:
            return path_parts[path_parts.index(marker) + 1 :]
    return []


def extract_category_id(url: str) -> str:
    """Extract the Amazon category identifier for a Best Sellers URL."""
    tail = best_sellers_tail(url)
    if tail:
        return tail[-1]

    path_parts = [part for part in urlsplit(normalize_url(url)).path.split("/") if part]
    return path_parts[-1] if path_parts else ""


def extract_top_level_slug(url: str) -> str:
    """Return the first category segment below Best Sellers, if present."""
    tail = best_sellers_tail(url)
    return tail[0] if tail else ""


def placement_id_for(category_path: str) -> str:
    """Create a stable ID for one category occurrence in the hierarchy."""
    digest = hashlib.sha1(category_path.encode("utf-8")).hexdigest()[:16]
    return f"pl_{digest}"


def split_path(path: str) -> list[str]:
    return [part for part in path.split(PATH_SEP) if part]


def has_corrupted_name(name: str) -> bool:
    """Detect Amazon merchandising IDs that sometimes leak into sidebars."""
    return bool(UUID_SEGMENT_RE.match(name.strip()))


def clean_category_name(title: str) -> str:
    """Strip Amazon boilerplate from page titles."""
    cleaned = re.sub(
        r"^Amazon(?:\.[a-z.]+)? Best Sellers:?\s*(?:Best\s+|The most popular items (?:on Amazon|in ))?",
        "",
        title,
    ).strip()
    cleaned = re.sub(r"^The most popular items in ", "", cleaned)
    return cleaned


def fetch_and_parse(url: str) -> dict:
    """Send a URL to the ASINSpotlight API and return parsed category data."""
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


def nav_level(link: str) -> int | None:
    """Extract Amazon sidebar navigation depth from a zg_bs_nav ref marker."""
    match = NAV_LEVEL_RE.search(link)
    return int(match.group(1)) if match else None


def is_ancestor_link(link: str) -> bool:
    return "zg_bs_unv_" in link


def is_nav_link(link: str) -> bool:
    return "zg_bs_nav_" in link


def valid_department(item: dict, valid_slugs: set[str]) -> bool:
    """Filter out links that do not belong to the seed Best Sellers tree."""
    link = item.get("link", "")
    name = item.get("name", "").strip()
    if not link or not name or has_corrupted_name(name):
        return False

    slug = extract_top_level_slug(link)
    return not valid_slugs or not slug or slug in valid_slugs


def breadcrumb_ancestors(departments: list[dict], valid_slugs: set[str]) -> list[dict]:
    """Return the valid Amazon sidebar breadcrumb ancestors for the current page."""
    return [
        item
        for item in departments
        if is_ancestor_link(item.get("link", "")) and valid_department(item, valid_slugs)
    ]


def child_departments(row: dict, departments: list[dict], valid_slugs: set[str]) -> list[dict]:
    """Select only true child category links from Amazon's sidebar.

    Amazon uses the same departments collection for children and leaf siblings.
    The ref marker depth distinguishes them: true children are one level deeper
    than the breadcrumb ancestor count. On the seed page, top-level categories
    are exposed as level 0 nav links.
    """
    if row["placement_id"] == ROOT_PLACEMENT_ID:
        expected_level = 0
    else:
        raw_ancestors = [item for item in departments if is_ancestor_link(item.get("link", ""))]
        valid_ancestors = breadcrumb_ancestors(departments, valid_slugs)
        if len(raw_ancestors) != len(valid_ancestors):
            return []
        expected_level = len(valid_ancestors) + 1

    children = []
    seen_ids: set[str] = set()
    for item in departments:
        link = item.get("link", "")
        if not is_nav_link(link) or not valid_department(item, valid_slugs):
            continue
        if nav_level(link) != expected_level:
            continue

        category_id = extract_category_id(link)
        if not category_id or category_id in seen_ids:
            continue
        seen_ids.add(category_id)
        children.append(item)
    return children


def explicit_sub_departments(items: list[dict], valid_slugs: set[str], seen_ids: set[str]) -> list[dict]:
    """Return API-provided subcategory links that were not already selected."""
    children = []
    for item in items:
        if not valid_department(item, valid_slugs):
            continue

        category_id = extract_category_id(item["link"])
        if not category_id or category_id in seen_ids:
            continue
        seen_ids.add(category_id)
        children.append(item)
    return children


def new_root_row() -> dict:
    category_id = extract_category_id(SEED_URL)
    return {
        "placement_id": ROOT_PLACEMENT_ID,
        "parent_placement_id": ROOT_PARENT_ID,
        "category_id": category_id,
        "category_name": "Best Sellers",
        "parent_category_id": "",
        "depth": "0",
        "category_path": category_id,
        "category_name_path": "",
        "url": normalize_url(SEED_URL),
        "status": "pending",
    }


def enqueue_child(state: dict[str, dict], parent: dict, item: dict) -> dict | None:
    """Add one child placement under the given parent."""
    url = normalize_url(item["link"])
    category_id = extract_category_id(url)
    name = item.get("name", "").strip()
    if not category_id or not name:
        return None

    parent_path_ids = split_path(parent["category_path"])
    if category_id in parent_path_ids:
        return None

    category_path = PATH_SEP.join(parent_path_ids + [category_id])
    placement_id = placement_id_for(category_path)
    if placement_id in state:
        return state[placement_id]

    parent_name_path = split_path(parent["category_name_path"])
    row = {
        "placement_id": placement_id,
        "parent_placement_id": parent["placement_id"],
        "category_id": category_id,
        "category_name": name,
        "parent_category_id": parent["category_id"],
        "depth": str(int(parent["depth"]) + 1),
        "category_path": category_path,
        "category_name_path": PATH_SEP.join(parent_name_path + [name]),
        "url": url,
        "status": "pending",
    }
    state[placement_id] = row
    return row


def learn_seed_slugs(row: dict, departments: list[dict], valid_slugs: set[str]) -> None:
    """Populate the top-level Best Sellers slug whitelist from the seed page."""
    if row["placement_id"] != ROOT_PLACEMENT_ID or valid_slugs:
        return

    for item in departments:
        if not is_nav_link(item.get("link", "")):
            continue
        slug = extract_top_level_slug(item["link"])
        if slug:
            valid_slugs.add(slug)


def infer_seed_slugs(state: dict[str, dict]) -> set[str]:
    """Recover the slug whitelist when resuming an existing placement crawl."""
    slugs = set()
    for row in state.values():
        if row.get("parent_placement_id") == ROOT_PLACEMENT_ID:
            slug = extract_top_level_slug(row["url"])
            if slug:
                slugs.add(slug)
    return slugs


def validate_tree(state: dict[str, dict]) -> list[str]:
    """Return structural errors that would make the CSV an invalid tree."""
    errors = []
    roots = [row for row in state.values() if not row["parent_placement_id"]]
    if len(roots) != 1 or roots[0]["placement_id"] != ROOT_PLACEMENT_ID:
        errors.append(f"expected exactly one root placement, found {len(roots)}")

    for row in state.values():
        placement_id = row["placement_id"]
        parent_id = row["parent_placement_id"]
        if placement_id != ROOT_PLACEMENT_ID and parent_id not in state:
            errors.append(f"{placement_id} has missing parent placement {parent_id}")
            continue

        path_ids = split_path(row["category_path"])
        if len(path_ids) != int(row["depth"]) + 1:
            errors.append(f"{placement_id} has depth {row['depth']} but path {row['category_path']}")
        if len(path_ids) != len(set(path_ids)):
            errors.append(f"{placement_id} repeats a category in its ancestor path")

        if placement_id == ROOT_PLACEMENT_ID:
            continue

        parent = state[parent_id]
        if not row["category_path"].startswith(parent["category_path"] + PATH_SEP):
            errors.append(f"{placement_id} path is not below parent {parent_id}")
        if row["parent_category_id"] != parent["category_id"]:
            errors.append(f"{placement_id} parent_category_id does not match parent row")

    return errors


def crawl(fresh: bool = False, max_placements: int | None = None) -> None:
    """Main crawl loop: resume from placement state, process pending URLs."""
    state = {} if fresh else load_state()
    lock = threading.Lock()
    save_counter = 0

    if not state:
        state[ROOT_PLACEMENT_ID] = new_root_row()

    for row in state.values():
        if row["status"] == "in_progress":
            row["status"] = "pending"

    valid_slugs = infer_seed_slugs(state)

    def pick_row() -> dict | None:
        if max_placements is not None:
            active_count = sum(1 for row in state.values() if row["status"] in {"done", "in_progress"})
            if active_count >= max_placements:
                return None

        for row in state.values():
            if row["status"] == "pending":
                row["status"] = "in_progress"
                return row
        return None

    def process(placement_id: str) -> None:
        nonlocal save_counter
        with lock:
            row = state[placement_id]
            url = row["url"]

        try:
            result = fetch_and_parse(url)
        except Exception as e:
            with lock:
                state[placement_id]["status"] = "pending"
            print(f"  error: {url}: {e}")
            return

        with lock:
            row = state[placement_id]
            category_name = result.get("category_name") or row["category_name"] or row["category_id"]
            row["category_name"] = category_name
            if placement_id == ROOT_PLACEMENT_ID:
                row["category_name_path"] = ""
            elif not row["category_name_path"]:
                parent = state[row["parent_placement_id"]]
                row["category_name_path"] = PATH_SEP.join(split_path(parent["category_name_path"]) + [category_name])

            departments = result.get("departments", [])
            learn_seed_slugs(row, departments, valid_slugs)

            children = child_departments(row, departments, valid_slugs)
            seen_child_ids = {extract_category_id(item["link"]) for item in children}
            children.extend(explicit_sub_departments(result.get("sub_departments", []), valid_slugs, seen_child_ids))

            for item in children:
                enqueue_child(state, row, item)

            row["status"] = "done"
            save_counter += 1
            if save_counter % WORKERS == 0:
                save_state(state)

    done = sum(1 for row in state.values() if row["status"] == "done")
    total = len(state)
    print(f"Resuming: {done} done, {total - done} remaining, {WORKERS} workers")

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        while True:
            with lock:
                rows = []
                for _ in range(WORKERS):
                    row = pick_row()
                    if row:
                        rows.append(row)

            if not rows:
                break

            done = sum(1 for row in state.values() if row["status"] == "done")
            print(f"[{done}/{len(state)}] dispatching {len(rows)} placements")

            futures = [pool.submit(process, row["placement_id"]) for row in rows]
            for future in as_completed(futures):
                future.result()

        with lock:
            errors = validate_tree(state)
            if errors:
                raise RuntimeError("Invalid tree:\n" + "\n".join(f"  - {error}" for error in errors[:20]))
            save_state(state)

    done = sum(1 for row in state.values() if row["status"] == "done")
    print(f"Done. {done} placements crawled, {len(state)} total placements.")


def main() -> None:
    global MARKETPLACE, DOMAIN, SEED_URL, STATE_FILE

    parser = argparse.ArgumentParser(description="Crawl Amazon Best Sellers category placements")
    parser.add_argument(
        "-m",
        "--marketplace",
        choices=sorted(MARKETPLACES),
        default=MARKETPLACE,
        help=f"Amazon marketplace to crawl (default: {MARKETPLACE})",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Start a new placement crawl and replace any existing output for the marketplace.",
    )
    parser.add_argument(
        "--max-placements",
        type=int,
        default=None,
        help="Stop after this many placements are crawled. Useful for smoke checks.",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=None,
        help="Write crawl state to this CSV path instead of output/categories_{marketplace}.csv.",
    )
    args = parser.parse_args()

    MARKETPLACE = args.marketplace
    DOMAIN = MARKETPLACES[MARKETPLACE]
    SEED_URL = f"https://{DOMAIN}{SEED_PATHS.get(MARKETPLACE, '/Best-Sellers/zgbs')}"
    STATE_FILE = OUTPUT_DIR / f"categories_{MARKETPLACE}.csv"
    if args.output_file:
        STATE_FILE = args.output_file

    print(f"Marketplace: {MARKETPLACE} ({DOMAIN})")
    crawl(fresh=args.fresh, max_placements=args.max_placements)


if __name__ == "__main__":
    main()
