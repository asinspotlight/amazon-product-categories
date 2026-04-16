# Amazon Product Categories — Complete Tree

Built with Python using the [ASINSpotlight Scraping API](https://www.asinspotlight.com/api).

Discovers all Amazon product categories by recursively crawling the Best Sellers tree. Supports 20 marketplaces (US, UK, CA, DE, FR, JP, and more). The entire scraper is a single Python file — the API does the heavy lifting.

## Download

- **Latest CSV** — [Download](https://www.asinspotlight.com/amz-categories-list-csv)
- **Browse online** — [ASINSpotlight Categories Browser](https://asinspotlight.com/amazon-categories-browser)

## How it works

The tool starts at Amazon's top-level Best Sellers page and recursively follows every category link. For each page, the ASINSpotlight Scraping API handles fetching, proxy rotation, and HTML parsing — returning structured JSON with category names and links. The crawler just manages the queue and writes results to CSV.

## ASINSpotlight Scraping API

The API parses any Amazon page into structured JSON. It handles proxies, rate limiting, and anti-bot measures so you don't have to.

[Get API access →](https://www.asinspotlight.com/api)

## Quick start

```bash
git clone https://github.com/asinspotlight/amazon-product-categories.git
cd amazon-product-categories

python -m venv .venv
source .venv/bin/activate
pip install .

cp .env.example .env
# Add your API key to .env

python crawl.py
```

### Other marketplaces

```bash
python crawl.py --marketplace uk
python crawl.py -m de
```

Supported: `us`, `uk`, `ca`, `de`, `fr`, `it`, `es`, `jp`, `au`, `in`, `mx`, `br`, `nl`, `se`, `pl`, `be`, `sg`, `sa`, `ae`, `tr`.

You can also set the `MARKETPLACE` env var instead of the flag.

Output is saved to `output/categories_{marketplace}.csv` (e.g. `categories_uk.csv`). Each marketplace has its own state file, so crawls are independent and resumable.

## License

MIT
