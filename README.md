# Amazon Product Categories — Complete Tree

Built with Python using the [ASINSpotlight Scraping API](https://www.asinspotlight.com/api).

Discovers all Amazon US product categories by recursively crawling the Best Sellers tree. The entire scraper is ~200 lines of Python — the API does the heavy lifting.

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

Output is saved to `output/categories.csv`. The crawl is resumable — restart and it picks up where it left off.

## License

MIT
