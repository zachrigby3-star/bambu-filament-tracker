import requests
import duckdb
from datetime import datetime, timedelta, timezone

BASE_URL = "https://db-public.bbltracker.com"
REGION = "us"
PRODUCT_NAME = "PLA Basic"
OUTPUT_FILE = f"bambu_pla_basic_{REGION}.csv"


def candidate_filenames():
    now = datetime.now(timezone.utc)
    block_hour = (now.hour // 6) * 6
    current_block = now.replace(hour=block_hour, minute=0, second=0, microsecond=0)

    candidates = []
    for i in range(0, 20):
        dt = current_block - timedelta(hours=6 * i)
        candidates.append(dt.strftime("%Y-%m-%d-%H%M.parquet"))
    return candidates


def find_latest_available_parquet():
    for filename in candidate_filenames():
        url = f"{BASE_URL}/{filename}"
        try:
            resp = requests.head(url, timeout=20, allow_redirects=True)
            if resp.status_code == 200:
                return url, filename
        except requests.RequestException:
            pass
    raise RuntimeError("Could not find a reachable parquet file.")


def main():
    parquet_url, parquet_name = find_latest_available_parquet()
    print(f"Using parquet file: {parquet_name}")

    query = f"""
    WITH latest_rows AS (
        SELECT
            timestamp,
            region,
            product_name,
            variant_name,
            stock,
            eta,
            max_quantity,
            is_flash_sale,
            ROW_NUMBER() OVER (
                PARTITION BY region, product_name, variant_name
                ORDER BY timestamp DESC
            ) AS rn
        FROM read_parquet('{parquet_url}')
        WHERE lower(region) = lower('{REGION}')
          AND product_name = '{PRODUCT_NAME}'
    )
    SELECT
        lower(region) AS region,
        variant_name,
        stock,
        eta,
        timestamp,
        CASE
            WHEN stock = 0 THEN 'Out of Stock'
            WHEN stock < 10 THEN 'Low Stock'
            ELSE 'In Stock'
        END AS status,
        max_quantity,
        is_flash_sale
    FROM latest_rows
    WHERE rn = 1
    ORDER BY variant_name;
    """

    con = duckdb.connect()
    df = con.execute(query).df()

    if df.empty:
        raise RuntimeError("Query returned no rows.")

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Wrote {len(df)} rows to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
