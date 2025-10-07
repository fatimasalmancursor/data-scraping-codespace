import os
import time
import random
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://gis.data.census.gov/arcgis/rest/services/Hosted/VT_2022_860_Z2_PY_D1/VectorTileServer/tile"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "Accept": "application/x-protobuf,application/octet-stream,*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://gis.data.census.gov/",
    "Connection": "keep-alive",
}

SAVE_DIR = "zip-tiles"
FAILED_FILE = "failed_tiles.txt"
ACCEPTABLE_CT = ("application/x-protobuf", "application/octet-stream")


def ensure_dir(p):
    os.makedirs(p, exist_ok=True)


def tile_path(z, x, y):
    return os.path.join(SAVE_DIR, str(z), str(x), f"{y}.pbf")


def tile_exists_ok(p):
    return os.path.isfile(p) and os.path.getsize(p) > 0


def make_session(max_retries=3, backoff=0.5, pool=64):
    s = requests.Session()
    r = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        status=max_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods={"GET"},
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    ad = HTTPAdapter(max_retries=r, pool_connections=pool, pool_maxsize=pool)
    s.mount("https://", ad)
    s.mount("http://", ad)
    s.headers.update(HEADERS)
    return s


def is_bad_mime(ct: str) -> bool:
    if not ct:
        return True
    ct = ct.split(";")[0].strip().lower()
    return ct not in ACCEPTABLE_CT


def looks_like_html_or_json(b: bytes) -> bool:
    s = b.lstrip()[:64].lower()
    return s.startswith(b"<!doctype") or s.startswith(b"<html") or s.startswith(b"{")


def log_failure(z, x, y, reason):
    ensure_dir(os.path.dirname(FAILED_FILE) or ".")
    with open(FAILED_FILE, "a") as f:
        f.write(f"{z}/{x}/{y}  {reason}\n")


def _fetch_once(session: requests.Session, url: str, timeout):
    r = session.get(url, timeout=timeout, stream=True)
    if r.status_code != 200:
        return None, f"status:{r.status_code}", None
    r.raw.decode_content = True
    ct = (r.headers.get("Content-Type") or "").lower()
    return r, None, ct


def download_tile(session, z, x, y, timeout=(5, 25), html_retries=3):
    url = f"{BASE_URL}/{z}/{x}/{y}.pbf"
    out_dir = os.path.join(SAVE_DIR, str(z), str(x))
    out_file = tile_path(z, x, y)

    if tile_exists_ok(out_file):
        return "skipped"

    ensure_dir(out_dir)

    last_reason = None
    for attempt in range(html_retries + 1):
        try:
            r, err, ct = _fetch_once(session, url, timeout)
        except Exception as e:
            last_reason = f"request_error:{e}"
            r = None

        if r is None:
            last_reason = last_reason or err or "request_failed"
        else:
            tmp_file = out_file + ".part"
            try:
                it = r.iter_content(chunk_size=65536)
                first_chunk = next(it, b"")
                if not first_chunk:
                    if os.path.exists(tmp_file):
                        os.remove(tmp_file)
                    return "empty"

                if looks_like_html_or_json(first_chunk):
                    last_reason = f"html_or_json:{ct or 'unknown'}"
                else:
                    with open(tmp_file, "wb") as f:
                        f.write(first_chunk)
                        for chunk in it:
                            if chunk:
                                f.write(chunk)

                    if os.path.getsize(tmp_file) < 100:
                        os.replace(tmp_file, out_file)
                        time.sleep(0.01 + random.random() * 0.02)
                        return "saved"
                    else:
                        os.replace(tmp_file, out_file)
                        time.sleep(0.01 + random.random() * 0.02)
                        return "saved"

                if os.path.exists(tmp_file):
                    os.remove(tmp_file)

            except Exception as e:
                if os.path.exists(tmp_file):
                    os.remove(tmp_file)
                last_reason = f"io_or_stream:{e}"

        if (
            last_reason
            and last_reason.startswith("html_or_json")
            and attempt < html_retries
        ):
            time.sleep((0.5 * (2**attempt)) + random.uniform(0, 0.25))
            continue
        else:
            break

    log_failure(z, x, y, last_reason or "unknown")
    return "failed"


def generate_tiles(z_range, x_range, y_range):
    for z in z_range:
        for x in x_range:
            for y in y_range:
                yield (z, x, y)


def main(args):
    session = make_session()
    total = saved = skipped = empty = failed = 0
    start = time.time()

    z_range = range(args.z_start, args.z_end + 1)
    x_range = range(args.x_start, args.x_end + 1)
    y_range = range(args.y_start, args.y_end + 1)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        fut2zxy = {
            ex.submit(download_tile, session, z, x, y): (z, x, y)
            for (z, x, y) in generate_tiles(z_range, x_range, y_range)
        }
        for fut in as_completed(fut2zxy):
            status = fut.result()
            total += 1
            if status == "saved":
                saved += 1
            elif status == "skipped":
                skipped += 1
            elif status == "empty":
                empty += 1
            else:
                failed += 1

            if total % 1000 == 0:
                dt = time.time() - start
                print(
                    f"[{total:,}] saved={saved:,} skipped={skipped:,} empty={empty:,} failed={failed:,} ~{total/dt:.1f}/s"
                )

    dt = time.time() - start
    print(
        f"Done total={total:,} saved={saved:,} skipped={skipped:,} empty={empty:,} failed={failed:,} in {dt:.1f}s"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Census Vector Tiles")
    parser.add_argument("--z-start", type=int, required=True, help="Starting zoom level")
    parser.add_argument("--z-end", type=int, required=True, help="Ending zoom level")
    parser.add_argument("--x-start", type=int, required=True, help="Starting X tile")
    parser.add_argument("--x-end", type=int, required=True, help="Ending X tile")
    parser.add_argument("--y-start", type=int, required=True, help="Starting Y tile")
    parser.add_argument("--y-end", type=int, required=True, help="Ending Y tile")
    parser.add_argument("--workers", type=int, default=12, help="Thread pool size")
    args = parser.parse_args()

    main(args)
