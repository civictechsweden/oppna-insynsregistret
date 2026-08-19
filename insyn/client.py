"""
Low-level API client for Finansinspektionen Market Search (marknadssok.fi.se).
"""

from __future__ import annotations

import csv
import io
import ssl
import time
from datetime import date, datetime, timedelta
from typing import Callable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

BASE_URL = "https://marknadssok.fi.se/Publiceringsklient/sv-SE/Search/Search"
MAR_INCEPTION_DATE = date(2016, 7, 3)
MAX_EXPORT_ROWS = 1000

EXPECTED_HEADER = [
    "Publiceringsdatum",
    "Emittent",
    "LEI-kod",
    "Anmälningsskyldig",
    "Person i ledande ställning",
    "Befattning",
    "Närstående",
    "Korrigering",
    "Beskrivning av korrigering",
    "Är förstagångsrapportering",
    "Är kopplad till aktieprogram",
    "Karaktär",
    "Instrumenttyp",
    "Instrumentnamn",
    "ISIN",
    "Transaktionsdatum",
    "Volym",
    "Volymsenhet",
    "Pris",
    "Valuta",
    "Handelsplats",
    "Status",
]


class TLS12Adapter(HTTPAdapter):
    """Force TLS 1.2 for reliable communication with FI's Windows Server / IIS stack."""

    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.maximum_version = ssl.TLSVersion.TLSv1_2
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        kwargs["ssl_context"] = ctx
        super().init_poolmanager(*args, **kwargs)


def clean_cell(text: str) -> str:
    """Clean non-breaking spaces (\\xa0) and normalize internal newlines."""
    if not text:
        return ""
    cleaned = text.replace("\xa0", " ").strip()
    cleaned = cleaned.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    return cleaned


def parse_export_csv(raw_bytes: bytes) -> Tuple[List[str], List[List[str]]]:
    """
    Parse UTF-16LE CSV bytes from Finansinspektionen export.
    Handles trailing semicolons, missing trailing NULL columns, and embedded newlines.
    Guarantees every returned data row matches the header length.
    """
    text = raw_bytes.decode("utf-16", errors="replace")
    if not text.strip():
        return [], []

    reader = csv.reader(io.StringIO(text), delimiter=";")
    parsed_rows = list(reader)
    if not parsed_rows:
        return [], []

    header = [clean_cell(col) for col in parsed_rows[0]]
    while header and header[-1] == "":
        header.pop()

    if not header:
        header = list(EXPECTED_HEADER)

    target_len = len(header)
    data_rows = []

    for row in parsed_rows[1:]:
        cleaned = [clean_cell(cell) for cell in row]
        if not any(cleaned):
            continue
        # Pad row if FI truncated trailing NULL columns
        if len(cleaned) < target_len:
            cleaned.extend([""] * (target_len - len(cleaned)))
        elif len(cleaned) > target_len:
            while len(cleaned) > target_len and cleaned[-1] == "":
                cleaned.pop()
            if len(cleaned) > target_len:
                cleaned = cleaned[:target_len]

        data_rows.append(cleaned)

    return header, data_rows


class InsynClient:
    """HTTP Client for querying Finansinspektionen's Insynsregister."""

    def __init__(self, concurrency: int = 4, timeout: float = 12.0):
        self.timeout = timeout
        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.4,
            status_forcelist=[500, 502, 503, 504],
            raise_on_status=False,
        )
        adapter = TLS12Adapter(
            max_retries=retries,
            pool_connections=concurrency * 2,
            pool_maxsize=concurrency * 2,
        )
        self.session.mount("https://", adapter)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
        })

    def fetch_interval(
        self,
        date_from: date,
        date_to: date,
        search_type: str = "Insyn",
    ) -> Tuple[List[str], List[List[str]]]:
        """Fetch a single date range from FI's CSV export endpoint."""
        params = {
            "SearchFunctionType": search_type,
            "Publiceringsdatum.From": date_from.isoformat(),
            "Publiceringsdatum.To": date_to.isoformat(),
            "button": "export",
        }
        for attempt in range(5):
            try:
                resp = self.session.get(BASE_URL, params=params, timeout=self.timeout)
                if resp.status_code == 200 and resp.content:
                    header, rows = parse_export_csv(resp.content)
                    return header, rows
            except Exception:
                time.sleep(0.3 * (attempt + 1))

        return list(EXPECTED_HEADER), []

    def fetch_interval_adaptive(
        self,
        date_from: date,
        date_to: date,
        search_type: str = "Insyn",
    ) -> Tuple[List[str], List[List[str]], int]:
        """
        Fetch a date interval with automatic recursive bisection if the row count
        hits the 1,000-row export ceiling.
        """
        stack = [(date_from, date_to)]
        all_rows: List[List[str]] = []
        final_header: List[str] = []
        queries = 0

        while stack:
            s, e = stack.pop()
            queries += 1
            header, rows = self.fetch_interval(s, e, search_type=search_type)
            if header and not final_header:
                final_header = header

            if len(rows) >= MAX_EXPORT_ROWS and s < e:
                mid = s + (e - s) // 2
                stack.append((mid + timedelta(days=1), e))
                stack.append((s, mid))
            else:
                all_rows.extend(rows)

        return final_header or list(EXPECTED_HEADER), all_rows, queries
