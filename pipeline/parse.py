import re
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

_money = re.compile(r"\$?\s*([0-9]+(?:\.[0-9]{2})?)")

def _text_from_html(s: str) -> str:
    if "<html" in s.lower():
        return BeautifulSoup(s, "lxml").get_text("\n")
    return s

def parse_order(platform, account_email, label, subject, date_str, body):
    text = _text_from_html(body)
    try:
        order_ts = dtparser.parse(date_str)
    except Exception:
        order_ts = None

    merchant = None
    m = re.search(r"from\s+(.+)", subject, re.IGNORECASE)
    if m:
        merchant = m.group(1).strip()

    total = None
    mt = _money.search(text)
    if mt:
        total = float(mt.group(1))

    return {
        "platform": platform,
        "account_email": account_email,
        "label": label,
        "order_ts": order_ts,
        "merchant": merchant,
        "subtotal": None,
        "fees": None,
        "total": total,
        "currency": "USD",
        "raw_subject": subject,
    }
