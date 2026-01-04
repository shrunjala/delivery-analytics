import os
from pipeline.imap_utils import fetch_from_mailbox
from pipeline.parse import parse_order
from pipeline.db import init_db, upsert_orders

IMAP_HOST = "imap.gmail.com"

def process_account(account_email, app_password, mailbox, platform):
    msgs = fetch_from_mailbox(IMAP_HOST, account_email, app_password, mailbox, limit=200)
    rows = []
    for m in msgs:
        if not m["message_id"]:
            continue
        r = parse_order(platform, account_email, mailbox, m["subject"], m["date"], m["body"])
        r["message_id"] = m["message_id"]
        rows.append(r)
    return upsert_orders(rows)

def main():
    init_db()
    dd_email = os.environ["DD_EMAIL"]
    dd_pass = os.environ["DD_APP_PASSWORD"]
    dd_label = os.environ["DD_LABEL"]
    ic_email = os.environ["IC_EMAIL"]
    ic_pass = os.environ["IC_APP_PASSWORD"]
    ic_label = os.environ["IC_LABEL"]

    inserted_dd = process_account(dd_email, dd_pass, dd_label, "doordash")
    inserted_ic = process_account(ic_email, ic_pass, ic_label, "instacart")

    print(f"Inserted DD rows: {inserted_dd}, IC rows: {inserted_ic}")

if __name__ == "__main__":
    main()
