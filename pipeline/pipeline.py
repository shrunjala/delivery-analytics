import os
from pipeline.imap_utils import fetch_from_mailbox
from pipeline.parse import parse_order
from pipeline.db import init_db, upsert_orders

IMAP_HOST = "imap.gmail.com"

def main():
    init_db()

    email_addr = os.environ["DD_EMAIL"]
    app_password = os.environ["DD_APP_PASSWORD"]
    label = os.environ["DD_LABEL"]

    msgs = fetch_from_mailbox(
        IMAP_HOST,
        email_addr,
        app_password,
        label,
        limit=200
    )

    rows = []
    for m in msgs:
        if not m["message_id"]:
            continue
        r = parse_order(
            platform="doordash",
            account_email=email_addr,
            label=label,
            subject=m["subject"],
            date_str=m["date"],
            body=m["body"]
        )
        r["message_id"] = m["message_id"]
        rows.append(r)

    inserted = upsert_orders(rows)
    print(f"Inserted DoorDash rows: {inserted}")

if __name__ == "__main__":
    main()
