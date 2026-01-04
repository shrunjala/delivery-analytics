import imaplib
import email
from email.message import Message

def fetch_from_mailbox(host, user, password, mailbox, limit=200):
    m = imaplib.IMAP4_SSL(host)
    m.login(user, password)
    try:
        m.select(f'"{mailbox}"', readonly=True)
    except Exception as e:
        m.logout()
        raise e

    typ, data = m.search(None, "ALL")
    if typ != "OK":
        m.logout()
        return []

    ids = data[0].split()
    ids = ids[-limit:]

    emails = []
    for eid in ids:
        _, msg_data = m.fetch(eid, "(RFC822)")
        raw = msg_data[0][1]
        msg = email.message_from_bytes(raw)

        message_id = msg.get("Message-ID") or ""
        subject = msg.get("Subject") or ""
        date = msg.get("Date") or ""

        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() in ("text/plain", "text/html"):
                    payload = part.get_payload(decode=True)
                    if payload:
                        body += payload.decode(errors="ignore") + "\n"
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                body = payload.decode(errors="ignore")

        emails.append({"message_id": message_id, "subject": subject, "date": date, "body": body})

    m.logout()
    return emails
