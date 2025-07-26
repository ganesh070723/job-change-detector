#!/usr/bin/env python3
"""
amazon_job_monitor.py
=====================

Monitors Amazon Germany's fulfillment & operations "Rheinland-Pfalz" jobs section,
checking every 2 seconds and emailing on additions/removals.

Set via `.env` or environment:
  SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD,
  SENDER_EMAIL, RECIPIENTS

Dependencies:
  pip install requests beautifulsoup4 python-dotenv

Usage:
  python amazon_job_monitor.py

Ctrl+C to stop. State stored in previous_jobs.json.
"""
import os
import sys
import time
import json
import logging
import smtplib
from typing import Dict, Tuple, List
import requests
from bs4 import BeautifulSoup, Tag
from email.message import EmailMessage
from dotenv import load_dotenv

# load environment
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

class AmazonJobMonitor:
    JOBS_URL = (
        "https://www.amazon.jobs/content/de/teams/fulfillment-and-operations/germany"
    )
    STATE_FILE = "previous_jobs.json"

    def __init__(self, interval: int = 2):
        self.interval = interval

    def _load_state(self) -> Dict[str, str]:
        if os.path.exists(self.STATE_FILE):
            try:
                with open(self.STATE_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logging.warning(f"Failed to load state: {e}")
        return {}

    def _save_state(self, state: Dict[str, str]):
        try:
            with open(self.STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logging.warning(f"Failed to save state: {e}")

    def fetch_jobs(self) -> Dict[str, str]:
        headers = {'User-Agent': 'Mozilla/5.0'}
        try:
            r = requests.get(self.JOBS_URL, headers=headers, timeout=30)
            r.raise_for_status()
        except Exception as e:
            logging.error(f"HTTP error: {e}")
            return {}
        soup = BeautifulSoup(r.text, 'html.parser')
        heading = soup.find(lambda t: isinstance(t, Tag)
                            and t.name in ('h3','h4')
                            and 'Rheinland' in t.get_text())
        if not heading:
            logging.warning("Rheinland-Pfalz heading not found.")
            return {}
        jobs = {}
        for el in heading.next_siblings:
            if isinstance(el, Tag) and el.name in ('h3','h4'):
                break
            if not isinstance(el, Tag):
                continue
            for a in el.find_all('a', href=True):
                title = a.get_text(strip=True)
                href = a['href']
                if href.startswith('/'):
                    href = requests.compat.urljoin(self.JOBS_URL, href)
                parent_text = a.parent.get_text(' ', strip=True)
                loc = parent_text.replace(title, '').strip()
                key = f"{loc} – {title}" if loc else title
                jobs[key] = href
        return jobs

    def compare(self, old: Dict[str,str], new: Dict[str,str]) -> Tuple[List[str],List[str]]:
        old_set, new_set = set(old), set(new)
        return sorted(new_set - old_set), sorted(old_set - new_set)

    def send_email(self, subject: str, body: str):
        host = os.getenv('SMTP_HOST')
        port = os.getenv('SMTP_PORT')
        user = os.getenv('SMTP_USERNAME')
        pwd = os.getenv('SMTP_PASSWORD')
        sender = os.getenv('SENDER_EMAIL')
        rcpts = os.getenv('RECIPIENTS')
        if not all([host,port,user,pwd,sender,rcpts]):
            logging.warning("Missing SMTP config.")
            return
        msg = EmailMessage()
        msg['From'] = sender
        msg['To'] = rcpts
        msg['Subject'] = subject
        msg.set_content(body)
        try:
            port_i = int(port)
            if port_i == 465:
                with smtplib.SMTP_SSL(host, port_i) as smtp:
                    smtp.login(user,pwd)
                    smtp.send_message(msg)
            else:
                with smtplib.SMTP(host, port_i) as smtp:
                    smtp.starttls()
                    smtp.login(user,pwd)
                    smtp.send_message(msg)
            logging.info("Email sent.")
        except Exception as e:
            logging.error(f"Email failed: {e}")

    def run(self):
        logging.info(f"Starting monitor: interval={self.interval}s")
        try:
            while True:
                old_state = self._load_state()
                new_state = self.fetch_jobs()
                added, removed = self.compare(old_state, new_state)
                if added or removed:
                    logging.info(f"Detected +{len(added)} -{len(removed)}")
                    lines = []
                    if added:
                        lines.append("New:")
                        lines += [f"• {i} ({new_state[i]})" for i in added]
                    if removed:
                        lines.append("Removed:")
                        lines += [f"• {i} ({old_state[i]})" for i in removed]
                    subj = "[Amazon Jobs] Rheinland-Pfalz Update"
                    self.send_email(subj, '\n'.join(lines))
                    self._save_state(new_state)
                else:
                    logging.debug("No change.")
                time.sleep(self.interval)
        except KeyboardInterrupt:
            logging.info("Exiting monitor.")

if __name__ == '__main__':
    monitor = AmazonJobMonitor(interval=2)
    sys.exit(0 if monitor.run() is None else 1)
