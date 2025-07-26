#!/usr/bin/env python3
"""
amazon_job_monitor.py
=====================

This script monitors the Amazon Germany fulfilment and operations jobs page for
openings in the Bundesland "Rheinland‑Pfalz".  It periodically checks the
website every two minutes and, whenever it detects a change in the job
postings within the Rheinland‑Pfalz section, it sends a notification email to
the configured recipient(s).

Usage
-----

Before running the script, you must set the following environment variables:

```
SMTP_HOST      The hostname of your SMTP server (e.g. "smtp.gmail.com")
SMTP_PORT      The port number of your SMTP server (e.g. "465" for SSL)
SMTP_USERNAME  The username to authenticate with the SMTP server
SMTP_PASSWORD  The password or app‑specific password for your SMTP account
SENDER_EMAIL   The email address the notifications will be sent from
RECIPIENTS     Comma‑separated list of addresses to notify
```

Alternatively, you can hardcode these values in the `send_email` function
below, but using environment variables helps keep credentials out of your
source code.  If any of the variables are missing, the script will log a
warning and skip sending emails.

The script writes a JSON file named `previous_jobs.json` into the current
working directory.  This file stores the last known set of job postings for
Rheinland‑Pfalz so the script can detect changes across runs.  If this file
does not exist on startup, it will treat all current postings as new.

While the script is designed to run indefinitely, you can stop it at any
time with Ctrl+C.  On the next run, it will pick up where it left off by
loading `previous_jobs.json`.

Note
----
The script uses the `requests` and `beautifulsoup4` packages for HTTP
requests and HTML parsing.  If they are not installed, you can install
them via pip:

```
pip install requests beautifulsoup4
```

"""

import json
import os
import sys
import time
import logging
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from email.message import EmailMessage
import smtplib

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class AmazonJobMonitor:
    """Monitor the Amazon jobs page for changes in the Rheinland‑Pfalz section."""

    JOBS_URL = "https://www.amazon.jobs/content/de/teams/fulfillment-and-operations/germany"
    DATA_FILE = "previous_jobs.json"

    def __init__(self, check_interval: int = 120) -> None:
        """
        Initialise the monitor.

        Parameters
        ----------
        check_interval: int
            How often (in seconds) to check the jobs page.  Defaults to 120 (2 minutes).
        """
        self.check_interval = check_interval
        self.previous_jobs: Dict[str, str] = self._load_previous_jobs()

    def _load_previous_jobs(self) -> Dict[str, str]:
        """Load previously saved jobs from the JSON file, if it exists."""
        if os.path.exists(self.DATA_FILE):
            try:
                with open(self.DATA_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        return data
            except Exception as e:
                logging.warning(f"Failed to load previous jobs: {e}")
        return {}

    def _save_current_jobs(self, jobs: Dict[str, str]) -> None:
        """Persist the current set of jobs to disk."""
        try:
            with open(self.DATA_FILE, "w", encoding="utf-8") as f:
                json.dump(jobs, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logging.warning(f"Failed to save jobs to {self.DATA_FILE}: {e}")

    def fetch_rheinland_jobs(self) -> Dict[str, str]:
        """
        Fetch the job postings for Rheinland‑Pfalz.

        Returns
        -------
        Dict[str, str]
            A mapping from job title text to the URL for each posting.
        """
        try:
            # Some websites return a 403 or other error for unknown user agents.
            # Provide a common desktop browser User‑Agent string to improve
            # compatibility.
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) "
                    "Gecko/20100101 Firefox/117.0"
                )
            }
            response = requests.get(self.JOBS_URL, headers=headers, timeout=30)
            response.raise_for_status()
        except Exception as e:
            logging.error(f"Error fetching {self.JOBS_URL}: {e}")
            return {}

        soup = BeautifulSoup(response.text, "html.parser")

        # Find the heading for Rheinland‑Pfalz.  The jobs page uses <h4> or <strong> with
        # the state names prefaced by '####'.  We'll search for text matching exactly
        # "Rheinland-Pfalz".
        heading = soup.find(lambda tag: tag.name in ["h4", "h3"] and "Rheinland" in tag.get_text())
        if not heading:
            logging.warning("Rheinland‑Pfalz section not found on the page.")
            return {}

        # The job links follow immediately after the heading until the next heading (which
        # starts with another Bundesland).  We'll iterate through the siblings after the
        # heading until we hit another heading element.
        jobs: Dict[str, str] = {}
        for sibling in heading.find_all_next():
            # Stop if we encounter another state heading (e.g. #### Saarland)
            if sibling.name in ["h3", "h4"] and sibling is not heading:
                break
            # The job entries are anchor tags (<a>)
            if sibling.name == "a":
                title = sibling.get_text(strip=True)
                link = sibling.get("href")
                if not link:
                    continue
                # Some links may be relative; normalise to absolute URL
                if link.startswith("/"):
                    link = requests.compat.urljoin(self.JOBS_URL, link)
                jobs[title] = link
        return jobs

    def compare_jobs(self, current_jobs: Dict[str, str]) -> Tuple[List[str], List[str]]:
        """
        Compare current jobs with the previously saved list and return additions and removals.

        Parameters
        ----------
        current_jobs: Dict[str, str]
            The latest set of job postings for Rheinland‑Pfalz.

        Returns
        -------
        Tuple[List[str], List[str]]
            Two lists: newly added job titles and removed job titles.
        """
        previous_titles = set(self.previous_jobs.keys())
        current_titles = set(current_jobs.keys())

        added = sorted(current_titles - previous_titles)
        removed = sorted(previous_titles - current_titles)
        return added, removed

    def send_email(self, subject: str, body: str) -> None:
        """
        Send a notification email.  Uses SMTP configuration from environment.

        Parameters
        ----------
        subject: str
            The email subject line.
        body: str
            The plain‑text body of the email.
        """
        smtp_host = os.getenv("SMTP_HOST")
        smtp_port = os.getenv("SMTP_PORT")
        smtp_username = os.getenv("SMTP_USERNAME")
        smtp_password = os.getenv("SMTP_PASSWORD")
        sender_email = os.getenv("SENDER_EMAIL")
        recipients = os.getenv("RECIPIENTS")

        if not all([smtp_host, smtp_port, smtp_username, smtp_password, sender_email, recipients]):
            logging.warning(
                "SMTP credentials not fully set; skipping email notification."
            )
            return

        to_addresses = [addr.strip() for addr in recipients.split(",") if addr.strip()]
        if not to_addresses:
            logging.warning("No recipient addresses provided; skipping email notification.")
            return

        msg = EmailMessage()
        msg["From"] = sender_email
        msg["To"] = ", ".join(to_addresses)
        msg["Subject"] = subject
        msg.set_content(body)

        try:
            # Use SSL if port is 465, otherwise start TLS
            port_num = int(smtp_port)
            if port_num == 465:
                with smtplib.SMTP_SSL(smtp_host, port_num) as server:
                    server.login(smtp_username, smtp_password)
                    server.send_message(msg)
            else:
                with smtplib.SMTP(smtp_host, port_num) as server:
                    server.starttls()
                    server.login(smtp_username, smtp_password)
                    server.send_message(msg)
            logging.info(f"Notification email sent to: {to_addresses}")
        except Exception as e:
            logging.error(f"Failed to send notification email: {e}")

    def run(self) -> None:
        """Start the monitoring loop."""
        logging.info(
            f"Starting Amazon job monitor; checking every {self.check_interval} seconds."
        )
        while True:
            current_jobs = self.fetch_rheinland_jobs()
            added, removed = self.compare_jobs(current_jobs)
            if added or removed:
                logging.info(f"Change detected: {len(added)} added, {len(removed)} removed.")
                # Build a human‑readable message summarising the changes
                lines = []
                if added:
                    lines.append("New job postings:")
                    for title in added:
                        url = current_jobs.get(title, "")
                        lines.append(f"  • {title} ({url})")
                if removed:
                    lines.append("Removed job postings:")
                    for title in removed:
                        url = self.previous_jobs.get(title, "")
                        lines.append(f"  • {title} ({url})")
                body = "\n".join(lines)
                subject = "Amazon Jobs Alert: Rheinland‑Pfalz updates"
                # Send email notification
                self.send_email(subject, body)
                # Update saved state
                self.previous_jobs = current_jobs
                self._save_current_jobs(current_jobs)
            else:
                logging.debug("No changes detected.")
            # Wait before checking again
            try:
                time.sleep(self.check_interval)
            except KeyboardInterrupt:
                logging.info("Monitor interrupted by user; exiting.")
                break


def main(argv: Optional[List[str]] = None) -> int:
    monitor = AmazonJobMonitor(check_interval=120)
    try:
        monitor.run()
    except Exception as e:
        logging.error(f"Unexpected error in monitor: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))