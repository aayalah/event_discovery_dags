from __future__ import annotations

import os
import requests
from datetime import datetime
from airflow.decorators import dag, task


TICKETMASTER_API_KEY = os.getenv("TICKETMASTER_API_KEY")
BASE_PATH = "/usr/local/airflow/data"   # this exists in your container
FILENAME = "ticketmaster_events.json"

@dag(
    dag_id="fetch_ticketmaster_taskflow",
    start_date=datetime(2025, 11, 1),
    schedule="@daily",  # or None if you want to trigger manually
    catchup=False,
    tags=["ticketmaster", "astro", "example"],
)
def fetch_ticketmaster_pipeline():
    """Fetch events from Ticketmaster API and log them."""

    @task
    def fetch_events(city: str = "San Francisco", size: int = 10):
        if not TICKETMASTER_API_KEY:
            raise ValueError("Missing TICKETMASTER_API_KEY environment variable")

        url = "https://app.ticketmaster.com/discovery/v2/events.json"
        params = {"apikey": TICKETMASTER_API_KEY, "city": city, "size": size}

        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()

        data = resp.json()
        events = data.get("_embedded", {}).get("events", [])

        print(f"Fetched {len(events)} events in {city}:")
        for e in events:
            print(f" - {e['name']}")

        # return to next task or XCom
        return [{"id": e["id"], "name": e["name"]} for e in events]

    @task
    def save_events_to_file(events: list[dict]):
        """Save fetched events to a temporary file."""
        import json
        os.makedirs(BASE_PATH, exist_ok=True)
       
        full_path = os.path.join(BASE_PATH, FILENAME)
        with open(full_path, "w") as f:
            json.dump(events, f, indent=2)
        print("Wrote file to:", full_path)
        print("Directory listing:")
        print(os.listdir(BASE_PATH))
        return full_path

    # Define task order
    events = fetch_events()
    save_events_to_file(events)


fetch_ticketmaster_pipeline()