import os
import requests
from dotenv import load_dotenv

load_dotenv()


class PosthogClient:
    def __init__(self):
        self.base_url = "https://eu.posthog.com"
        self.api_key = os.getenv("POSTHOG_API_KEY")
        self.project_id = os.getenv("POSTHOG_PROJECT_ID")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        }

    def fetch_all(self, endpoint):
        url = f"{self.base_url}{endpoint}"
        results = []

        while url:
            response = requests.get(url, headers=self.headers, timeout=60)
            response.raise_for_status()

            payload = response.json()

            results.extend(payload.get("results", []))
            url = payload.get("next")

        return results

    def get_events(self):
        endpoint = f"/api/projects/{self.project_id}/events"
        return self.fetch_all(endpoint)

    def get_persons(self):
        endpoint = f"/api/projects/{self.project_id}/persons"
        return self.fetch_all(endpoint)


if __name__ == "__main__":
    pass
