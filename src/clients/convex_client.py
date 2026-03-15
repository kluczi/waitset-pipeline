import os
from dotenv import load_dotenv
import requests

load_dotenv()


class ConvexClient:
    def __init__(self):
        self.base_url = os.getenv("CONVEX_URL")
        self.headers = {"accept": "application/json"}
        self.body = {"path": "projects:list", "args": {}, "format": "json"}

    def get_data(self):
        url = f"{self.base_url}/api/query"
        response = requests.post(url, headers=self.headers, json=self.body)
        response.raise_for_status()
        return response


client = ConvexClient()

response = client.get_data()

print("Status:", response.status_code)
print("Headers:", response.headers)
print("Body:", response.json())
