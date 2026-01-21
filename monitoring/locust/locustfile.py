from locust import HttpUser, task, between
import random

class CDCUser(HttpUser):
    wait_time = between(0.1, 0.5)

    @task
    def create_player(self):
        self.client.post("/insert")

