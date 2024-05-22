import asyncio
import concurrent.futures
from dataclasses import dataclass
from datetime import timedelta
import os
from temporalio import activity, workflow
from temporalio.client import Client, TLSConfig
from temporalio.worker import Worker
from threading import Thread
from wsgiref.simple_server import make_server


@dataclass
class ComposeGreetingInput:
    greeting: str
    name: str


# Basic activity that logs and does string concatenation
@activity.defn
async def compose_greeting(input: ComposeGreetingInput) -> str:
    return f"{input.greeting}, {input.name}!"


# Basic workflow that logs and invokes an activity
@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return await workflow.execute_activity(
            compose_greeting,
            ComposeGreetingInput("Hello", name),
            start_to_close_timeout=timedelta(seconds=10),
        )

async def run_worker():
    target_host = os.environ.get("HOST")
    namespace = os.environ.get("NAMESPACE")
    client_key = os.environ.get("CLIENT_KEY").replace("<NL>","""
""")
    client_cert = os.environ.get("CLIENT_CERT").replace("<NL>","""
""")
    client_key = bytes(client_key, "utf-8")
    client_cert = bytes(client_cert, "utf-8")

    # Start client with TLS configured
    client = await Client.connect(
        target_host,
        namespace=namespace,
        tls=TLSConfig(
            client_cert=client_cert,
            client_private_key=client_key,
        ),
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as activity_executor:
        worker = Worker(
            client,
            task_queue="greeting-task-queue",
            workflows=[GreetingWorkflow],
            activities=[compose_greeting],
            activity_executor=activity_executor,
            max_concurrent_activities=100,
        )

        print("Worker starting...")

        await worker.run()


def simple_app(environ, start_response):
    status = '200 OK'
    headers = [('Content-type', 'text/plain; charset=utf-8')]

    start_response(status, headers)

    return ["Running...\n".encode("utf-8")]

def run_server():
    with make_server('', 8000, simple_app) as httpd:
        print("Serving on port 8000...")
        httpd.serve_forever()


if __name__ == "__main__":
    Thread(target = run_server).start()
    asyncio.run(run_worker())