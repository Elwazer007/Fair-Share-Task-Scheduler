import os
import signal
import socket

from django.core.management.base import BaseCommand

from scheduler.runtime.worker import Worker


class Command(BaseCommand):
    help = "Run a worker process with a number of job slots"

    def add_arguments(self, parser):
        parser.add_argument("--slots", type=int, default=8)
        parser.add_argument("--worker-id", default=f"{socket.gethostname()}:{os.getpid()}")

    def handle(self, slots, worker_id, **options):
        worker = Worker(slots=slots, worker_id=worker_id)
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: worker.stop())
        worker.run()
