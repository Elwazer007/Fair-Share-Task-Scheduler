import signal

from django.core.management.base import BaseCommand

from scheduler.runtime.maintenance import Maintenance


class Command(BaseCommand):
    help = "Recover expired leases, promote due retries and reconcile the queue with the database"

    def handle(self, **options):
        maintenance = Maintenance()
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: maintenance.stop())
        maintenance.run()
