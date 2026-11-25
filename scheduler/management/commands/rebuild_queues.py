from django.core.management.base import BaseCommand

from scheduler.fairqueue.scripts import FairQueue
from scheduler.services.reconcile import rebuild


class Command(BaseCommand):
    help = "Restore every open job from the database into the queue"

    def handle(self, **options):
        restored = rebuild(FairQueue.default())
        self.stdout.write(f"restored {restored} jobs")
