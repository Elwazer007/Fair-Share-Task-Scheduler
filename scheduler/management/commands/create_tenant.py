from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError

from scheduler.models import Tenant


class Command(BaseCommand):
    help = "Create a tenant, or update its weight and queue cap if it exists"

    def add_arguments(self, parser):
        parser.add_argument("tenant_id")
        parser.add_argument("--weight", type=int, default=1)
        parser.add_argument("--max-queued", type=int)

    def handle(self, tenant_id, weight, max_queued, **options):
        tenant = Tenant.objects.filter(pk=tenant_id).first() or Tenant(id=tenant_id)
        tenant.weight = weight
        if max_queued is not None:
            tenant.max_queued = max_queued
        try:
            tenant.full_clean()
        except ValidationError as exc:
            raise CommandError(exc.message_dict) from exc
        created = tenant._state.adding
        tenant.save()
        verb = "created" if created else "updated"
        self.stdout.write(
            f"{verb} {tenant.id} weight={tenant.weight} max_queued={tenant.max_queued}"
        )
