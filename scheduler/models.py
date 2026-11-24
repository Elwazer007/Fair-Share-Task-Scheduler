import logging
import uuid

from django.conf import settings
from django.core.validators import MaxValueValidator, MinValueValidator, RegexValidator
from django.db import models
from django.db.models import Q

logger = logging.getLogger(__name__)


def default_max_queued():
    return settings.DEFAULT_MAX_QUEUED


def default_max_attempts():
    return settings.MAX_ATTEMPTS


class Tenant(models.Model):
    id = models.CharField(
        primary_key=True,
        max_length=64,
        validators=[RegexValidator(r"^[a-z0-9-]{1,64}$")],
    )
    weight = models.PositiveIntegerField(
        default=1, validators=[MinValueValidator(1), MaxValueValidator(100)]
    )
    max_queued = models.PositiveIntegerField(default=default_max_queued)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.id

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        from scheduler.fairqueue.scripts import FairQueue

        try:
            FairQueue.default().set_weight(self.id, self.weight)
        except Exception:
            logger.warning("weight for %s not pushed to redis; reconcile will restore it", self.id)


class JobState(models.TextChoices):
    QUEUED = "queued"
    RUNNING = "running"
    RETRYING = "retrying"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


TERMINAL_STATES = (JobState.SUCCEEDED, JobState.FAILED)


class Job(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    tenant = models.ForeignKey(Tenant, on_delete=models.PROTECT, related_name="jobs")
    payload = models.JSONField()
    state = models.CharField(max_length=16, choices=JobState.choices, default=JobState.QUEUED)
    attempts = models.PositiveIntegerField(default=0)
    max_attempts = models.PositiveIntegerField(default=default_max_attempts)
    run_after = models.DateTimeField(null=True, blank=True)
    worker_id = models.CharField(max_length=128, blank=True, default="")
    submitted_at = models.DateTimeField(auto_now_add=True)
    first_started_at = models.DateTimeField(null=True, blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    runtime_seconds = models.FloatField(default=0)
    last_error = models.TextField(blank=True, default="")

    class Meta:
        indexes = [
            models.Index(fields=["tenant", "submitted_at"], name="job_tenant_submitted"),
            models.Index(
                fields=["submitted_at"],
                name="job_open_submitted",
                condition=~Q(state__in=TERMINAL_STATES),
            ),
        ]

    def __str__(self):
        return str(self.id)


class ShareSample(models.Model):
    taken_at = models.DateTimeField()
    tenant = models.ForeignKey(Tenant, on_delete=models.CASCADE, related_name="samples")
    running = models.PositiveIntegerField()
    worker_seconds = models.FloatField()
    queued = models.PositiveIntegerField()

    class Meta:
        indexes = [models.Index(fields=["taken_at"], name="sample_taken_at")]

    def __str__(self):
        return f"{self.tenant_id}@{self.taken_at.isoformat()}"
