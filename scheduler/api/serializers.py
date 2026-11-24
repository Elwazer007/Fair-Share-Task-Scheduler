import json

from rest_framework import serializers

from scheduler.models import Job

MAX_PAYLOAD_BYTES = 64 * 1024
MAX_BATCH = 1000


def validate_payload(value):
    if not isinstance(value, dict):
        raise serializers.ValidationError("payload must be a JSON object")
    if len(json.dumps(value, separators=(",", ":"))) > MAX_PAYLOAD_BYTES:
        raise serializers.ValidationError("payload is larger than 64 KB")
    return value


class SubmitSerializer(serializers.Serializer):
    tenant = serializers.CharField(max_length=64)
    payload = serializers.JSONField(validators=[validate_payload])


class BatchSubmitSerializer(serializers.Serializer):
    tenant = serializers.CharField(max_length=64)
    payloads = serializers.ListField(
        child=serializers.JSONField(validators=[validate_payload]),
        min_length=1,
        max_length=MAX_BATCH,
    )


class JobStatusSerializer(serializers.ModelSerializer):
    tenant = serializers.CharField(source="tenant_id")
    last_error = serializers.SerializerMethodField()

    class Meta:
        model = Job
        fields = [
            "id",
            "tenant",
            "state",
            "attempts",
            "max_attempts",
            "submitted_at",
            "first_started_at",
            "started_at",
            "finished_at",
            "run_after",
            "last_error",
        ]

    def get_last_error(self, job):
        return job.last_error or None
