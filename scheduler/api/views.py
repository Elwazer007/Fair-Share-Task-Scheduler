import uuid

from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from scheduler.api.serializers import BatchSubmitSerializer, JobStatusSerializer, SubmitSerializer
from scheduler.models import Job, JobState
from scheduler.services.errors import JobNotFound
from scheduler.services.submit import submit_jobs


class SubmitJobView(APIView):
    def post(self, request):
        data = SubmitSerializer(data=request.data)
        data.is_valid(raise_exception=True)
        [job_id] = submit_jobs(data.validated_data["tenant"], [data.validated_data["payload"]])
        return Response({"id": job_id, "state": JobState.QUEUED}, status=status.HTTP_201_CREATED)


class SubmitBatchView(APIView):
    def post(self, request):
        data = BatchSubmitSerializer(data=request.data)
        data.is_valid(raise_exception=True)
        ids = submit_jobs(data.validated_data["tenant"], data.validated_data["payloads"])
        return Response({"ids": ids}, status=status.HTTP_201_CREATED)


class JobStatusView(APIView):
    def get(self, request, job_id):
        try:
            job = Job.objects.get(pk=uuid.UUID(job_id))
        except (ValueError, Job.DoesNotExist):
            raise JobNotFound(job_id) from None
        return Response(JobStatusSerializer(job).data)
