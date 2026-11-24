from rest_framework import status
from rest_framework.exceptions import APIException, ValidationError
from rest_framework.response import Response
from rest_framework.views import exception_handler

from scheduler.services.errors import SchedulerError


def error_response(code, detail, status_code):
    return Response({"error": code, "detail": detail}, status=status_code)


def handle_exception(exc, context):
    if isinstance(exc, SchedulerError):
        return error_response(exc.code, str(exc) or exc.code, exc.status)
    if isinstance(exc, ValidationError):
        return error_response("invalid_request", exc.detail, status.HTTP_400_BAD_REQUEST)
    if isinstance(exc, APIException) and exc.status_code == status.HTTP_400_BAD_REQUEST:
        return error_response("invalid_request", str(exc.detail), status.HTTP_400_BAD_REQUEST)
    response = exception_handler(exc, context)
    if response is not None:
        response.data = {"error": getattr(exc, "default_code", "error"), "detail": response.data}
    return response
