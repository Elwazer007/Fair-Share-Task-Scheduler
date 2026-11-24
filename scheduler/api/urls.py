from django.urls import path

from scheduler.api import views

urlpatterns = [
    path("jobs", views.SubmitJobView.as_view()),
    path("jobs/batch", views.SubmitBatchView.as_view()),
    path("jobs/<str:job_id>", views.JobStatusView.as_view()),
]
