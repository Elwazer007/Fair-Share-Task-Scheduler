from django.contrib import admin

from scheduler.models import Job, Tenant


@admin.register(Tenant)
class TenantAdmin(admin.ModelAdmin):
    list_display = ("id", "weight", "max_queued", "created_at")
    search_fields = ("id",)


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    list_display = ("id", "tenant", "state", "attempts", "submitted_at", "finished_at")
    list_filter = ("state",)
    search_fields = ("id", "tenant__id")
    readonly_fields = [f.name for f in Job._meta.fields]
