class SchedulerError(Exception):
    code = "error"
    status = 500


class TenantNotFound(SchedulerError):
    code = "tenant_not_found"
    status = 404


class JobNotFound(SchedulerError):
    code = "job_not_found"
    status = 404


class StoreUnavailable(SchedulerError):
    code = "store_unavailable"
    status = 503
