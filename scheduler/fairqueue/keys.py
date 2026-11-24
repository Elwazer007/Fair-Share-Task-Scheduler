PREFIX = "fs:"


class Keys:
    def __init__(self, prefix=PREFIX):
        self.prefix = prefix
        self.ready = prefix + "ready"
        self.vtime = prefix + "vtime"
        self.gvt = prefix + "gvt"
        self.weight = prefix + "weight"
        self.est = prefix + "est"
        self.known = prefix + "known"
        self.leases = prefix + "leases"
        self.delayed = prefix + "delayed"
        self.attempt = prefix + "attempt"
        self.running = prefix + "running"
        self.usage = prefix + "usage"
        self.epoch = prefix + "epoch"

    def queue(self, tenant):
        return f"{self.prefix}q:{tenant}"

    def lease(self, job_id):
        return f"{self.prefix}lease:{job_id}"
