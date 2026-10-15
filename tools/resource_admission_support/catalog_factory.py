"""Installed test worker bindings; each process consumes only its declared Jobs."""

from __future__ import annotations

import os
from superjobs import RetryPolicy
from superjobs_contract_example import ManifestResult
from constants import RESOURCE_KEY


def register_scenario_handlers(
    catalog, *, scenario, run_id, hold_until_control, on_enter, on_exit
):
    from scenario_jobs import (
        job_a,
        job_b,
        job_redelivery,
        job_crash_successor,
        job_broker_restart,
    )

    factories = {
        "a": job_a,
        "b": job_b,
        "redelivery": job_redelivery,
        "successor": job_crash_successor,
        "broker_restart": job_broker_restart,
    }

    def make_callback(label):
        async def execute(request, context):
            await on_enter(label, context, context.attempt)
            try:
                await hold_until_control(context.id)
                return ManifestResult(revision=f"ra-{label}-{request.device_id}")
            finally:
                await on_exit(label, context)
                context.acknowledge_resource_cleanup()

        return execute

    for label in os.environ["SUPERJOBS_RA_REGISTER_JOBS"].split(","):
        catalog.register(
            factories[label](run_id),
            make_callback(label),
            resources={RESOURCE_KEY: 1},
            retry=RetryPolicy(max_attempts=1),
        )
