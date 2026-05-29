from __future__ import annotations

from spark.jobs.silver_common.rebuild import rebuild
from spark.jobs.silver_common.specs import PAYMENTS_SPEC


if __name__ == "__main__":
    rebuild(PAYMENTS_SPEC, "silver-payments")
