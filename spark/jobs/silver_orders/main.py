from __future__ import annotations

from spark.jobs.silver_common.rebuild import rebuild
from spark.jobs.silver_common.specs import ORDERS_SPEC


if __name__ == "__main__":
    rebuild(ORDERS_SPEC, "silver-orders")
