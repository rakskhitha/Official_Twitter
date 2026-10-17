"""Real credit consumption per run, measured from X's own balance (GET /2/usage/credits).

X does not report the cost of individual requests, so each run reads the balance at its start and
at its end: credits used = balance before - balance after. This is the amount X actually charged
(after its same-UTC-day deduplication), as opposed to the run's own estimate (est_cost_usd).

Reading the balance never affects the run: one attempt, no waiting, any failure = "unknown".
"""

import logging
from dataclasses import dataclass

log = logging.getLogger("x_monitor")


@dataclass
class CreditBalance:
    total: float      # spendable USD: max(0, prepaid + free)
    prepaid: float    # purchased USD (can be slightly negative when overdrawn)
    free: float       # USD left from free grants

    def describe(self) -> str:
        return f"${self.total:.3f} (prepaid ${self.prepaid:.3f}, free ${self.free:.3f})"


def read_credit_balance(client, moment: str) -> CreditBalance | None:
    """Read the balance; on any problem log a warning and return None (the run carries on)."""
    try:
        body = client.get_credit_balance()
    except Exception as exc:  # noqa: BLE001 - measuring credits must never break a run
        log.warning("Could not read the credit balance at the %s of the run (%s); credits used is not "
                    "measured for this run.", moment, exc)
        return None
    data = body.get("data") if isinstance(body, dict) else None
    try:
        return CreditBalance(total=float(data["total_balance"]),
                             prepaid=float(data.get("prepaid_balance") or 0),
                             free=float(data.get("free_balance") or 0))
    except (TypeError, KeyError, ValueError):
        log.warning("Unexpected credit balance response at the %s of the run: %s", moment, str(body)[:300])
        return None


def credits_used(before: CreditBalance | None, after: CreditBalance | None) -> tuple[float | None, str]:
    """Return (USD used, explanation). None when it cannot be measured."""
    if before is None or after is None:
        return None, "unknown (balance not available)"
    used = round(before.total - after.total, 4)
    if used < 0:
        # The balance went UP: a top-up, auto-recharge or new free grant happened during the run.
        return None, f"not measurable (balance went up by ${-used:.3f}: top-up or auto-recharge during the run)"
    return used, f"${used:.3f}"
