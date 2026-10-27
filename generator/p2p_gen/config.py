"""
Configuration for the Plant-to-Provider control tower generator.
================================================================
Every knob that changes generated data lives here, so the manifest records
the exact configuration behind every table on both platforms.

Profiles change only COUNTS. The business logic, physics and scenarios are
identical between `lite` and `baseline`, so moving up is a flag, not a
rewrite.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone


@dataclass(frozen=True)
class Profile:
    name: str
    n_plants: int
    n_dcs: int
    n_molding_machines: int
    n_sterilizers: int
    n_dc_assets: int
    n_trucks: int
    routes_per_truck: int
    n_skus: int
    n_customers: int
    n_suppliers: int
    n_idns: int
    history_days: int
    raw_tail_days: int
    machines_per_molding_line: int


PROFILES: dict[str, Profile] = {
    "lite": Profile(
        name="lite", n_plants=2, n_dcs=3, n_molding_machines=20, n_sterilizers=4,
        n_dc_assets=150, n_trucks=30, routes_per_truck=2, n_skus=300,
        n_customers=400, n_suppliers=30, n_idns=12, history_days=30,
        raw_tail_days=2, machines_per_molding_line=5,
    ),
    "baseline": Profile(
        name="baseline", n_plants=5, n_dcs=10, n_molding_machines=200,
        n_sterilizers=36, n_dc_assets=1500, n_trucks=300, routes_per_truck=2,
        n_skus=2000, n_customers=3000, n_suppliers=150, n_idns=60,
        history_days=90, raw_tail_days=7, machines_per_molding_line=6,
    ),
}


@dataclass
class P2PConfig:
    profile: str = "lite"

    seed: int = 20261027
    """Master seed. Every table derives its own RNG from (seed, table key) via
    a CRC32-based stable hash, so changing one table's size never perturbs
    another table's values."""

    company_name: str = "Lakeshore Medical Supply Co. (FICTIONAL)"
    """Deliberately fictional. Nothing in this dataset is, or resembles, real
    customer data -- say so on every slide that shows it."""

    history_start: datetime = field(
        default_factory=lambda: datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc)
    )
    """Fixed, so both platforms load byte-identical data across runs."""

    outage_dc_id: str | None = None
    """DC used for the outage/rebalancing scenario. None = catalogue default."""

    # ---- step 2: supply chain simulation ---------------------------------
    cover_days: tuple[int, int] = (60, 80)
    """Target days of inventory on hand, drawn per SKU in this range. The
    real company publicly cites ~80 days; 60-80 keeps the same order of
    magnitude without copying its number."""

    kit_cover_days: tuple[int, int] = (5, 12)
    """Exception to cover_days for procedure packs. Custom packs are built
    close to demand (1-2 weeks of cover), not stocked like commodities --
    which is exactly why a steriliser problem reaches customers fast. Set to
    cover_days to treat packs like everything else."""

    prehistory_days: int = 110
    """The simulation starts this long before the history window so opening
    stock is produced, sterilised and shipped like any other -- every lot on
    hand at day 1 has genealogy."""

    open_order_business_days: int = 5
    """Customer orders for the first N business days after history are
    created OPEN and soft-allocated to lots: the orders the hero scenario
    puts at risk."""

    hero_sterilizer_id: str | None = None
    """Steriliser whose humidity probe drifts in the hero scenario.
    None = first steriliser at the first EtO plant."""

    hero_drift_days: int = 10
    """How long before live start the probe began drifting. Every load on the
    hero steriliser in that window is in the blast radius."""

    story_minutes: dict = field(default_factory=lambda: {
        "sorter_vibration_alarm": 5, "rh_probe_failure": 15, "reefer_failure": 30, "dc_outage": 45})
    """Live story clock: minutes after live start at which each live scenario
    becomes alarm-worthy. Live start is 00:00 UTC -- night picking at the DCs,
    trailers staged at docks, plants running -- so every scenario has
    something real happening around it."""

    hero_true_rh_drift_at_live: float = 18.0
    """RH-probe reading error (percentage points, reads HIGH) reached at live
    start. The controller trusts the probe, so TRUE humidity sits this far
    below setpoint (65%) -- below the 50% validated minimum once the error
    passes 15. Reported RH looks perfect throughout; only steam consumption
    reveals it."""

    out_root: str = "output"
    write_csv: bool = True

    def __post_init__(self) -> None:
        if self.profile not in PROFILES:
            raise ValueError(f"profile must be one of {sorted(PROFILES)}")

    @property
    def p(self) -> Profile:
        return PROFILES[self.profile]

    @property
    def history_end(self) -> datetime:
        return self.history_start + timedelta(days=self.p.history_days)

    @property
    def live_start(self) -> datetime:
        """The live publisher's clock starts where history ends."""
        return self.history_end

    def manifest(self) -> dict:
        d = asdict(self)
        d["history_start"] = self.history_start.isoformat()
        d["profile_counts"] = asdict(self.p)
        d["derived"] = {
            "history_end": self.history_end.isoformat(),
            "live_start": self.live_start.isoformat(),
        }
        return d
