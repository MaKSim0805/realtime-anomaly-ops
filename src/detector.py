from __future__ import annotations
from dataclasses import dataclass
from statistics import fmean, pstdev

@dataclass(frozen=True)
class Decision:
    z_score: float
    severity: str
    alert: bool

def detect(value: float, baseline: list[float], threshold: float = 3.0) -> Decision:
    if len(baseline) < 5:
        raise ValueError("baseline needs at least 5 observations")
    std = pstdev(baseline)
    z = 0.0 if std == 0 else (value - fmean(baseline)) / std
    magnitude = abs(z)
    severity = "critical" if magnitude >= threshold * 2 else "warning" if magnitude >= threshold else "normal"
    return Decision(round(z, 3), severity, magnitude >= threshold)
