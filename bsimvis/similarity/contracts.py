from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum

EXACT_V1 = "exact-v1"


class ProductState(str, Enum):
    EXACT = "exact"
    PENDING = "pending"
    UNKNOWN = "unknown"
    FAILED = "failed"
    NOT_MATERIALIZED = "not_materialized"


class EvidenceState(str, Enum):
    MEASURED = "measured"
    EXAMINED_NOT_RETAINED = "examined_not_retained"
    NOT_EXAMINED = "not_examined"
    PENDING = "pending"


@dataclass(frozen=True)
class DistantEvidence:
    retained: EvidenceState = EvidenceState.MEASURED
    remainder: EvidenceState = EvidenceState.EXAMINED_NOT_RETAINED
    pending: bool = False
    fidelity_contract: str = EXACT_V1

    def as_dict(self):
        return {
            "retained": self.retained.value,
            "remainder": self.remainder.value,
            "pending": self.pending,
            "fidelity_contract": self.fidelity_contract,
        }


def distant_evidence(*, candidate_space_examined: bool, pending: bool = False):
    return DistantEvidence(
        retained=EvidenceState.PENDING if pending else EvidenceState.MEASURED,
        remainder=(
            EvidenceState.PENDING
            if pending
            else (
                EvidenceState.EXAMINED_NOT_RETAINED
                if candidate_space_examined
                else EvidenceState.NOT_EXAMINED
            )
        ),
        pending=pending,
    ).as_dict()


@dataclass(frozen=True)
class SimilarityProvenance:
    generation: str
    backend: str
    scheduler: str = "synchronous"
    state: ProductState = ProductState.EXACT
    fallback_reason: str | None = None
    fidelity_contract: str = EXACT_V1

    def as_dict(self):
        result = asdict(self)
        result["state"] = self.state.value
        return result


def exact_v1_envelope(
    result, *, generation, backend, scheduler="synchronous", fallback_reason=None
):
    provenance = SimilarityProvenance(
        generation=str(generation),
        backend=str(backend),
        scheduler=str(scheduler),
        fallback_reason=fallback_reason,
    )
    return {
        "product_state": provenance.state.value,
        "provenance": provenance.as_dict(),
        "result": result,
    }
