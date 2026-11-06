"""Hydrological Modeling and Basin Intelligence Package.

Provides river network topology representations, Streeter-Phelps oxygen sag modeling,
1D advection-dispersion-reaction simulation, and spatial causal attribution.
"""

from src.hydrology.propagation_engine import (
    ADRSimulationResult,
    AdvectionDispersionReactionModel,
    AttributionCandidate,
    CausalAttributionReport,
    RiverBasinGraph,
    RiverReach,
    SagCurvePoint,
    SagCurveResult,
    SpatialCausalAttributionEngine,
    StreeterPhelpsSagModel,
)

__all__ = [
    "ADRSimulationResult",
    "AdvectionDispersionReactionModel",
    "AttributionCandidate",
    "CausalAttributionReport",
    "RiverBasinGraph",
    "RiverReach",
    "SagCurvePoint",
    "SagCurveResult",
    "SpatialCausalAttributionEngine",
    "StreeterPhelpsSagModel",
]
