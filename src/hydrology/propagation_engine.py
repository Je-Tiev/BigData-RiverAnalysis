"""Hydrological Propagation, Water Quality Modeling, and Spatial Attribution Engine.

Implements:
1. RiverBasinGraph: Directed network topology of river reaches and monitoring stations.
2. StreeterPhelpsSagModel: Analytical dissolved oxygen sag curve with Arrhenius temperature adjustment.
3. AdvectionDispersionReactionModel: 1D ADR analytical transport for instantaneous spills.
4. SpatialCausalAttributionEngine: Time-lagged backtracking and upstream attribution analysis.
"""

import logging
import math
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

logger = logging.getLogger("HydrologicalPropagation")


@dataclass
class RiverReach:
    """Represents a river stretch connecting an upstream station to a downstream station."""

    reach_id: str
    upstream_station_id: str
    downstream_station_id: str
    distance_km: float
    average_velocity_mps: float  # River flow velocity in meters per second
    channel_width_m: float = 120.0
    channel_depth_m: float = 4.5
    manning_n: float = 0.035
    channel_slope: float = 0.00015
    longitudinal_dispersion_m2s: float = 85.0  # Dx in m^2/s


@dataclass
class SagCurvePoint:
    """Represents a discrete calculation point along a river reach."""

    travel_time_hours: float
    distance_km: float
    deficit_mg_l: float
    dissolved_oxygen_mg_l: float
    bod_remaining_mg_l: float


@dataclass
class SagCurveResult:
    """Output summary of Streeter-Phelps oxygen sag curve analysis."""

    critical_travel_time_hours: float
    critical_distance_km: float
    maximum_deficit_mg_l: float
    minimum_do_mg_l: float
    do_saturation_mg_l: float
    trajectory: list[SagCurvePoint] = field(default_factory=list)


@dataclass
class ADRSimulationResult:
    """Output of 1D Advection-Dispersion-Reaction simulation."""

    peak_arrival_time_hours: float
    peak_concentration_mg_l: float
    plume_standard_deviation_km: float
    travel_distance_km: float
    time_series_at_target: list[dict[str, float]] = field(default_factory=list)


@dataclass
class AttributionCandidate:
    """Attribution score for a candidate upstream pollution source."""

    station_id: str
    station_name: str
    travel_time_hours: float
    correlation_score: float  # [0.0, 1.0]
    mass_conservation_ratio: float
    is_probable_source: bool
    confidence_level: str  # "HIGH", "MEDIUM", "LOW"
    narrative: str


@dataclass
class CausalAttributionReport:
    """Complete upstream attribution report for a detected downstream pollution incident."""

    downstream_station_id: str
    incident_timestamp: str
    pollutant_parameter: str
    observed_peak_value: float
    suspected_spill_window_hours: tuple[float, float]
    candidate_attributions: list[AttributionCandidate]
    unaccounted_residual_ratio: float
    probable_lateral_discharge_detected: bool
    diagnostic_summary: str


class RiverBasinGraph:
    """Directed topological network representing river reaches and monitoring stations."""

    def __init__(self, load_defaults: bool = True) -> None:
        """Initializes empty basin graph or preloads Vietnamese river network topologies."""
        self.stations: dict[str, dict[str, Any]] = {}
        self.reaches: list[RiverReach] = []
        self._downstream_map: dict[str, list[RiverReach]] = {}
        self._upstream_map: dict[str, list[RiverReach]] = {}

        if load_defaults:
            self._load_standard_topologies()

    def add_station(self, station_id: str, metadata: dict[str, Any] | None = None) -> None:
        """Adds a station node to the basin graph."""
        self.stations[station_id] = metadata or {"station_id": station_id}
        if station_id not in self._downstream_map:
            self._downstream_map[station_id] = []
        if station_id not in self._upstream_map:
            self._upstream_map[station_id] = []

    def add_reach(self, reach: RiverReach) -> None:
        """Adds a directed river reach connecting upstream to downstream station."""
        self.reaches.append(reach)
        self.add_station(reach.upstream_station_id)
        self.add_station(reach.downstream_station_id)

        self._downstream_map[reach.upstream_station_id].append(reach)
        self._upstream_map[reach.downstream_station_id].append(reach)

    def get_downstream_reach(self, station_id: str) -> RiverReach | None:
        """Returns the primary downstream reach originating from the given station."""
        reaches = self._downstream_map.get(station_id, [])
        return reaches[0] if reaches else None

    def get_upstream_reaches(self, station_id: str) -> list[RiverReach]:
        """Returns all reaches that flow directly into the given station."""
        return self._upstream_map.get(station_id, [])

    def compute_path_travel_time(self, upstream_id: str, downstream_id: str) -> float | None:
        """Computes total hydrodynamic travel time (in hours) along the river reach network."""
        if upstream_id == downstream_id:
            return 0.0

        if upstream_id not in self.stations or downstream_id not in self.stations:
            return None

        # Breadth-first search / shortest path based on travel time
        queue: deque[tuple[str, float]] = deque([(upstream_id, 0.0)])
        visited: dict[str, float] = {upstream_id: 0.0}

        shortest_time: float | None = None

        while queue:
            curr_node, curr_time = queue.popleft()

            if curr_node == downstream_id:
                if shortest_time is None or curr_time < shortest_time:
                    shortest_time = curr_time
                continue

            for reach in self._downstream_map.get(curr_node, []):
                next_node = reach.downstream_station_id
                v_mps = max(0.05, reach.average_velocity_mps)
                reach_time = reach.distance_km / (v_mps * 3.6)
                total_time = curr_time + reach_time

                if next_node not in visited or total_time < visited[next_node]:
                    visited[next_node] = total_time
                    queue.append((next_node, total_time))

        return shortest_time

    def get_all_upstream_ancestors(self, station_id: str) -> list[str]:
        """Traverses the network upstream to discover all contributor station IDs."""
        ancestors: list[str] = []
        queue: deque[str] = deque([station_id])
        visited: set[str] = {station_id}

        while queue:
            curr = queue.popleft()
            for reach in self.get_upstream_reaches(curr):
                up = reach.upstream_station_id
                if up not in visited:
                    visited.add(up)
                    ancestors.append(up)
                    queue.append(up)
        return ancestors

    def _load_standard_topologies(self) -> None:
        """Preloads standard river basin topologies for Red River, Dong Nai, and Mekong basins."""
        # 1. Red River - Thai Binh Basin
        self.add_reach(
            RiverReach(
                reach_id="REACH_DA_HB_BV",
                upstream_station_id="ST_DA_HB_02",
                downstream_station_id="ST_CONFLUENCE_BV",
                distance_km=65.0,
                average_velocity_mps=1.2,
                channel_width_m=200.0,
                channel_depth_m=6.5,
                longitudinal_dispersion_m2s=110.0,
            )
        )
        self.add_reach(
            RiverReach(
                reach_id="REACH_LO_VT_BV",
                upstream_station_id="ST_LO_VT_03",
                downstream_station_id="ST_CONFLUENCE_BV",
                distance_km=35.0,
                average_velocity_mps=1.0,
                channel_width_m=150.0,
                channel_depth_m=5.0,
                longitudinal_dispersion_m2s=95.0,
            )
        )
        self.add_reach(
            RiverReach(
                reach_id="REACH_HONG_BV_LB",
                upstream_station_id="ST_CONFLUENCE_BV",
                downstream_station_id="ST_HONG_LB_01",
                distance_km=55.0,
                average_velocity_mps=0.9,
                channel_width_m=350.0,
                channel_depth_m=5.5,
                longitudinal_dispersion_m2s=130.0,
            )
        )

        # 2. Dong Nai - Saigon Basin
        self.add_reach(
            RiverReach(
                reach_id="REACH_SAIGON_BC_CL",
                upstream_station_id="ST_SAIGON_BC_06",
                downstream_station_id="ST_DONGNAI_CL_07",
                distance_km=15.0,
                average_velocity_mps=0.5,
                channel_width_m=180.0,
                channel_depth_m=7.0,
                longitudinal_dispersion_m2s=75.0,
            )
        )
        self.add_reach(
            RiverReach(
                reach_id="REACH_DONGNAI_CL_SR",
                upstream_station_id="ST_DONGNAI_CL_07",
                downstream_station_id="ST_SOAI_RAP",
                distance_km=25.0,
                average_velocity_mps=0.6,
                channel_width_m=400.0,
                channel_depth_m=8.5,
                longitudinal_dispersion_m2s=140.0,
            )
        )

        # 3. Mekong / Cuu Long Basin
        self.add_reach(
            RiverReach(
                reach_id="REACH_TIEN_MYTHO_CD",
                upstream_station_id="ST_TIEN_MYTHO_08",
                downstream_station_id="ST_CUA_DAI",
                distance_km=45.0,
                average_velocity_mps=0.8,
                channel_width_m=600.0,
                channel_depth_m=9.0,
                longitudinal_dispersion_m2s=180.0,
            )
        )
        self.add_reach(
            RiverReach(
                reach_id="REACH_HAU_CT_CDA",
                upstream_station_id="ST_HAU_CT_09",
                downstream_station_id="ST_CUA_DINH_AN",
                distance_km=60.0,
                average_velocity_mps=0.85,
                channel_width_m=750.0,
                channel_depth_m=10.0,
                longitudinal_dispersion_m2s=210.0,
            )
        )


class StreeterPhelpsSagModel:
    """Classical Streeter-Phelps Dissolved Oxygen Sag Model with Arrhenius temperature adjustment."""

    @staticmethod
    def compute_saturation_do(temperature_c: float) -> float:
        """Calculates oxygen saturation concentration Cs (mg/L) using Benson-Krause formulation."""
        t = max(0.0, min(50.0, float(temperature_c)))
        cs = 14.652 - (0.41022 * t) + (0.007991 * (t**2)) - (0.000077774 * (t**3))
        return max(0.0, cs)

    def simulate_sag_curve(
        self,
        initial_bod: float,
        initial_do: float,
        temperature_c: float,
        flow_velocity_mps: float,
        max_travel_hours: float = 72.0,
        step_hours: float = 1.0,
        kd_20: float = 0.35,  # day^-1
        ka_20: float = 0.70,  # day^-1
    ) -> SagCurveResult:
        """Simulates oxygen deficit and recovery trajectory along a river reach."""
        t_c = float(temperature_c)
        v_mps = max(0.05, float(flow_velocity_mps))
        l0 = max(0.0, float(initial_bod))

        cs = self.compute_saturation_do(t_c)
        d0 = max(0.0, cs - float(initial_do))

        # Arrhenius temperature adjustment (rates in day^-1 converted to hr^-1)
        kd_day = kd_20 * (1.047 ** (t_c - 20.0))
        ka_day = ka_20 * (1.024 ** (t_c - 20.0))

        kd_hr = kd_day / 24.0
        ka_hr = ka_day / 24.0

        # Critical travel time tc
        tc_hours = 0.0
        if l0 > 0.0 and abs(ka_hr - kd_hr) > 1e-9:
            ratio = ka_hr / kd_hr
            inner_term = 1.0 - (d0 * (ka_hr - kd_hr) / (kd_hr * l0))
            if inner_term > 0.0:
                arg = ratio * inner_term
                if arg > 0.0:
                    tc_calc = (1.0 / (ka_hr - kd_hr)) * math.log(arg)
                    tc_hours = max(0.0, tc_calc)

        # Critical distance and maximum deficit
        crit_dist_km = tc_hours * (v_mps * 3.6)

        def deficit_at_t(t: float) -> float:
            if abs(ka_hr - kd_hr) < 1e-9:
                return (kd_hr * l0 * t + d0) * math.exp(-ka_hr * t)
            bod_effect = (kd_hr * l0 / (ka_hr - kd_hr)) * (math.exp(-kd_hr * t) - math.exp(-ka_hr * t))
            init_effect = d0 * math.exp(-ka_hr * t)
            return bod_effect + init_effect

        max_def = max(0.0, deficit_at_t(tc_hours))
        min_do = max(0.0, cs - max_def)

        # Build trajectory
        trajectory: list[SagCurvePoint] = []
        steps = int(max(1.0, max_travel_hours / max(0.1, step_hours)))
        for i in range(steps + 1):
            t = min(max_travel_hours, i * step_hours)
            def_t = max(0.0, deficit_at_t(t))
            do_t = max(0.0, cs - def_t)
            bod_t = max(0.0, l0 * math.exp(-kd_hr * t))
            dist_km = t * (v_mps * 3.6)
            trajectory.append(
                SagCurvePoint(
                    travel_time_hours=round(t, 2),
                    distance_km=round(dist_km, 2),
                    deficit_mg_l=round(def_t, 3),
                    dissolved_oxygen_mg_l=round(do_t, 3),
                    bod_remaining_mg_l=round(bod_t, 3),
                )
            )

        return SagCurveResult(
            critical_travel_time_hours=round(tc_hours, 2),
            critical_distance_km=round(crit_dist_km, 2),
            maximum_deficit_mg_l=round(max_def, 3),
            minimum_do_mg_l=round(min_do, 3),
            do_saturation_mg_l=round(cs, 3),
            trajectory=trajectory,
        )


class AdvectionDispersionReactionModel:
    """1D Analytical Advection-Dispersion-Reaction (ADR) contaminant transport model."""

    def simulate_pulse_propagation(
        self,
        spill_mass_kg: float,
        reach: RiverReach,
        duration_hours: float = 48.0,
        step_hours: float = 0.5,
        decay_rate_day: float = 0.15,
    ) -> ADRSimulationResult:
        """Simulates 1D longitudinal dispersion and advection of an instantaneous pollutant spill."""
        mass_kg = max(0.0, float(spill_mass_kg))
        dist_m = reach.distance_km * 1000.0
        v_mps = max(0.05, reach.average_velocity_mps)
        area_m2 = max(1.0, reach.channel_width_m * reach.channel_depth_m)
        dx_m2s = max(1.0, reach.longitudinal_dispersion_m2s)
        kr_s = max(0.0, decay_rate_day / 86400.0)

        # Theoretical peak arrival time
        t_peak_sec = dist_m / v_mps
        t_peak_hours = t_peak_sec / 3600.0

        # Plume standard deviation: sigma = sqrt(2 * Dx * t)
        sigma_m = math.sqrt(2.0 * dx_m2s * t_peak_sec)
        sigma_km = sigma_m / 1000.0

        def calc_c(t_sec: float) -> float:
            if t_sec <= 1.0 or mass_kg <= 0.0:
                return 0.0
            denom = area_m2 * math.sqrt(4.0 * math.pi * dx_m2s * t_sec)
            if denom <= 0.0:
                return 0.0
            exponent = -(((dist_m - (v_mps * t_sec)) ** 2) / (4.0 * dx_m2s * t_sec)) - (kr_s * t_sec)
            if exponent < -60.0:
                return 0.0
            # 1 kg/m^3 = 1000 mg/L
            c_kg_m3 = (mass_kg / denom) * math.exp(exponent)
            return c_kg_m3 * 1000.0

        peak_conc = calc_c(t_peak_sec)

        time_series: list[dict[str, float]] = []
        steps = int(max(1.0, duration_hours / max(0.1, step_hours)))
        for i in range(steps + 1):
            t_h = i * step_hours
            c_mg_l = calc_c(t_h * 3600.0)
            time_series.append({"time_hours": round(t_h, 2), "concentration": round(c_mg_l, 4)})

        return ADRSimulationResult(
            peak_arrival_time_hours=round(t_peak_hours, 2),
            peak_concentration_mg_l=round(peak_conc, 4),
            plume_standard_deviation_km=round(sigma_km, 3),
            travel_distance_km=round(reach.distance_km, 2),
            time_series_at_target=time_series,
        )

    def simulate_spatial_profile(
        self,
        spill_mass_kg: float,
        reach: RiverReach,
        time_hours: float,
        num_points: int = 100,
    ) -> list[dict[str, float]]:
        """Simulates spatial snapshot profile of pollutant concentration C(x, t) at fixed time t."""
        mass_kg = max(0.0, float(spill_mass_kg))
        t_sec = max(1.0, float(time_hours) * 3600.0)
        v_mps = max(0.05, reach.average_velocity_mps)
        area_m2 = max(1.0, reach.channel_width_m * reach.channel_depth_m)
        dx_m2s = max(1.0, reach.longitudinal_dispersion_m2s)

        denom = area_m2 * math.sqrt(4.0 * math.pi * dx_m2s * t_sec)
        max_dist_m = reach.distance_km * 1500.0
        step_m = max_dist_m / max(10, num_points)

        profile: list[dict[str, float]] = []
        for i in range(num_points + 1):
            x_m = i * step_m
            exponent = -(((x_m - (v_mps * t_sec)) ** 2) / (4.0 * dx_m2s * t_sec))
            c_mg_l = 0.0
            if exponent >= -60.0 and denom > 0.0:
                c_mg_l = (mass_kg / denom) * math.exp(exponent) * 1000.0
            profile.append({"distance_km": round(x_m / 1000.0, 2), "concentration": round(c_mg_l, 4)})

        return profile


class SpatialCausalAttributionEngine:
    """Backtracking and causal attribution engine for downstream pollution spikes."""

    def attribute_pollution_incident(
        self,
        downstream_station_id: str,
        incident_time: datetime,
        pollutant_param: str,
        downstream_series: list[dict[str, Any]],
        upstream_series_by_station: dict[str, list[dict[str, Any]]],
        graph: RiverBasinGraph,
    ) -> CausalAttributionReport:
        """Traces back travel times and determines upstream source attribution or illegal lateral spill."""
        if incident_time.tzinfo is None:
            incident_time = incident_time.replace(tzinfo=UTC)

        # 1. Identify downstream peak value
        observed_peak = 0.0
        for r in downstream_series:
            val = float(r.get(pollutant_param, 0.0) or 0.0)
            observed_peak = max(observed_peak, val)

        # 2. Query upstream stations connected via graph
        upstream_candidates: list[AttributionCandidate] = []
        all_upstream_ids = graph.get_all_upstream_ancestors(downstream_station_id)

        earliest_window_hours = 0.0
        latest_window_hours = 0.0
        lateral_detected = False

        if not all_upstream_ids:
            diagnostic = f"Trạm {downstream_station_id} là trạm khởi nguồn hoặc không có trạm thượng lưu ghi nhận trong đồ thị lưu vực."
            lateral_detected = observed_peak > 5.0
            return CausalAttributionReport(
                downstream_station_id=downstream_station_id,
                incident_timestamp=incident_time.isoformat(),
                pollutant_parameter=pollutant_param,
                observed_peak_value=round(observed_peak, 3),
                suspected_spill_window_hours=(0.0, 0.0),
                candidate_attributions=[],
                unaccounted_residual_ratio=1.0 if lateral_detected else 0.0,
                probable_lateral_discharge_detected=lateral_detected,
                diagnostic_summary=diagnostic,
            )

        max_upstream_corr = 0.0

        for up_id in all_upstream_ids:
            travel_time = graph.compute_path_travel_time(up_id, downstream_station_id)
            if travel_time is None:
                continue

            earliest_window_hours = max(earliest_window_hours, travel_time + 4.0)
            latest_window_hours = min(latest_window_hours, travel_time - 2.0)

            up_series = upstream_series_by_station.get(up_id, [])
            up_meta = graph.stations.get(up_id, {})
            up_name = up_meta.get("station_name", up_id)

            if not up_series:
                upstream_candidates.append(
                    AttributionCandidate(
                        station_id=up_id,
                        station_name=up_name,
                        travel_time_hours=round(travel_time, 2),
                        correlation_score=0.0,
                        mass_conservation_ratio=0.0,
                        is_probable_source=False,
                        confidence_level="LOW",
                        narrative=f"Thiếu chuỗi dữ liệu thượng lưu tại {up_name} để đối soát.",
                    )
                )
                continue

            # Check peak in upstream window around (incident_time - travel_time)
            target_time = incident_time - timedelta(hours=travel_time)
            window_start = target_time - timedelta(hours=4.0)
            window_end = target_time + timedelta(hours=4.0)

            up_peak = 0.0
            for r in up_series:
                r_ts = r.get("timestamp")
                if r_ts:
                    try:
                        ts_dt = datetime.fromisoformat(str(r_ts))
                        if not (window_start <= ts_dt <= window_end):
                            continue
                    except (ValueError, TypeError):
                        pass
                v = float(r.get(pollutant_param, 0.0) or 0.0)
                up_peak = max(up_peak, v)

            # Rough correlation / peak correspondence
            if observed_peak > 0.0:
                mass_ratio = min(1.0, up_peak / observed_peak)
            else:
                mass_ratio = 0.0

            # Signal correlation heuristic: if upstream also showed a spike
            is_probable = up_peak >= 0.5 * observed_peak and up_peak > 3.0
            corr = 0.85 if is_probable else max(0.05, mass_ratio * 0.4)
            max_upstream_corr = max(max_upstream_corr, corr)

            conf = "HIGH" if corr > 0.7 else ("MEDIUM" if corr > 0.4 else "LOW")
            narrative = (
                f"Phát hiện đỉnh nồng độ tương đồng ({up_peak:.2f}) tại thượng nguồn {up_name} trước đó {travel_time:.1f} giờ."
                if is_probable
                else f"Chỉ số tại {up_name} ({up_peak:.2f}) không đủ lớn để giải trình nồng độ ô nhiễm tại hạ lưu."
            )

            upstream_candidates.append(
                AttributionCandidate(
                    station_id=up_id,
                    station_name=up_name,
                    travel_time_hours=round(travel_time, 2),
                    correlation_score=round(corr, 3),
                    mass_conservation_ratio=round(mass_ratio, 3),
                    is_probable_source=is_probable,
                    confidence_level=conf,
                    narrative=narrative,
                )
            )

        # Check if downstream spike is unaccounted for by upstream stations
        unaccounted_ratio = max(0.0, 1.0 - max_upstream_corr)
        if observed_peak > 5.0 and max_upstream_corr < 0.4:
            lateral_detected = True
            diagnostic = (
                f"CẢNH BÁO: Nồng độ {pollutant_param} đạt {observed_peak:.2f} tại {downstream_station_id} "
                f"nhưng toàn bộ các trạm thượng nguồn ghi nhận giá trị thấp. Khả năng cao có xả thải lén giữa dòng."
            )
        else:
            lateral_detected = False
            probable_names = [c.station_name for c in upstream_candidates if c.is_probable_source]
            if probable_names:
                diagnostic = (
                    f"Nguồn phát tán ô nhiễm được quy kết chính xác về phía thượng lưu: {', '.join(probable_names)}."
                )
            else:
                diagnostic = "Hệ thống ghi nhận dao động thủy văn bình thường, không có dấu hiệu xả thải bất thường."

        return CausalAttributionReport(
            downstream_station_id=downstream_station_id,
            incident_timestamp=incident_time.isoformat(),
            pollutant_parameter=pollutant_param,
            observed_peak_value=round(observed_peak, 3),
            suspected_spill_window_hours=(round(latest_window_hours, 1), round(earliest_window_hours, 1)),
            candidate_attributions=upstream_candidates,
            unaccounted_residual_ratio=round(unaccounted_ratio, 3),
            probable_lateral_discharge_detected=lateral_detected,
            diagnostic_summary=diagnostic,
        )
