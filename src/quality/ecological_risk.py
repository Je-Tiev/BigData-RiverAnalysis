"""Ecological Risk Assessment and Vietnamese Water Quality Classification (QCVN 08:2023/BTNMT).

Implements:
1. EmersonAmmoniaKinetics: Thermodynamic pKa, free toxic unionized ammonia (NH3) speciation.
2. QCVN082023Classifier: Limiting factor water quality classification (Tiers A, B, C, D).
3. AquaticToxicityRiskEngine: Acute Aquatic Toxicity Risk Index (AATRI, 0-100 scale).
"""

import logging
from typing import Any, ClassVar

logger = logging.getLogger("EcologicalRisk")


class EmersonAmmoniaKinetics:
    """Calculates aqueous ammonia equilibrium and toxic un-ionized NH3 according to Emerson et al. (1975)."""

    @staticmethod
    def compute_pka(temperature_c: float) -> float:
        """Computes the dissociation constant pKa of ammonia at a given temperature in Celsius."""
        t_kelvin = max(273.15, float(temperature_c) + 273.15)
        # Emerson et al. (1975) equation: pKa = 2729.92 / T_K + 0.09018
        return (2729.92 / t_kelvin) + 0.09018

    @classmethod
    def compute_free_fraction(cls, ph: float, temperature_c: float) -> float:
        """Calculates the fraction of un-ionized toxic ammonia f_NH3 in total ammonia."""
        pka = cls.compute_pka(temperature_c)
        ph_val = float(ph)
        exponent = pka - ph_val
        # Guard against math overflow
        if exponent > 30.0:
            return 0.0
        if exponent < -30.0:
            return 1.0
        return 1.0 / ((10.0**exponent) + 1.0)

    @classmethod
    def compute_free_ammonia(cls, ammonia_total: float, ph: float, temperature_c: float) -> float:
        """Calculates toxic free ammonia concentration [NH3]_free (mg/L) from total ammonia, pH, and temp."""
        tot_nh3 = max(0.0, float(ammonia_total))
        f_free = cls.compute_free_fraction(ph, temperature_c)
        return tot_nh3 * f_free

    @staticmethod
    def evaluate_toxicity_level(free_ammonia_mg_l: float) -> str:
        """Classifies aquatic ecotoxicity risk based on USEPA and international aquatic life criteria."""
        c = max(0.0, float(free_ammonia_mg_l))
        if c < 0.02:
            return "SAFE"
        if c < 0.05:
            return "CHRONIC"
        if c < 0.20:
            return "SUBLETHAL"
        return "ACUTE"


class QCVN082023Classifier:
    """Classifies surface water quality according to Vietnamese Standard QCVN 08:2023/BTNMT."""

    # Tier ordering for limiting factor principle
    TIER_RANK: ClassVar[dict[str, int]] = {"A": 1, "B": 2, "C": 3, "D": 4}
    RANK_TO_TIER: ClassVar[dict[int, str]] = {1: "A", 2: "B", 3: "C", 4: "D"}

    TIER_DESCRIPTIONS: ClassVar[dict[str, str]] = {
        "A": "Mức A: Phù hợp cấp nước sinh hoạt sau khi xử lý; bảo tồn động thực vật thủy sinh.",
        "B": "Mức B: Phù hợp cho tưới tiêu nông nghiệp, nuôi trồng thủy sản có biện pháp xử lý.",
        "C": "Mức C: Phù hợp cho giao thông thủy, làm mát công nghiệp và các mục đích khác.",
        "D": "Mức D: Ô nhiễm nặng, chỉ phục vụ thoát nước hoặc cần cải tạo phục hồi khẩn cấp.",
    }

    def classify_parameter(self, parameter: str, value: float) -> str:
        """Evaluates a single physicochemical parameter against QCVN 08:2023/BTNMT criteria."""
        p = parameter.lower().strip()
        v = float(value)

        if p == "ph":
            if 6.0 <= v <= 8.5:
                return "A"
            if 5.5 <= v <= 9.0:
                return "B"
            if 5.0 <= v <= 9.0:
                return "C"
            return "D"

        if p in ("dissolved_oxygen", "do"):
            if v >= 6.0:
                return "A"
            if v >= 5.0:
                return "B"
            if v >= 3.0:
                return "C"
            return "D"

        if p in ("ammonia", "nh4", "ammonium"):
            if v <= 0.3:
                return "A"
            if v <= 0.6:
                return "B"
            if v <= 1.0:
                return "C"
            return "D"

        if p in ("bod5", "bod"):
            if v <= 4.0:
                return "A"
            if v <= 6.0:
                return "B"
            if v <= 15.0:
                return "C"
            return "D"

        if p in ("turbidity_ntu", "turbidity"):
            if v <= 20.0:
                return "A"
            if v <= 50.0:
                return "B"
            if v <= 100.0:
                return "C"
            return "D"

        if p in ("chloride", "cl"):
            if v <= 250.0:
                return "A"
            if v <= 350.0:
                return "B"
            if v <= 600.0:
                return "C"
            return "D"

        # Default fallback for unlisted or neutral parameters
        return "A"

    def classify_reading(self, reading: dict[str, Any]) -> dict[str, Any]:
        """Classifies a multi-parameter reading using the limiting factor principle (worst parameter wins)."""
        evaluable_params = ["ph", "dissolved_oxygen", "ammonia", "bod5", "turbidity_ntu", "chloride"]
        parameter_tiers: dict[str, str] = {}
        max_rank = 1

        for param in evaluable_params:
            if param in reading and reading[param] is not None:
                tier = self.classify_parameter(param, float(reading[param]))
                parameter_tiers[param] = tier
                rank = self.TIER_RANK.get(tier, 1)
                max_rank = max(max_rank, rank)

        overall_tier = self.RANK_TO_TIER.get(max_rank, "A")
        limiting_params = [p for p, t in parameter_tiers.items() if t == overall_tier and overall_tier != "A"]

        return {
            "overall_tier": overall_tier,
            "description": self.TIER_DESCRIPTIONS.get(overall_tier, ""),
            "limiting_parameters": limiting_params,
            "parameter_tiers": parameter_tiers,
        }


class AquaticToxicityRiskEngine:
    """Computes Acute Aquatic Toxicity Risk Index (AATRI) considering synergistic environmental stressors."""

    def __init__(self) -> None:
        self.kinetics = EmersonAmmoniaKinetics()
        self.qcvn = QCVN082023Classifier()

    def compute_aatri(self, reading: dict[str, Any]) -> dict[str, Any]:
        """Computes composite AATRI index (0 to 100) and ecotoxicity profile."""
        temp = float(reading.get("temperature", 25.0) or 25.0)
        ph = float(reading.get("ph", 7.0) or 7.0)
        do = float(reading.get("dissolved_oxygen", 6.0) or 6.0)
        tot_ammonia = float(reading.get("ammonia", 0.0) or 0.0)

        # 1. Toxic Free Ammonia Penalty (0 to 50 pts)
        free_nh3 = self.kinetics.compute_free_ammonia(tot_ammonia, ph, temp)
        s_nh3 = min(50.0, (free_nh3 / 0.20) * 50.0)

        # 2. Dissolved Oxygen Hypoxia Penalty (0 to 50 pts)
        if do >= 6.0:
            s_do = 0.0
        elif do >= 4.0:
            s_do = (6.0 - do) * 7.5
        else:
            s_do = 15.0 + ((4.0 - max(0.0, do)) * 8.75)
        s_do = min(50.0, max(0.0, s_do))

        # 3. pH Stress Penalty (0 to 25 pts)
        if 6.5 <= ph <= 8.5:
            s_ph = 0.0
        elif ph < 6.5:
            s_ph = min(25.0, (6.5 - ph) * 15.0)
        else:
            s_ph = min(25.0, (ph - 8.5) * 15.0)

        # 4. Synergistic interaction penalty (Hypoxia + Ammonia)
        s_interaction = 0.0
        if do < 4.0 and free_nh3 > 0.05:
            s_interaction = 15.0

        raw_aatri = s_nh3 + s_do + s_ph + s_interaction
        aatri = min(100.0, max(0.0, raw_aatri))

        if aatri < 25.0:
            risk_level = "LOW_RISK"
        elif aatri < 50.0:
            risk_level = "MODERATE_RISK"
        elif aatri < 75.0:
            risk_level = "HIGH_RISK"
        else:
            risk_level = "CRITICAL_ACUTE_RISK"

        qcvn_summary = self.qcvn.classify_reading(reading)

        return {
            "aatri_score": round(aatri, 2),
            "risk_level": risk_level,
            "free_ammonia_mg_l": round(free_nh3, 4),
            "free_ammonia_toxicity": self.kinetics.evaluate_toxicity_level(free_nh3),
            "subscores": {
                "ammonia_penalty": round(s_nh3, 2),
                "hypoxia_penalty": round(s_do, 2),
                "ph_stress_penalty": round(s_ph, 2),
                "synergistic_interaction": round(s_interaction, 2),
            },
            "qcvn_tier": qcvn_summary["overall_tier"],
            "limiting_parameters": qcvn_summary["limiting_parameters"],
        }
