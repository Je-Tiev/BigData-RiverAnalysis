"""Dead-Letter Queue (DLQ) Self-Healing Engine and Recovery Audit Logger.

Implements deterministic healing rules for corrupted, legacy, or malformed sensor telemetry:
1. Rule 1: Numeric String Coercion (e.g. " 24.5\\n" -> 24.5).
2. Rule 2: Legacy Schema Column Key Mapping (e.g. "Dissolved Oxygen" -> "dissolved_oxygen").
3. Rule 3: ISO Timestamp Normalization (e.g. "2026-11-06" -> "2026-11-06T00:00:00Z").
4. Rule 4: Physical Boundary Value Clipping with Audit Trail Flagging.
"""

import json
import logging
import re
from datetime import UTC, datetime
from typing import Any

from src.contracts.river_schema import PHYSICAL_LIMITS

logger = logging.getLogger("DLQRecoveryEngine")

LEGACY_KEY_MAPPING: dict[str, str] = {
    "dissolved oxygen": "dissolved_oxygen",
    "dissolved_oxygen_mg_l": "dissolved_oxygen",
    "do": "dissolved_oxygen",
    "ammonia-total (as n)": "ammonia",
    "ammonia_total": "ammonia",
    "nh4": "ammonia",
    "bod - 5 days (total)": "bod5",
    "bod_5": "bod5",
    "bod": "bod5",
    "conductivity @25°c": "conductivity",
    "conductivity_at_25c": "conductivity",
    "ec": "conductivity",
    "turbidity": "turbidity_ntu",
    "turbidity (ntu)": "turbidity_ntu",
    "water temperature": "temperature",
    "temp": "temperature",
    "temperature_c": "temperature",
    "stationid": "station_id",
    "station_code": "station_id",
    "timestamp_iso": "timestamp",
    "flowrate": "flow_rate_m3s",
    "flow_rate": "flow_rate_m3s",
}


class DLQRecoveryEngine:
    """Production self-healing engine recovering malformed payloads from Dead-Letter Queues."""

    def __init__(self) -> None:
        self.total_processed: int = 0
        self.healed_count: int = 0
        self.unrecoverable_count: int = 0
        self.fixes_by_rule: dict[str, int] = {
            "RULE_1_NUMERIC_COERCION": 0,
            "RULE_2_LEGACY_KEY_MAPPING": 0,
            "RULE_3_ISO_TIMESTAMP_NORMALIZATION": 0,
            "RULE_4_BOUNDARY_CLIPPING": 0,
        }

    def heal_payload(self, raw_message: dict[str, Any] | str) -> dict[str, Any]:
        """Applies deterministic recovery rules to repair a corrupted or non-conforming payload."""
        self.total_processed += 1
        applied_fixes: list[str] = []

        # 0. Parse JSON string if necessary
        payload: dict[str, Any]
        if isinstance(raw_message, str):
            try:
                payload = json.loads(raw_message)
                applied_fixes.append("Parsed raw JSON string to dictionary")
            except (json.JSONDecodeError, ValueError) as err:
                self.unrecoverable_count += 1
                logger.warning("Unrecoverable non-JSON payload encountered: %s", err)
                return {
                    "healed": False,
                    "original_payload": raw_message,
                    "repaired_payload": {},
                    "applied_fixes": ["FAILED_JSON_DECODE"],
                    "error": str(err),
                    "healing_timestamp": datetime.now(UTC).isoformat(),
                }
        elif isinstance(raw_message, dict):
            payload = dict(raw_message)
        else:
            self.unrecoverable_count += 1
            return {
                "healed": False,
                "original_payload": str(raw_message),
                "repaired_payload": {},
                "applied_fixes": ["FAILED_UNSUPPORTED_TYPE"],
                "error": f"Unsupported payload type {type(raw_message)}",
                "healing_timestamp": datetime.now(UTC).isoformat(),
            }

        repaired: dict[str, Any] = {}

        # 1. Rule 2: Legacy Schema Column Key Mapping
        for key, val in payload.items():
            normalized_key = key.strip().lower()
            if normalized_key in LEGACY_KEY_MAPPING:
                target_key = LEGACY_KEY_MAPPING[normalized_key]
                repaired[target_key] = val
                applied_fixes.append(f"Mapped legacy key '{key}' -> '{target_key}'")
                self.fixes_by_rule["RULE_2_LEGACY_KEY_MAPPING"] += 1
            else:
                target_key = normalized_key
                repaired[target_key] = val
                if target_key != key:
                    applied_fixes.append(f"Normalized key '{key}' -> '{target_key}'")
                    self.fixes_by_rule["RULE_2_LEGACY_KEY_MAPPING"] += 1

        # 2. Rule 1: Numeric String Coercion
        numeric_fields = [
            "temperature",
            "ph",
            "dissolved_oxygen",
            "conductivity",
            "ammonia",
            "bod5",
            "turbidity_ntu",
            "chloride",
            "total_hardness",
            "flow_rate_m3s",
        ]
        for field in numeric_fields:
            if field in repaired and repaired[field] is not None:
                val = repaired[field]
                if isinstance(val, str):
                    clean_str = val.strip().replace(",", ".")
                    # Remove trailing units or noise like "24.5 °C"
                    match = re.search(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", clean_str)
                    if match:
                        try:
                            coerced = float(match.group(0))
                            repaired[field] = coerced
                            applied_fixes.append(f"Coerced field '{field}' from string '{val}' to float {coerced}")
                            self.fixes_by_rule["RULE_1_NUMERIC_COERCION"] += 1
                        except (ValueError, TypeError):
                            pass

        # 3. Rule 3: ISO Timestamp Normalization
        if "timestamp" in repaired and repaired["timestamp"] is not None:
            ts_val = str(repaired["timestamp"]).strip()
            # If plain date 'YYYY-MM-DD'
            if re.match(r"^\d{4}-\d{2}-\d{2}$", ts_val):
                normalized_ts = f"{ts_val}T00:00:00Z"
                repaired["timestamp"] = normalized_ts
                applied_fixes.append(f"Normalized date '{ts_val}' to ISO timestamp '{normalized_ts}'")
                self.fixes_by_rule["RULE_3_ISO_TIMESTAMP_NORMALIZATION"] += 1
            # If missing timezone 'YYYY-MM-DDTHH:MM:SS' or space separator
            elif " " in ts_val and "T" not in ts_val:
                clean_ts = ts_val.replace(" ", "T")
                if not clean_ts.endswith("Z") and "+" not in clean_ts:
                    clean_ts += "Z"
                repaired["timestamp"] = clean_ts
                applied_fixes.append(f"Formatted space-delimited timestamp '{ts_val}' to '{clean_ts}'")
                self.fixes_by_rule["RULE_3_ISO_TIMESTAMP_NORMALIZATION"] += 1
            elif not ts_val.endswith("Z") and "+" not in ts_val and "-" not in ts_val[10:]:
                clean_ts = f"{ts_val}Z"
                repaired["timestamp"] = clean_ts
                applied_fixes.append(f"Appended UTC 'Z' to naive timestamp '{ts_val}'")
                self.fixes_by_rule["RULE_3_ISO_TIMESTAMP_NORMALIZATION"] += 1
        elif "timestamp" not in repaired:
            # Fallback current time if completely missing
            now_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            repaired["timestamp"] = now_iso
            applied_fixes.append(f"Injected current UTC timestamp '{now_iso}' for missing timestamp")
            self.fixes_by_rule["RULE_3_ISO_TIMESTAMP_NORMALIZATION"] += 1

        # 4. Rule 4: Boundary Value Clipping with Audit Trail Flagging
        clipped_fields: list[str] = []
        for field, (p_min, p_max) in PHYSICAL_LIMITS.items():
            if field in repaired and repaired[field] is not None:
                try:
                    num_val = float(repaired[field])
                    # If slightly below min (e.g. DO = -0.05 within 10% margin)
                    if num_val < p_min and (p_min - num_val) <= (0.15 * (p_max - p_min)):
                        repaired[field] = p_min
                        clipped_fields.append(field)
                        applied_fixes.append(f"Clipped underflow '{field}' from {num_val} to minimum {p_min}")
                        self.fixes_by_rule["RULE_4_BOUNDARY_CLIPPING"] += 1
                    # If slightly above max (e.g. pH = 14.05 within 10% margin)
                    elif num_val > p_max and (num_val - p_max) <= (0.15 * (p_max - p_min)):
                        repaired[field] = p_max
                        clipped_fields.append(field)
                        applied_fixes.append(f"Clipped overflow '{field}' from {num_val} to maximum {p_max}")
                        self.fixes_by_rule["RULE_4_BOUNDARY_CLIPPING"] += 1
                except (ValueError, TypeError):
                    pass

        if clipped_fields:
            repaired["is_clipped"] = True
            repaired["clipped_parameters"] = clipped_fields

        # Ensure station_id exists
        if "station_id" not in repaired or not repaired["station_id"]:
            repaired["station_id"] = "ST_UNKNOWN_RECOVERED"
            applied_fixes.append("Assigned default station_id 'ST_UNKNOWN_RECOVERED'")

        self.healed_count += 1

        return {
            "healed": True,
            "original_payload": raw_message,
            "repaired_payload": repaired,
            "applied_fixes": applied_fixes,
            "healing_timestamp": datetime.now(UTC).isoformat(),
        }

    def get_recovery_audit_stats(self) -> dict[str, Any]:
        """Returns cumulative DLQ self-healing audit metrics."""
        heal_rate = (self.healed_count / max(1, self.total_processed)) * 100.0
        return {
            "total_processed": self.total_processed,
            "healed_count": self.healed_count,
            "unrecoverable_count": self.unrecoverable_count,
            "heal_rate_pct": round(heal_rate, 2),
            "fixes_by_rule": self.fixes_by_rule,
        }
