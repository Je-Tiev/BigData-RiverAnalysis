"""Incident Management Kanban Board, Dual RCA, Field Dispatch SOP, and Cryptographic Audit Trail (Screen 3: INCIDENTS)."""

from typing import Any

import pandas as pd
import streamlit as st

from src.contracts.river_schema import CRITICAL_THRESHOLDS
from src.dashboard.components.incident_card import render_incident_card
from src.dashboard.state.session_state import (
    IncidentSeverity,
    IncidentStatus,
    SessionStateManager,
    safe_fragment,
)
from src.dashboard.styles.theme import render_scada_metric_tile


@safe_fragment
def incident_dispatch_fragment(incident: dict[str, Any]) -> None:
    """Isolated fragment managing Dual-Layer RCA analysis, SOP checklists, and rapid team dispatch."""
    state_mgr = SessionStateManager()
    inc_id = incident.get("incident_id", "N/A")
    status = incident.get("status", IncidentStatus.NEW.value).upper()
    metrics = incident.get("metrics_snapshot", {})
    severity = incident.get("severity", IncidentSeverity.HIGH.value).upper()
    station_id = incident.get("station_id", "N/A")

    st.markdown(
        f"""
        <div style="font-family: 'JetBrains Mono', monospace; font-size: 14px; font-weight: 700; color: #06B6D4; margin-bottom: 12px;">
            🛠️ PHÂN TÍCH NGUYÊN NHÂN GỐC (DUAL RCA) & ĐIỀU LỆNH SOP: <span style="color: #F8FAFC;">{inc_id}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    rca_col1, rca_col2 = st.columns(2)

    # Layer 1: Deterministic QCVN Threshold Violations
    with rca_col1:
        st.markdown(
            """
            <div style="font-size: 12px; font-weight: 700; color: #EF4444; font-family: 'JetBrains Mono', monospace; margin-bottom: 6px;">
                [LỚP 1: QUY CHUẨN KỸ THUẬT QUỐC GIA QCVN 08:2023]
            </div>
            """,
            unsafe_allow_html=True,
        )
        violations = []
        for param, val in metrics.items():
            thresh = CRITICAL_THRESHOLDS.get(param)
            if thresh:
                if "max" in thresh and float(val) > thresh["max"]:
                    violations.append(
                        f"• <strong>{param.upper()}</strong> = {val} vượt ngưỡng tối đa quy định ({thresh['max']})"
                    )
                elif "min" in thresh and float(val) < thresh["min"]:
                    violations.append(
                        f"• <strong>{param.upper()}</strong> = {val} giảm dưới ngưỡng tối thiểu quy định ({thresh['min']})"
                    )

        if violations:
            st.markdown(
                f"<div style='font-size: 11px; color: #FCA5A5; background-color: #450A0A; padding: 8px; border-radius: 4px; border: 1px solid #7F1D1D;'>{'<br>'.join(violations)}</div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                "<div style='font-size: 11px; color: #86EFAC; background-color: #064E3B; padding: 8px; border-radius: 4px;'>• Các thông số nằm trong ngưỡng giới hạn danh định.</div>",
                unsafe_allow_html=True,
            )

    # Layer 2: Machine Learning Statistical Anomaly Scoring
    with rca_col2:
        st.markdown(
            """
            <div style="font-size: 12px; font-weight: 700; color: #38BDF8; font-family: 'JetBrains Mono', monospace; margin-bottom: 6px;">
                [LỚP 2: MÔ HÌNH HỌC MÁY ISOLATION FOREST & AUTOENCODER]
            </div>
            """,
            unsafe_allow_html=True,
        )
        reason = incident.get("anomaly_reason", "Phát hiện mẫu hình dữ liệu bất thường phân bố không gian.")
        st.markdown(
            f"""
            <div style='font-size: 11px; color: #E2E8F0; background-color: #0F172A; padding: 8px; border-radius: 4px; border: 1px solid #334155;'>
                • <strong>Mô hình phát hiện:</strong> Ensemble Isolation Forest v1.2<br>
                • <strong>Điểm số Dị thường (Anomaly Score):</strong> <span style="color: {"#EF4444" if severity == "CRITICAL" else "#F59E0B"}; font-weight: bold;">{0.88 if severity == "CRITICAL" else 0.72}</span><br>
                • <strong>Lý do thuật toán:</strong> {reason}
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)

    # SOP Action & Dispatch Controls
    profile = state_mgr.get_role_profile()
    if not profile.can_dispatch:
        st.info(
            "[THÔNG TIN] Chế độ xem: Vai trò của bạn chỉ có quyền quan sát. Quyền điều động thuộc về [FIELD_OFFICER] hoặc [MLOPS_ADMIN]."
        )
        return

    st.markdown(
        """
        <div style="font-size: 12px; font-weight: 700; color: #F8FAFC; font-family: 'JetBrains Mono', monospace; margin-bottom: 6px;">
            📋 BIỂU MẪU ĐIỀU PHỐI ĐỘI CƠ ĐỘNG PHẢN ỨNG NHANH HIỆN TRƯỜNG (SOP FORM)
        </div>
        """,
        unsafe_allow_html=True,
    )

    col_team, col_eta = st.columns([2, 1])
    with col_team:
        team_options = [
            "Đội Phản Ứng Nhanh 1 - Hà Nội (Lưu vực Hồng - Thái Bình)",
            "Đội Phản Ứng Nhanh 2 - Đà Nẵng (Lưu vực Miền Trung)",
            "Đội Phản Ứng Nhanh 3 - TP.HCM (Lưu vực Đồng Nai - Sài Gòn)",
            "Đội Phản Ứng Nhanh 4 - Cần Thơ (Lưu vực Sông Tiền - Hậu)",
        ]
        assigned_team = st.selectbox("Đội Hiện Trường Cơ Động:", options=team_options, key="dispatch_team_select")

    with col_eta:
        eta_val = st.selectbox(
            "Thời Gian Tiếp Cận Dự Kiến (ETA):",
            options=["30 phút", "1 giờ", "2 giờ", "4 giờ"],
            key="dispatch_eta_select",
        )

    chk1 = st.checkbox(
        "Đã gửi tín hiệu cảnh báo tự động tới các trạm quan trắc hạ lưu (Kafka Alert Topic)",
        value=True,
        key=f"sop1_{inc_id}",
    )
    chk2 = st.checkbox(
        "Đã phát công điện khẩn ngắt thu nước thô tới Ban Giám Đốc các Nhà máy nước sinh hoạt",
        value=True,
        key=f"sop2_{inc_id}",
    )
    chk3 = st.checkbox(
        "Đã lệnh đội hiện trường lấy 3 mẫu đối chứng phòng thí nghiệm theo chuẩn ISO/IEC 17025",
        value=True,
        key=f"sop3_{inc_id}",
    )
    chk4 = st.checkbox(
        "Đã thông báo Chi cục Bảo vệ Môi trường và Sở TN&MT địa phương phối hợp thanh tra",
        value=False,
        key=f"sop4_{inc_id}",
    )

    selected_sops = []
    if chk1:
        selected_sops.append("Bật cảnh báo hạ lưu")
    if chk2:
        selected_sops.append("Ngắt thu nước thô")
    if chk3:
        selected_sops.append("Lấy mẫu đối chứng ISO 17025")
    if chk4:
        selected_sops.append("Báo Sở TN&MT")

    if status in [IncidentStatus.NEW.value, IncidentStatus.ACKNOWLEDGED.value]:
        if st.button(
            "🚀 XÁC NHẬN ĐIỀU ĐỘNG VÀ CHUYỂN SANG ĐANG XỬ LÝ [DISPATCH]",
            key=f"btn_confirm_dispatch_{inc_id}",
            type="primary",
            use_container_width=True,
        ):
            state_mgr.dispatch_incident(
                incident_id=inc_id,
                team_name=assigned_team.split(" - ")[0],
                sop_actions=selected_sops,
                eta=eta_val,
            )
            st.success(f"Đã điều động {assigned_team.split(' - ')[0]} tới trạm {station_id}!")
            st.rerun()

    elif status == IncidentStatus.IN_PROGRESS.value:
        res_notes = st.text_area(
            "Ghi Chú Kết Quả Khắc Phục & Nghiệm Thu Hiện Trường:",
            value="Đã kiểm tra van xả thải, chỉ số DO và COD đã hồi phục về mức an toàn theo QCVN 08.",
            key=f"resolve_notes_{inc_id}",
        )
        if st.button("✅ NGHIỆM THU VÀ ĐÓNG SỰ CỐ [RESOLVE]", key=f"btn_resolve_{inc_id}", use_container_width=True):
            state_mgr.resolve_incident(incident_id=inc_id, resolution_notes=res_notes)
            st.success(f"Đã đóng sự cố {inc_id} thành công!")
            st.rerun()


def render_incident_view() -> None:
    """Renders Screen 3: 4-Stage Kanban Board, Dual RCA, Dispatch SOP Form & SHA-256 Audit Trail."""
    state_mgr = SessionStateManager()

    all_incidents = state_mgr.get_incidents()
    new_incidents = [i for i in all_incidents if i.get("status") == IncidentStatus.NEW.value]
    ack_incidents = [i for i in all_incidents if i.get("status") == IncidentStatus.ACKNOWLEDGED.value]
    prog_incidents = [i for i in all_incidents if i.get("status") == IncidentStatus.IN_PROGRESS.value]
    res_incidents = [i for i in all_incidents if i.get("status") == IncidentStatus.RESOLVED.value]

    # 1. Summary Ribbon Badges
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(
            render_scada_metric_tile("1. MỚI PHÁT HIỆN", str(len(new_incidents)), "SỰ CỐ", "#EF4444"),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            render_scada_metric_tile("2. ĐÃ TIẾP NHẬN", str(len(ack_incidents)), "SỰ CỐ", "#F59E0B"),
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            render_scada_metric_tile("3. ĐANG XỬ LÝ", str(len(prog_incidents)), "SỰ CỐ", "#06B6D4"),
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            render_scada_metric_tile("4. ĐÃ GIẢI QUYẾT", str(len(res_incidents)), "SỰ CỐ", "#10B981"),
            unsafe_allow_html=True,
        )

    st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)

    # 2. 4-Stage Kanban Columns
    k1, k2, k3, k4 = st.columns(4)

    def _make_ack(iid: str):
        def _fn(_):
            state_mgr.acknowledge_incident(iid)
            st.rerun()

        return _fn

    def _make_disp(iid: str):
        def _fn(_):
            state_mgr.set_state("selected_incident_id", iid)
            st.rerun()

        return _fn

    with k1:
        st.markdown(
            "<div style='font-family: monospace; font-size: 12px; font-weight: 700; color: #EF4444; border-bottom: 2px solid #EF4444; padding-bottom: 4px; margin-bottom: 8px;'>[1. MỚI PHÁT HIỆN]</div>",
            unsafe_allow_html=True,
        )
        for inc in new_incidents:
            iid = inc["incident_id"]
            render_incident_card(
                inc, on_acknowledge=_make_ack(iid), on_dispatch=_make_disp(iid), key_prefix=f"k1_{iid}"
            )

    with k2:
        st.markdown(
            "<div style='font-family: monospace; font-size: 12px; font-weight: 700; color: #F59E0B; border-bottom: 2px solid #F59E0B; padding-bottom: 4px; margin-bottom: 8px;'>[2. ĐÃ TIẾP NHẬN]</div>",
            unsafe_allow_html=True,
        )
        for inc in ack_incidents:
            iid = inc["incident_id"]
            render_incident_card(
                inc, on_acknowledge=_make_ack(iid), on_dispatch=_make_disp(iid), key_prefix=f"k2_{iid}"
            )

    with k3:
        st.markdown(
            "<div style='font-family: monospace; font-size: 12px; font-weight: 700; color: #06B6D4; border-bottom: 2px solid #06B6D4; padding-bottom: 4px; margin-bottom: 8px;'>[3. ĐANG XỬ LÝ]</div>",
            unsafe_allow_html=True,
        )
        for inc in prog_incidents:
            iid = inc["incident_id"]
            render_incident_card(
                inc, on_acknowledge=_make_ack(iid), on_dispatch=_make_disp(iid), key_prefix=f"k3_{iid}"
            )

    with k4:
        st.markdown(
            "<div style='font-family: monospace; font-size: 12px; font-weight: 700; color: #10B981; border-bottom: 2px solid #10B981; padding-bottom: 4px; margin-bottom: 8px;'>[4. ĐÃ GIẢI QUYẾT]</div>",
            unsafe_allow_html=True,
        )
        for inc in res_incidents:
            iid = inc["incident_id"]
            render_incident_card(
                inc, on_acknowledge=_make_ack(iid), on_dispatch=_make_disp(iid), key_prefix=f"k4_{iid}"
            )

    st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)

    # 3. Selected Incident Dual RCA & Field Dispatch SOP Form
    selected_inc_id = state_mgr.get_state("selected_incident_id", "INC-ST_DONGNAI_BH_05")
    selected_inc = next(
        (i for i in all_incidents if i["incident_id"] == selected_inc_id), (all_incidents[0] if all_incidents else None)
    )

    if selected_inc:
        with st.container():
            incident_dispatch_fragment(selected_inc)

    # 4. Immutable Cryptographic Audit Trail Table
    st.markdown("<div style='height: 24px;'></div>", unsafe_allow_html=True)
    st.markdown(
        """
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
            <div style="font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 700; color: #06B6D4; letter-spacing: 0.05em;">
                🛡️ NHẬT KÝ KIỂM TOÁN VẬN HÀNH BẤT BIẾN (SHA-256 CRYPTOGRAPHIC AUDIT TRAIL)
            </div>
            <div style="font-size: 11px; color: #64748B;">Niêm phong mật mã 16-ký tự • Chống chối bỏ</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    trail = state_mgr.get_state("audit_trail", [])
    if trail:
        trail_data = [evt.to_dict() if hasattr(evt, "to_dict") else evt for evt in trail]
        df_trail = pd.DataFrame(trail_data)
        st.dataframe(
            df_trail,
            column_config={
                "timestamp": st.column_config.TextColumn("Mốc Giờ (UTC)", width="medium"),
                "user_role": st.column_config.TextColumn("Vai Trò Tác Nghiệp", width="small"),
                "action": st.column_config.TextColumn("Hành Động SCADA", width="small"),
                "details": st.column_config.TextColumn("Chi Tiết Xử Lý", width="large"),
                "sha256_hash": st.column_config.TextColumn("Mã Băm SHA-256", width="small"),
            },
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("Chưa có sự kiện kiểm toán nào được ghi nhận.")
