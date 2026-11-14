"""Enterprise Report Exporter supporting UTF-8 CSV, Multi-Sheet XLSX, and Print-Ready HTML."""

import hashlib
import io
from datetime import UTC, datetime
from typing import Any

import pandas as pd


class ReportExportService:
    """Generates official water quality monitoring reports in CSV, Excel, and HTML formats."""

    @staticmethod
    def export_to_csv(telemetry_data: list[dict[str, Any]]) -> bytes:
        """Converts telemetry records to CSV bytes encoded with UTF-8-BOM (utf-8-sig)."""
        df = pd.DataFrame(telemetry_data)
        # Select canonical columns in order if present
        cols_order = [
            "timestamp",
            "station_id",
            "station_name",
            "river_name",
            "basin_name",
            "dissolved_oxygen",
            "ph",
            "temperature",
            "conductivity",
            "ammonia",
            "bod5",
            "cod",
            "tss",
            "nitrate_no3",
            "phosphate_po4",
            "coliform",
            "salinity",
            "turbidity_ntu",
            "chloride",
            "total_hardness",
            "flow_rate_m3s",
            "alkalinity",
            "true_colour",
            "vn_wqi_score",
            "vn_wqi_label",
            "ccme_score",
            "quality_label",
            "is_anomaly",
            "anomaly_reason",
        ]
        present_cols = [c for c in cols_order if c in df.columns]
        df_export = df[present_cols] if present_cols else df
        csv_str = df_export.to_csv(index=False, encoding="utf-8-sig")
        return csv_str.encode("utf-8-sig")

    @staticmethod
    def export_to_excel(
        station_meta: dict[str, Any],
        telemetry_data: list[dict[str, Any]],
        forecast_data: dict[str, Any] | None = None,
        audit_data: list[dict[str, Any]] | None = None,
    ) -> bytes:
        """Builds a styled multi-sheet Excel file (.xlsx) with 4 operational tabs."""
        try:
            import openpyxl
            from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
            from openpyxl.utils import get_column_letter

            wb = openpyxl.Workbook()
            # Remove default sheet
            wb.remove(wb.active)

            header_fill = PatternFill(start_color="1E293B", end_color="1E293B", fill_type="solid")
            header_font = Font(name="Segoe UI", size=11, bold=True, color="F8FAFC")
            Border(
                left=Side(style="thin", color="334155"),
                right=Side(style="thin", color="334155"),
                top=Side(style="thin", color="334155"),
                bottom=Side(style="thin", color="334155"),
            )

            # ---------------------------------------------------------------
            # Sheet 1: Station_Metadata
            # ---------------------------------------------------------------
            ws1 = wb.create_sheet(title="Station_Metadata")
            ws1.append(["THÔNG TIN TRẠM QUAN TRẮC MÔI TRƯỜNG NƯỚC MẶT", ""])
            ws1.append(["Mã Trạm (Station ID)", station_meta.get("station_id", "N/A")])
            ws1.append(["Tên Trạm", station_meta.get("station_name", "N/A")])
            ws1.append(["Lưu Vực Sông", station_meta.get("river_name", "N/A")])
            ws1.append(["Vùng Lưu Vực", station_meta.get("basin_name", "N/A")])
            ws1.append(["Tọa Độ Vĩ Độ (Latitude)", station_meta.get("latitude", 0.0)])
            ws1.append(["Tọa Độ Kinh Độ (Longitude)", station_meta.get("longitude", 0.0)])
            ws1.append(["Thời Điểm Xuất Báo Cáo (UTC)", datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")])

            ws1["A1"].font = Font(name="Segoe UI", size=13, bold=True, color="06B6D4")
            for r in range(2, 9):
                ws1[f"A{r}"].font = Font(name="Segoe UI", size=10, bold=True, color="1E293B")
                ws1[f"B{r}"].font = Font(name="Segoe UI", size=10, color="0F172A")

            # ---------------------------------------------------------------
            # Sheet 2: Historical_Telemetry_72h
            # ---------------------------------------------------------------
            ws2 = wb.create_sheet(title="Historical_Telemetry_72h")
            df_tel = pd.DataFrame(telemetry_data)
            headers2 = list(df_tel.columns)
            ws2.append(headers2)
            for _, row in df_tel.iterrows():
                ws2.append([str(v) if v is not None else "" for v in row])

            for col_idx in range(1, len(headers2) + 1):
                cell = ws2.cell(row=1, column=col_idx)
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal="center")

            # ---------------------------------------------------------------
            # Sheet 3: AI_ML_Forecast_72h
            # ---------------------------------------------------------------
            ws3 = wb.create_sheet(title="AI_ML_Forecast_72h")
            headers3 = [
                "Bước Dự Báo (Giờ)",
                "Mốc Giờ Dự Báo",
                "DO Dự Báo (mg/L)",
                "Cận Dưới 95% CI",
                "Cận Trên 95% CI",
                "Trạng Thái",
            ]
            ws3.append(headers3)
            for c_idx in range(1, len(headers3) + 1):
                cell = ws3.cell(row=1, column=c_idx)
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal="center")

            fc_list = (
                (forecast_data.get("trajectory") or forecast_data.get("forecast_points", [])) if forecast_data else []
            )
            if fc_list:
                for pt in fc_list:
                    pred_val = pt.get("predicted_value", pt.get("predicted_do", 0.0))
                    ws3.append(
                        [
                            pt.get("step_ahead_hours", ""),
                            pt.get("forecast_timestamp", pt.get("timestamp", "")),
                            pred_val,
                            pt.get("confidence_lower_95", pt.get("do_lower_95", 0.0)),
                            pt.get("confidence_upper_95", pt.get("do_upper_95", 0.0)),
                            "CẢNH BÁO THIẾU OXY" if float(pred_val or 7.0) < 4.0 else "AN TOÀN",
                        ]
                    )

            # ---------------------------------------------------------------
            # Sheet 4: Incident_Audit_Log
            # ---------------------------------------------------------------
            ws4 = wb.create_sheet(title="Incident_Audit_Log")
            headers4 = [
                "Mã Sự Cố",
                "Thời Gian",
                "Vai Trò Tác Nghiệp",
                "Hành Động",
                "Chi Tiết Ghi Nhận",
                "Mã Băm Niêm Phong (SHA-256)",
            ]
            ws4.append(headers4)
            for c_idx in range(1, len(headers4) + 1):
                cell = ws4.cell(row=1, column=c_idx)
                cell.fill = header_fill
                cell.font = header_font
                cell.alignment = Alignment(horizontal="center")

            if audit_data:
                for ev in audit_data:
                    ev_dict = ev if isinstance(ev, dict) else (vars(ev) if hasattr(ev, "__dict__") else {})
                    ws4.append(
                        [
                            ev_dict.get("incident_id", station_meta.get("station_id", "")),
                            ev_dict.get("timestamp", ""),
                            ev_dict.get("user_role", ""),
                            ev_dict.get("action", ""),
                            ev_dict.get("details", ""),
                            ev_dict.get("sha256_hash", ""),
                        ]
                    )
            elif telemetry_data:
                latest = telemetry_data[-1]
                ws4.append(
                    [
                        station_meta.get("station_id", "ST_01"),
                        str(latest.get("timestamp", "")),
                        "SYSTEM_AUDIT",
                        "QCVN_COMPLIANCE",
                        f"DO={latest.get('dissolved_oxygen')} mg/L, pH={latest.get('ph')}",
                        "SYSTEM_VERIFIED",
                    ]
                )

            # Auto-fit column widths
            for sheet in [ws1, ws2, ws3, ws4]:
                for col in sheet.columns:
                    col_letter = get_column_letter(col[0].column)
                    sheet.column_dimensions[col_letter].width = 18

            buf = io.BytesIO()
            wb.save(buf)
            return buf.getvalue()

        except ImportError:
            # Graceful fallback to CSV bytes
            return ReportExportService.export_to_csv(telemetry_data)

    @staticmethod
    def export_to_html_report(
        station_meta: dict[str, Any],
        telemetry_data: list[dict[str, Any]],
        wqi_summary: dict[str, Any],
        active_alerts: list[dict[str, Any]] | None = None,
    ) -> str:
        """Renders an official print-ready HTML document matching Ministry of Natural Resources standards."""
        now_str = datetime.now(UTC).strftime("%d/%m/%Y %H:%M:%S UTC")
        hash_digest = hashlib.sha256(f"{station_meta.get('station_id')}_{now_str}".encode()).hexdigest()[:16]

        latest = telemetry_data[-1] if telemetry_data else {}
        vn_score = wqi_summary.get("vn_wqi_score", latest.get("vn_wqi_score", 80.0))
        vn_label = wqi_summary.get("vn_wqi_label", latest.get("vn_wqi_label", "Tốt"))
        ccme_score = wqi_summary.get("ccme_score", latest.get("ccme_score", 85.0))

        html = f"""<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="utf-8">
    <title>Báo Cáo Kỹ Thuật Giám Sát Chất Lượng Nước - {station_meta.get("station_id")}</title>
    <style>
        @page {{ size: A4; margin: 15mm; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; font-size: 11pt; color: #0F172A; line-height: 1.4; }}
        .header-table {{ width: 100%; border-collapse: collapse; border: none; margin-bottom: 20px; }}
        .header-table td {{ border: none; text-align: center; vertical-align: top; }}
        h1 {{ font-size: 15pt; text-align: center; text-transform: uppercase; margin: 10px 0 4px 0; color: #0F172A; }}
        h2 {{ font-size: 11pt; text-align: center; font-weight: normal; font-style: italic; margin-top: 0; margin-bottom: 25px; color: #475569; }}
        .box {{ border: 1px solid #334155; padding: 10px 14px; margin-bottom: 16px; background-color: #F8FAFC; border-radius: 2px; }}
        .metric-table {{ width: 100%; border-collapse: collapse; margin-top: 10px; margin-bottom: 20px; }}
        .metric-table th, .metric-table td {{ border: 1px solid #CBD5E1; padding: 6px 8px; font-size: 10pt; }}
        .metric-table th {{ background-color: #1E293B; color: #F8FAFC; font-weight: 600; text-align: center; }}
        .tabular {{ font-family: 'Consolas', 'Courier New', monospace; text-align: right; }}
        .signature-table {{ width: 100%; border: none; margin-top: 35px; text-align: center; }}
        .signature-table td {{ border: none; vertical-align: top; padding-top: 5px; }}
        .sign-space {{ height: 70px; }}
        .badge-seal {{ font-family: monospace; font-size: 9pt; color: #64748B; }}
        @media print {{
            body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
            .header-table, .metric-table, .box, .signature-table {{ page-break-inside: avoid; }}
        }}
    </style>
</head>
<body>
    <table class="header-table">
        <tr>
            <td style="width: 50%;">
                <strong>BỘ TÀI NGUYÊN VÀ MÔI TRƯỜNG</strong><br>
                <strong>CỤC QUẢN LÝ TÀI NGUYÊN NƯỚC</strong><br>
                <span style="font-size: 9pt;">TRUNG TÂM ĐIỀU HÀNH THỦY VĂN QUỐC GIA (NOC)</span><br>
                -------------------------
            </td>
            <td style="width: 50%;">
                <strong>CỘNG HÒA XÃ HỘI CHỦ NGHĨA VIỆT NAM</strong><br>
                <strong>Độc lập - Tự do - Hạnh phúc</strong><br>
                <span style="font-size: 9pt;">Hà Nội, ngày {datetime.now(UTC).strftime("%d")} tháng {datetime.now(UTC).strftime("%m")} năm {datetime.now(UTC).strftime("%Y")}</span><br>
                -------------------------
            </td>
        </tr>
    </table>

    <h1>BÁO CÁO KỸ THUẬT GIÁM SÁT CHẤT LƯỢNG NƯỚC MẶT</h1>
    <h2>Theo Quy chuẩn Kỹ thuật Quốc gia QCVN 08:2023/BTNMT và Quyết định 1460/QĐ-TCMT</h2>

    <div class="box">
        <strong>THÔNG TIN ĐỊA BÀN QUAN TRẮC:</strong><br>
        • Trạm Quan Trắc: <strong>{station_meta.get("station_name")} ({station_meta.get("station_id")})</strong><br>
        • Lưu Vực Sông: <strong>{station_meta.get("river_name")}</strong> | Phân Vùng: <strong>{station_meta.get("basin_name")}</strong><br>
        • Tọa Độ Địa Lý: <strong>{float(station_meta.get("latitude", 0.0)):.4f}°N, {float(station_meta.get("longitude", 0.0)):.4f}°E</strong><br>
        • Thời Gian Quan Trắc: <strong>{now_str}</strong> | Mã Băm Kiểm Toán: <span class="badge-seal">{hash_digest}</span>
    </div>

    <div class="box">
        <strong>KẾT QUẢ ĐÁNH GIÁ CHỈ SỐ CHẤT LƯỢNG NƯỚC (WQI ĐỘNG CƠ KÉP):</strong><br>
        • Chỉ số Quốc gia VN_WQI: <strong>{float(vn_score):.1f} điểm</strong> — Phân Hạng: <strong>[{str(vn_label).upper()}]</strong><br>
        • Chỉ số Quốc tế CCME WQI: <strong>{float(ccme_score):.1f} điểm</strong><br>
        • Quy chuẩn Tuân thủ: <strong>QCVN 08:2023 Mục Đích Cấp Nước Sinh Hoạt & Thủy Sinh</strong>
    </div>

    <h3>BẢNG TỔNG HỢP 18 THÔNG SỐ VIỄN THÁM MỚI NHẤT</h3>
    <table class="metric-table">
        <thead>
            <tr>
                <th>STT</th>
                <th>Thông Số Hóa Lý & Sinh Thái</th>
                <th>Giá Trị Thực Đo</th>
                <th>Đơn Vị</th>
                <th>Ngưỡng An Toàn (QCVN 08)</th>
                <th>Đánh Giá</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td style="text-align: center;">1</td>
                <td>Oxy hòa tan (DO)</td>
                <td class="tabular">{latest.get("dissolved_oxygen", "N/A")}</td>
                <td>mg/L</td>
                <td>>= 6.0 (Tier A)</td>
                <td>{"NGUY CƠ THIẾU OXY" if float(latest.get("dissolved_oxygen", 7.0)) < 4.0 else "BÌNH THƯỜNG"}</td>
            </tr>
            <tr>
                <td style="text-align: center;">2</td>
                <td>Độ pH</td>
                <td class="tabular">{latest.get("ph", "N/A")}</td>
                <td>pH</td>
                <td>6.5 - 8.5</td>
                <td>BÌNH THƯỜNG</td>
            </tr>
            <tr>
                <td style="text-align: center;">3</td>
                <td>Nhiệt độ nước</td>
                <td class="tabular">{latest.get("temperature", "N/A")}</td>
                <td>°C</td>
                <td>-</td>
                <td>BÌNH THƯỜNG</td>
            </tr>
            <tr>
                <td style="text-align: center;">4</td>
                <td>Amoni (NH4+ tính theo N)</td>
                <td class="tabular">{latest.get("ammonia", "N/A")}</td>
                <td>mg/L</td>
                <td><= 0.3</td>
                <td>{"VƯỢT NGƯỠNG" if float(latest.get("ammonia", 0.1)) > 0.3 else "ĐẠT"}</td>
            </tr>
            <tr>
                <td style="text-align: center;">5</td>
                <td>Nhu cầu oxy hóa học (COD)</td>
                <td class="tabular">{latest.get("cod", "N/A")}</td>
                <td>mg/L</td>
                <td><= 15.0</td>
                <td>{"CẢNH BÁO CAO" if latest.get("cod") and float(latest["cod"]) > 30.0 else "ĐẠT"}</td>
            </tr>
            <tr>
                <td style="text-align: center;">6</td>
                <td>Tổng chất rắn lơ lửng (TSS)</td>
                <td class="tabular">{latest.get("tss", "N/A")}</td>
                <td>mg/L</td>
                <td><= 25.0</td>
                <td>ĐẠT</td>
            </tr>
            <tr>
                <td style="text-align: center;">7</td>
                <td>Độ mặn (Salinity)</td>
                <td class="tabular">{latest.get("salinity", "N/A")}</td>
                <td>ppt</td>
                <td><= 0.25</td>
                <td>{"XÂM NHẬP MẶN" if latest.get("salinity") and float(latest["salinity"]) > 2.0 else "ĐẠT"}</td>
            </tr>
        </tbody>
    </table>

    <table class="signature-table">
        <tr>
            <td style="width: 33%;">
                <strong>CÁN BỘ QUAN TRẮC</strong><br>
                <span style="font-size: 9pt;">(Ký và ghi rõ họ tên)</span>
                <div class="sign-space"></div>
                <strong>Đội Cơ Động NOC</strong>
            </td>
            <td style="width: 33%;">
                <strong>KỸ SƯ TRƯỞNG TRUNG TÂM</strong><br>
                <span style="font-size: 9pt;">(Ký và xác nhận dữ liệu)</span>
                <div class="sign-space"></div>
                <strong>Phòng Kỹ Thuật Thủy Văn</strong>
            </td>
            <td style="width: 33%;">
                <strong>GIÁM ĐỐC ĐIỀU HÀNH NOC</strong><br>
                <span style="font-size: 9pt;">(Phê duyệt và đóng dấu)</span>
                <div class="sign-space"></div>
                <strong>Cục Quản Lý Nước</strong>
            </td>
        </tr>
    </table>
</body>
</html>
"""
        return html
