"""Session State Management and RBAC Configuration Package."""

from src.dashboard.state.session_state import (
    ROLE_REGISTRY,
    AuditEvent,
    IncidentRecord,
    IncidentSeverity,
    IncidentStatus,
    RoleProfile,
    SessionStateManager,
    UserRole,
    add_audit_event,
    safe_fragment,
)

__all__ = [
    "ROLE_REGISTRY",
    "AuditEvent",
    "IncidentRecord",
    "IncidentSeverity",
    "IncidentStatus",
    "RoleProfile",
    "SessionStateManager",
    "UserRole",
    "add_audit_event",
    "safe_fragment",
]
