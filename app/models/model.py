from dataclasses import dataclass

import structlog
from pydantic import BaseModel

logger = structlog.get_logger(__name__)


# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class WorkforceRecord(BaseModel):
    """One employee's assignment on a CT project."""
    empl_id: str
    proj_id: str
    proj_name: str
    bill_lab_cat_cd: str  # from the DFLT_FL=Y row


class EmployeeRecord(BaseModel):
    """Costpoint employee details needed for Connecteam user creation."""
    empl_id: str
    first_name: str
    last_name: str
    home_email_id: str
    orig_hire_dt: str   # ISO string e.g. "2011-07-11T00:00:00"
    birth_dt: str       # ISO string
    is_active: bool     # True when S_EMPL_STATUS_CD == "ACT"


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class DeltekConfig:
    """Configuration for Deltek/Costpoint API connection."""
    base_url: str
    system: str
    company: str
    username: str
    password: str
    filter_notes_value: str = "CT"

    @classmethod
    def from_env(cls, secrets: dict) -> "DeltekConfig":
        return cls(
            base_url=secrets.get("base_url", ""),
            system=secrets.get("system", ""),
            company=str(secrets.get("cp_company", "")),
            username=secrets.get("username", ""),
            password=secrets.get("password", ""),
            filter_notes_value=secrets.get("filter_notes_value", "CT"),
        )

    @property
    def full_url(self) -> str:
        return f"{self.base_url}?system={self.system}&company={self.company}"

    def validate(self) -> bool:
        return all([self.base_url, self.system, self.company, self.username, self.password])


@dataclass
class ConnecteamConfig:
    """Configuration for Connecteam API connection."""
    api_key: str
    users_base_url: str = "https://api.connecteam.com/users/v1/users"

    @classmethod
    def from_env(cls, secrets: dict) -> "ConnecteamConfig":
        api_key = secrets.get("key", "")
        if not api_key:
            raise ValueError("Connecteam API key not found in secrets")
        return cls(api_key=api_key)
