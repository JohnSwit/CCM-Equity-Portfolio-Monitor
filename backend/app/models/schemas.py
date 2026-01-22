from pydantic import BaseModel, EmailStr, Field, validator
from typing import Optional, List, Dict, Any
from datetime import date, datetime
from enum import Enum


# Auth schemas
class UserLogin(BaseModel):
    email: EmailStr
    password: str


class Token(BaseModel):
    access_token: str
    token_type: str


class UserResponse(BaseModel):
    id: int
    email: str
    full_name: Optional[str]
    is_active: bool
    is_admin: bool

    class Config:
        from_attributes = True


# Account schemas
class AccountResponse(BaseModel):
    id: int
    account_number: str
    display_name: str

    class Config:
        from_attributes = True


# Group schemas
class GroupType(str, Enum):
    family = "family"
    estate = "estate"
    custom = "custom"
    firm = "firm"


class GroupCreate(BaseModel):
    name: str
    type: GroupType


class GroupResponse(BaseModel):
    id: int
    name: str
    type: str
    member_count: Optional[int] = 0

    class Config:
        from_attributes = True


class GroupMemberAdd(BaseModel):
    account_ids: List[int]


# Basket schemas
class BasketConstituentInput(BaseModel):
    symbol: str
    weight: float


class BasketCreate(BaseModel):
    code: str
    name: str
    constituents: List[BasketConstituentInput]

    @validator('constituents')
    def validate_weights_sum(cls, v):
        total = sum(c.weight for c in v)
        if abs(total - 1.0) > 0.001:
            raise ValueError('Constituent weights must sum to 1.0')
        return v


class BasketUpdate(BaseModel):
    name: Optional[str]
    constituents: Optional[List[BasketConstituentInput]]

    @validator('constituents')
    def validate_weights_sum(cls, v):
        if v is not None:
            total = sum(c.weight for c in v)
            if abs(total - 1.0) > 0.001:
                raise ValueError('Constituent weights must sum to 1.0')
        return v


class BasketResponse(BaseModel):
    id: int
    code: str
    name: str
    constituents: List[Dict[str, Any]]

    class Config:
        from_attributes = True


# Import schemas
class ImportPreviewRow(BaseModel):
    row_num: int
    data: Dict[str, Any]
    errors: List[str] = []


class ImportPreviewResponse(BaseModel):
    total_rows: int
    preview_rows: List[ImportPreviewRow]
    detected_mappings: Dict[str, str]
    has_errors: bool


class ImportCommitResponse(BaseModel):
    import_log_id: int
    status: str
    rows_processed: int
    rows_imported: int
    rows_error: int
    errors: List[Dict[str, Any]]


# Analytics schemas
class ViewType(str, Enum):
    account = "account"
    group = "group"
    firm = "firm"


class SummaryResponse(BaseModel):
    view_type: str
    view_id: int
    view_name: str
    total_value: float
    as_of_date: date
    data_last_updated: datetime
    return_1m: Optional[float]
    return_3m: Optional[float]
    return_ytd: Optional[float]
    return_1y: Optional[float]
    return_3y: Optional[float]
    return_inception: Optional[float]


class ReturnDataPoint(BaseModel):
    date: date
    return_value: float
    index_value: float


class HoldingRow(BaseModel):
    symbol: str
    asset_name: str
    shares: float
    price: float
    market_value: float
    weight: float


class HoldingsResponse(BaseModel):
    as_of_date: date
    holdings: List[HoldingRow]
    total_value: float


class RiskResponse(BaseModel):
    as_of_date: date
    vol_21d: Optional[float]
    vol_63d: Optional[float]
    max_drawdown_1y: Optional[float]
    var_95_1d_hist: Optional[float]


class BenchmarkMetricResponse(BaseModel):
    benchmark_code: str
    as_of_date: date
    beta_252: Optional[float]
    alpha_252: Optional[float]
    te_252: Optional[float]
    corr_252: Optional[float]
    excess_return_252: Optional[float]


class FactorExposure(BaseModel):
    factor_name: str
    beta: float


class FactorResponse(BaseModel):
    factor_set_code: str
    as_of_date: date
    window: int
    exposures: List[FactorExposure]
    alpha: Optional[float]
    r_squared: Optional[float]


class UnpricedInstrument(BaseModel):
    symbol: str
    asset_name: str
    asset_class: str
    last_seen_date: date
    position_count: int


# Job schemas
class JobRunRequest(BaseModel):
    job_name: str


class JobRunResponse(BaseModel):
    status: str
    message: str
    started_at: datetime
