import argparse
import csv
import json
import logging
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime, date, timedelta
from decimal import Decimal, getcontext
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote_plus, urljoin
from urllib.request import Request, urlopen

LOGGER = logging.getLogger("bp_integration")

getcontext().prec = 28

TIER_INCREMENT = Decimal("0.0000000001")
PRICING_DECIMAL_PLACES = Decimal("0.0000000001")
UNLIMITED_TIER_SENTINEL = Decimal("-1")
ACCOUNT_CREATION_DEFAULTS: Dict[str, str] = {
    "AccountTypeId": "1",
    "ActivityTimeZone": "0",
    "AllowPricingInDifferentCurrency": "0",
    "InvoiceAtThisLevel": "1",
    "ParentAccountId": "1",
    "RateHierarchy": "0",
    "Status": "ACTIVE",
    "eligibleForCollections": "0",
}

BILLING_PROFILE_DEFAULTS: Dict[str, str] = {
    "AchBankAcctType": "Business Checking",
    "Address1": "401 Kentucky St",
    "BillingCycle": "MONTHLY",
    "BillingMethod": "MAIL",
    "CalendarClosingMonth": "January",
    "CalendarClosingWeekday": "Saturday",
    "CalendarType": "4-5-4",
    "City": "Bellingham",
    "Country": "United States",
    "DisablePDFGenerationOnInvoiceClose": "0",
    "Email": "patrick.hermann@ravusinc.com",
    "EventBasedBilling": "0",
    "HostedPaymentPageExternalId": "4032f619-4d78-3b3b-e065-002bff9e4b36",
    "InvoiceApprovalFlag": "1",
    "InvoiceTemplateId": "122",
    "ManualCloseFlag": "1",
    "MonthlyBillingDate": "31",
    "PaymentCreditAllocationMethod": "Allocate To Invoice",
    "PaymentTermDays": "30",
    "PeriodCutoffDate": "2017-01-01T07:59:59.000Z",
    "Phone": "5415564522",
    "QuarterlyBillingMonth": "March, June, September, December",
    "SemiAnnualBillingMonth": "June, December",
    "State": "WA",
    "StatementApprovalFlag": "0",
    "Status": "ACTIVE",
    "TimeZoneId": "351",
    "WeeklyBillingDate": "Monday - Sunday",
    "YearlyBillingMonth": "December",
    "Zip": "98225",
}


class ApiTimeout(RuntimeError):
    """Raised when BillingPlatform times out but likely completes the work."""


def load_env(env_path: Path) -> Dict[str, str]:
    variables: Dict[str, str] = {}
    with env_path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                LOGGER.warning("Skipping malformed env line: %s", line)
                continue
            key, value = line.split("=", 1)
            variables[key.strip()] = value.strip()
    return variables


def http_post_json(url: str, payload: Dict, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> Dict:
    body = json.dumps(payload).encode("utf-8")
    request = Request(url, data=body, method="POST")
    request.add_header("Content-Type", "application/json")
    if headers:
        for name, value in headers.items():
            request.add_header(name, value)
    try:
        with urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return json.loads(response.read().decode(charset))
    except HTTPError as err:
        details = err.read().decode("utf-8", "replace")
        raise RuntimeError(f"POST {url} failed ({err.code}): {details}") from err
    except URLError as err:
        raise RuntimeError(f"POST {url} failed: {err}") from err


def http_put_json(url: str, payload: Dict, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> Dict:
    body = json.dumps(payload).encode("utf-8")
    request = Request(url, data=body, method="PUT")
    request.add_header("Content-Type", "application/json")
    if headers:
        for name, value in headers.items():
            request.add_header(name, value)
    try:
        with urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return json.loads(response.read().decode(charset))
    except HTTPError as err:
        details = err.read().decode("utf-8", "replace")
        raise RuntimeError(f"PUT {url} failed ({err.code}): {details}") from err
    except URLError as err:
        raise RuntimeError(f"PUT {url} failed: {err}") from err


def http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> Dict:
    request = Request(url, method="GET")
    if headers:
        for name, value in headers.items():
            request.add_header(name, value)
    request.add_header("Accept", "application/json")
    try:
        with urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return json.loads(response.read().decode(charset))
    except HTTPError as err:
        details = err.read().decode("utf-8", "replace")
        raise RuntimeError(f"GET {url} failed ({err.code}): {details}") from err
    except URLError as err:
        raise RuntimeError(f"GET {url} failed: {err}") from err


def http_delete_json(url: str, payload: Dict, headers: Optional[Dict[str, str]] = None, timeout: int = 30) -> Dict:
    body = json.dumps(payload).encode("utf-8")
    request = Request(url, data=body, method="DELETE")
    request.add_header("Content-Type", "application/json")
    if headers:
        for name, value in headers.items():
            request.add_header(name, value)
    try:
        with urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return json.loads(response.read().decode(charset))
    except HTTPError as err:
        details = err.read().decode("utf-8", "replace")
        raise RuntimeError(f"DELETE {url} failed ({err.code}): {details}") from err
    except URLError as err:
        raise RuntimeError(f"DELETE {url} failed: {err}") from err


def login(login_url: str, username: str, password: str) -> str:
    LOGGER.info("Authenticating with BillingPlatform")
    payload = {"username": username, "password": password}
    response = http_post_json(login_url, payload)
    login_response = response.get("loginResponse")
    if not login_response:
        raise RuntimeError("Login response missing 'loginResponse'")
    first_entry = login_response[0]
    error_code = first_entry.get("ErrorCode")
    if error_code not in (None, "", 0, "0"):
        raise RuntimeError(
            f"Login failed: {error_code} - {first_entry.get('ErrorText')} (payload: {first_entry})"
        )
    session_id = first_entry.get("SessionID")
    if not session_id:
        raise RuntimeError("Login succeeded but no SessionID was returned")
    return session_id


def load_rows(csv_path: Path) -> Iterable[Dict[str, str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def sanitize_value(value: str) -> str:
    return value.replace("'", "''")


def build_account_query(account_name: str) -> str:
    escaped = sanitize_value(account_name)
    return (
        "SELECT Id, Name, AccountTypeId, ActivityTimeZone, AllowPricingInDifferentCurrency, "
        "InvoiceAtThisLevel, ParentAccountId, RateHierarchy, Status, eligibleForCollections, BillableBillingProfileId "
        f"FROM Account WHERE Name = '{escaped}'"
    )


def build_product_query(product_name: str) -> str:
    escaped = sanitize_value(product_name)
    return f"SELECT Id FROM Product WHERE Name = '{escaped}'"


def perform_lookup(api_base: str, session_id: str, sql: str, timeout: int = 30) -> Dict:
    base_url = api_base.rstrip("/") + "/"
    url = urljoin(base_url, f"query?sql={quote_plus(sql)}")
    try:
        return http_get_json(url, headers={"sessionId": session_id}, timeout=timeout)
    except RuntimeError as exc:
        message = str(exc)
        if "404" in message:
            LOGGER.debug("Lookup returned 404 for query '%s'; treating as empty result.", sql)
            return {}
        raise


_PRODUCT_CACHE: Dict[str, Optional[str]] = {}
_PRODUCT_LOOKUP_API_BASE: Optional[str] = None
_PRODUCT_LOOKUP_SESSION_ID: Optional[str] = None


def configure_product_lookup(api_base: str, session_id: str) -> None:
    global _PRODUCT_LOOKUP_API_BASE, _PRODUCT_LOOKUP_SESSION_ID
    _PRODUCT_LOOKUP_API_BASE = api_base
    _PRODUCT_LOOKUP_SESSION_ID = session_id
    _PRODUCT_CACHE.clear()


def get_product_id(product_name: str) -> Optional[str]:
    if product_name in _PRODUCT_CACHE:
        return _PRODUCT_CACHE[product_name]
    if not _PRODUCT_LOOKUP_API_BASE or not _PRODUCT_LOOKUP_SESSION_ID:
        LOGGER.error("Product lookup attempted before login context was configured.")
        return None

    LOGGER.info("Looking up product '%s'", product_name)
    try:
        lookup_response = perform_lookup(
            _PRODUCT_LOOKUP_API_BASE,
            _PRODUCT_LOOKUP_SESSION_ID,
            build_product_query(product_name),
        )
    except Exception as exc:
        LOGGER.error("Product lookup failed for '%s': %s", product_name, exc)
        _PRODUCT_CACHE[product_name] = None
        return None

    records = lookup_response.get("queryResponse") or []
    first_record = records[0] if records else {}
    product_id_value = first_record.get("Id") or first_record.get("id")
    if not product_id_value:
        LOGGER.warning("No product found for '%s'", product_name)
    _PRODUCT_CACHE[product_name] = product_id_value
    return product_id_value


def parse_date(value: Optional[str], fallback: Optional[date] = None) -> date:
    if value:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            LOGGER.warning("Invalid date '%s', defaulting to fallback", value)
    return fallback or date.today()


def parse_optional_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        LOGGER.warning("Invalid date '%s', defaulting to None", value)
        return None


def parse_iso_to_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    text = value.split("T", 1)[0].strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        LOGGER.warning("Invalid date '%s', defaulting to None", value)
        return None


def parse_quantity(value: Optional[str]) -> int:
    if not value:
        return 1
    try:
        parsed = int(value)
        return parsed if parsed > 0 else 1
    except ValueError:
        LOGGER.warning("Invalid quantity '%s', defaulting to 1", value)
        return 1


def parse_rate(value: Optional[str]) -> float:
    if not value:
        return 0.0
    try:
        return float(value)
    except ValueError:
        LOGGER.warning("Invalid rate '%s', defaulting to 0", value)
        return 0.0


def parse_bool(value: Optional[str]) -> bool:
    if value is None:
        return False
    normalized = value.strip().lower()
    return normalized in {"true", "1", "yes", "y", "t"}


def format_decimal(value: Decimal) -> str:
    if value == value.to_integral():
        return str(value.quantize(Decimal("1")))
    quantized = value.quantize(PRICING_DECIMAL_PLACES)
    text = f"{quantized:.10f}".rstrip("0").rstrip(".")
    return text or "0"


def parse_legacy_pricing_tiers(raw_value: Optional[str]) -> List[Dict[str, Decimal]]:
    tiers: List[Dict[str, Decimal]] = []
    if not raw_value:
        return tiers
    for part in raw_value.split(";"):
        entry = part.strip()
        if not entry:
            continue
        if ":" not in entry:
            LOGGER.warning("Skipping malformed tier definition '%s'", entry)
            continue
        upper_part, rate_part = entry.split(":", 1)
        upper_part = upper_part.strip()
        rate_part = rate_part.strip()
        try:
            upper_value = None if upper_part == "-1" else Decimal(upper_part)
        except Exception:
            LOGGER.warning("Invalid upper band '%s' in tier '%s'", upper_part, entry)
            continue
        try:
            rate_value = Decimal(rate_part)
        except Exception:
            LOGGER.warning("Invalid rate '%s' in tier '%s'", rate_part, entry)
            continue
        tiers.append({"upper": upper_value, "rate": rate_value})
    return tiers


def parse_structured_pricing_tiers(row: Dict[str, str], max_tiers: int = 12) -> List[Dict[str, Decimal]]:
    tiers: List[Dict[str, Decimal]] = []
    previous_upper: Optional[Decimal] = None
    seen_any = False
    for index in range(1, max_tiers + 1):
        prefix = f"tier{index}"
        lower_key = f"{prefix}_from_qty"
        upper_key = f"{prefix}_to_qty"
        rate_key = f"{prefix}_rate"

        lower_raw = (row.get(lower_key) or "").strip()
        upper_raw = (row.get(upper_key) or "").strip()
        rate_raw = (row.get(rate_key) or "").strip()

        if not lower_raw and not upper_raw and not rate_raw:
            continue

        seen_any = True
        if not rate_raw:
            LOGGER.warning(
                "Tier %d is missing a rate value; skipping remaining tiers for row: %s",
                index,
                row,
            )
            break

        try:
            rate_value = Decimal(rate_raw)
        except Exception:
            LOGGER.warning("Invalid rate '%s' for tier %d; skipping remaining tiers", rate_raw, index)
            break

        if lower_raw:
            try:
                lower_value = Decimal(lower_raw)
            except Exception:
                LOGGER.warning("Invalid lower quantity '%s' for tier %d; skipping remaining tiers", lower_raw, index)
                break
        else:
            lower_value = Decimal("0") if previous_upper is None else (previous_upper + TIER_INCREMENT)

        if previous_upper is not None:
            expected_lower = (previous_upper + TIER_INCREMENT).quantize(PRICING_DECIMAL_PLACES)
            if lower_raw and Decimal(lower_raw) != expected_lower:
                LOGGER.warning(
                    "Tier %d lower quantity %s does not match the previous tier upper quantity %s plus increment %s.",
                    index,
                    lower_value,
                    previous_upper,
                    TIER_INCREMENT,
                )
            elif not lower_raw and lower_value != expected_lower:
                lower_value = expected_lower

        if upper_raw:
            upper_text = upper_raw.strip()
            if not upper_text:
                LOGGER.warning("Invalid upper quantity '%s' for tier %d; skipping remaining tiers", upper_raw, index)
                break
            try:
                upper_value = Decimal(upper_text)
            except Exception:
                LOGGER.warning("Invalid upper quantity '%s' for tier %d; skipping remaining tiers", upper_raw, index)
                break
            if upper_value == UNLIMITED_TIER_SENTINEL:
                upper_value = None
        else:
            upper_value = None

        if upper_value is not None and upper_value < lower_value:
            LOGGER.warning(
                "Tier %d upper quantity %s is below lower quantity %s; skipping remaining tiers",
                index,
                upper_value,
                lower_value,
            )
            break

        tiers.append({"lower": lower_value, "upper": upper_value, "rate": rate_value})

        if upper_value is None:
            break
        previous_upper = upper_value

    if not seen_any:
        return []
    if tiers and tiers[-1]["upper"] is not None:
        LOGGER.warning(
            "Last tier upper quantity is %s; append an unlimited tier (to_qty = -1) if usage can exceed this value.",
            tiers[-1]["upper"],
        )
    return tiers


def parse_pricing_band(value: Optional[str]) -> Optional[Decimal]:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        numeric_value = Decimal(text)
    except Exception:
        LOGGER.debug("Unable to parse pricing band value '%s'", value)
        return None
    if numeric_value == UNLIMITED_TIER_SENTINEL:
        return None
    return numeric_value


def build_canonical_tiers_from_row(row: Dict[str, str], args) -> List[Dict[str, Decimal]]:
    tiers_column = args.tiers_column
    legacy_tiers_raw = row.get(tiers_column)
    structured_tiers = parse_structured_pricing_tiers(row)
    canonical_tiers: List[Dict[str, Decimal]] = []

    if structured_tiers:
        for idx, tier in enumerate(structured_tiers):
            lower_value = tier.get("lower")
            if lower_value is None:
                lower_value = Decimal("0") if idx == 0 else (
                    canonical_tiers[-1]["upper"] + TIER_INCREMENT
                    if canonical_tiers[-1]["upper"] is not None
                    else canonical_tiers[-1]["lower"]
                )
                lower_value = lower_value.quantize(PRICING_DECIMAL_PLACES)
            canonical_tiers.append(
                {
                    "lower": lower_value,
                    "upper": tier.get("upper"),
                    "rate": tier["rate"],
                }
            )
    else:
        legacy_tiers = parse_legacy_pricing_tiers(legacy_tiers_raw)
        if legacy_tiers:
            lower_band_value = Decimal("0")
            for legacy_tier in legacy_tiers:
                upper_value = legacy_tier["upper"]
                canonical_tiers.append(
                    {
                        "lower": lower_band_value,
                        "upper": upper_value,
                        "rate": legacy_tier["rate"],
                    }
                )
                if upper_value is None:
                    break
                lower_band_value = (upper_value + TIER_INCREMENT).quantize(PRICING_DECIMAL_PLACES)

        if not canonical_tiers:
            fallback_rate = Decimal(str(parse_rate(row.get(args.rate_column))))
            canonical_tiers = [
                {
                    "lower": Decimal("0"),
                    "upper": None,
                    "rate": fallback_rate,
                }
            ]

    return canonical_tiers


def build_pricing_payloads_from_rows(
    contract_rate_id: str,
    currency_code: str,
    rows_for_group: List[Dict[str, str]],
    args,
    *,
    fallback_start_date: Optional[date] = None,
    existing_pricing_index: Optional[Dict[Tuple[str, Optional[Decimal], Optional[Decimal]], Dict]] = None,
    product_name: Optional[str] = None,
) -> Tuple[List[Dict[str, str]], List[Dict[str, Decimal]], List[Dict[str, Any]]]:
    pricing_payload_entries: List[Dict[str, str]] = []
    skipped_existing: List[Dict[str, Any]] = []
    last_canonical_tiers: List[Dict[str, Decimal]] = []

    for row in rows_for_group:
        row_effective_date = parse_date(
            row.get(args.effective_date_column),
            fallback=parse_date(row.get(args.start_date_column), fallback=fallback_start_date),
        )
        row_end_date = parse_optional_date(row.get(args.end_date_column))
        effective_iso = f"{row_effective_date.isoformat()}T00:00:00.000Z"
        end_date_iso = f"{row_end_date.isoformat()}T00:00:00.000Z" if row_end_date else None

        canonical_tiers = build_canonical_tiers_from_row(row, args)
        if canonical_tiers:
            last_canonical_tiers = canonical_tiers

        tiers_for_creation = list(enumerate(canonical_tiers))
        tiers_for_creation.sort(key=lambda item: (1 if item[1]["upper"] is None else 0, item[1]["lower"]))

        for index, tier in tiers_for_creation:
            lower_value = tier["lower"]
            upper_value = tier["upper"]
            rate_value = tier["rate"]
            lower_band_str = format_decimal(lower_value)

            if upper_value is None:
                upper_band_str = "-1"
            else:
                if upper_value < lower_value:
                    LOGGER.error(
                        "Tier %d upper quantity (%s) is below lower quantity (%s)%s. Skipping remaining tiers.",
                        index + 1,
                        upper_value,
                        lower_value,
                        f" for product '{product_name}'" if product_name else "",
                    )
                    break
                upper_band_str = format_decimal(upper_value)

            if existing_pricing_index is not None:
                existing_key = (currency_code, lower_value, upper_value)
                existing_entry = existing_pricing_index.get(existing_key)
                if existing_entry:
                    LOGGER.info(
                        "Pricing tier already exists for product '%s' (currency %s, lower %s, upper %s); skipping creation.",
                        product_name or "",
                        currency_code,
                        lower_band_str,
                        upper_band_str,
                    )
                    skipped_existing.append({"existing": existing_entry})
                    continue

            pricing_entry: Dict[str, str] = {
                "CurrencyCode": currency_code,
                "ContractRateId": contract_rate_id,
                "EffectiveDate": effective_iso,
                "LowerBand": lower_band_str,
                "UpperBand": upper_band_str,
                "Rate": format_decimal(rate_value),
                "RerateFlag": "0",
            }
            if end_date_iso:
                pricing_entry["EndDate"] = end_date_iso
            pricing_payload_entries.append(pricing_entry)

    return pricing_payload_entries, last_canonical_tiers, skipped_existing


def next_contract_number(api_base: str, session_id: str, base_date: date) -> str:
    prefix = base_date.strftime("%Y-%m-%d")
    sql = f"SELECT ContractNumber FROM Contract WHERE ContractNumber LIKE '{prefix}_%'"
    existing: List[Dict] = []
    try:
        response = perform_lookup(api_base, session_id, sql)
    except RuntimeError as exc:
        if "404" in str(exc):
            LOGGER.info("No existing contracts found for prefix '%s'; starting new sequence.", prefix)
        else:
            raise
    else:
        existing = response.get("queryResponse", []) or []
    suffix_values: List[int] = []
    for entry in existing:
        number = (entry or {}).get("ContractNumber") or (entry or {}).get("contractnumber")
        if not number or not number.startswith(prefix + "_"):
            continue
        suffix = number.split("_", maxsplit=1)[1]
        try:
            suffix_values.append(int(suffix))
        except ValueError:
            continue
    next_index = max(suffix_values, default=0) + 1
    return f"{prefix}_{next_index:02d}"


def create_account(api_base: str, session_id: str, account_name: str) -> Dict:
    payload = {"brmObjects": dict(ACCOUNT_CREATION_DEFAULTS)}
    payload["brmObjects"]["Name"] = account_name
    base_url = api_base.rstrip("/") + "/"
    url = urljoin(base_url, "ACCOUNT")
    try:
        return http_post_json(url, payload, headers={"sessionId": session_id})
    except RuntimeError as exc:
        message = str(exc).lower()
        if "timed out" in message or "remote end closed" in message or "connection reset" in message:
            raise ApiTimeout(str(exc))
        raise


def find_billing_profiles(api_base: str, session_id: str, account_id: str) -> List[Dict]:
    sql = f"SELECT Id FROM BILLING_PROFILE WHERE AccountID = '{account_id}'"
    response = perform_lookup(api_base, session_id, sql)
    return response.get("queryResponse", []) or []


def get_billing_profile(api_base: str, session_id: str, billing_profile_id: str) -> List[Dict]:
    sql = f"SELECT Id, AccountID FROM BILLING_PROFILE WHERE Id = '{billing_profile_id}'"
    response = perform_lookup(api_base, session_id, sql)
    return response.get("queryResponse", []) or []


def create_billing_profile(
    api_base: str,
    session_id: str,
    account_id: str,
    account_name: str,
    currency_code: str,
    start_date_value: date,
    bill_to_name: Optional[str] = None,
) -> Dict:
    bill_to_value = bill_to_name or account_name
    payload = {"brmObjects": dict(BILLING_PROFILE_DEFAULTS)}
    payload["brmObjects"].update(
        {
            "AccountId": account_id,
            "BillingEntity": account_name,
            "BillTo": bill_to_value,
            "CurrencyCode": currency_code,
            "IniBillingStartDate": start_date_value.isoformat(),
        }
    )
    payload["brmObjects"].pop("PeriodCutoffDate", None)
    payload["brmObjects"]["HostedPaymentPageExternalId"] = str(uuid.uuid4())
    base_url = api_base.rstrip("/") + "/"
    url = urljoin(base_url, "BILLING_PROFILE")
    try:
        return http_post_json(url, payload, headers={"sessionId": session_id})
    except RuntimeError as exc:
        message = str(exc).lower()
        if "timed out" in message or "remote end closed" in message or "connection reset" in message:
            raise ApiTimeout(str(exc))
        raise


def update_account_billing_profile(
    api_base: str,
    session_id: str,
    account_id: str,
    account_name: str,
    billing_profile_id: str,
    account_fields: Optional[Dict[str, Any]] = None,
) -> Dict:
    fields = account_fields or {}
    payload_fields: Dict[str, Any] = {
        "Id": account_id,
        "Name": fields.get("Name") or fields.get("name") or account_name,
        "BillingProfileId": billing_profile_id,
        "BillableBillingProfileId": billing_profile_id,
    }
    preserved_fields = {
        "AccountTypeId": ACCOUNT_CREATION_DEFAULTS["AccountTypeId"],
        "ActivityTimeZone": ACCOUNT_CREATION_DEFAULTS["ActivityTimeZone"],
        "AllowPricingInDifferentCurrency": ACCOUNT_CREATION_DEFAULTS["AllowPricingInDifferentCurrency"],
        "InvoiceAtThisLevel": ACCOUNT_CREATION_DEFAULTS["InvoiceAtThisLevel"],
        "ParentAccountId": ACCOUNT_CREATION_DEFAULTS["ParentAccountId"],
        "RateHierarchy": ACCOUNT_CREATION_DEFAULTS["RateHierarchy"],
        "Status": ACCOUNT_CREATION_DEFAULTS["Status"],
        "eligibleForCollections": ACCOUNT_CREATION_DEFAULTS["eligibleForCollections"],
    }
    for field, default in preserved_fields.items():
        value = fields.get(field)
        if value is None and isinstance(field, str):
            value = fields.get(field.lower())
        if value is None:
            value = default
        payload_fields[field] = value

    base_url = api_base.rstrip("/") + "/"
    put_payload = {"brmObjects": payload_fields}
    put_url = urljoin(base_url, f"ACCOUNT/{account_id}")
    try:
        LOGGER.debug("Attempting account update via PUT to '%s'", put_url)
        response = http_put_json(put_url, put_payload, headers={"sessionId": session_id})
        LOGGER.debug("Account update via PUT succeeded.")
        return response
    except RuntimeError as exc:
        message = str(exc).lower()
        if "timed out" in message or "remote end closed" in message or "connection reset" in message:
            raise ApiTimeout(str(exc))
        if "404" in message:
            LOGGER.debug("PUT endpoint '%s' unavailable for account update (%s)", put_url, exc)
        else:
            LOGGER.debug("PUT endpoint '%s' returned error for account update: %s", put_url, exc)
            raise

    payload = {"brmObjects": payload_fields, "uniqueIdentifier": "Id", "mode": "UPDATE"}
    candidate_paths = ["ACCOUNT/UPSERT", "ACCOUNT/UPDATE", "ACCOUNT/update", "ACCOUNT"]
    last_error: Optional[Exception] = None
    for path in candidate_paths:
        url = urljoin(base_url, path)
        try:
            LOGGER.debug("Attempting account update via endpoint '%s'", path)
            response = http_post_json(url, payload, headers={"sessionId": session_id})
            LOGGER.debug("Account update via endpoint '%s' succeeded.", path)
            return response
        except RuntimeError as exc:
            message = str(exc).lower()
            if "timed out" in message or "remote end closed" in message or "connection reset" in message:
                raise ApiTimeout(str(exc))
            if "404" in message:
                LOGGER.debug("Endpoint '%s' unavailable for account update (%s)", path, exc)
                last_error = exc
                continue
            LOGGER.debug("Endpoint '%s' returned error for account update: %s", path, exc)
            raise
    if last_error:
        raise last_error
    raise RuntimeError("Unable to update account billing profile: no valid endpoint responded.")


def create_contract(
    api_base: str,
    session_id: str,
    account_id: str,
    contract_number: str,
    start_date_value: date,
    status: str,
    cpq_contract_id: str,
) -> Dict:
    payload = {
        "brmObjects": {
            "AccountId": account_id,
            "ContractNumber": contract_number,
            "StartDate": start_date_value.isoformat(),
            "OnEndDate": "Terminate",
            "ContractStatus": status,
            "C_CPQContractId": cpq_contract_id,
        }
    }
    base_url = api_base.rstrip("/") + "/"
    url = urljoin(base_url, "CONTRACT")
    return http_post_json(url, payload, headers={"sessionId": session_id})


def create_contract_currency(api_base: str, session_id: str, contract_id: str, currency_code: str) -> Dict:
    payload = {
        "brmObjects": {
            "ContractId": contract_id,
            "CurrencyCode": currency_code,
        }
    }
    base_url = api_base.rstrip("/") + "/"
    url = urljoin(base_url, "CONTRACT_CURRENCY")
    return http_post_json(url, payload, headers={"sessionId": session_id})


def create_account_product(
    api_base: str,
    session_id: str,
    account_id: str,
    contract_id: str,
    product_id: str,
    start_date_value: date,
    quantity: int,
    status: str,
    extra_fields: Optional[Dict[str, Any]] = None,
) -> Dict:
    payload = {
        "brmObjects": {
            "AccountId": account_id,
            "ContractID": contract_id,
            "ProductID": product_id,
            "StartDate": start_date_value.isoformat(),
            "Quantity": quantity,
            "Status": status,
        }
    }
    if extra_fields:
        payload["brmObjects"].update(extra_fields)
    base_url = api_base.rstrip("/") + "/"
    url = urljoin(base_url, "ACCOUNT_PRODUCT")
    try:
        return http_post_json(url, payload, headers={"sessionId": session_id}, timeout=120)
    except RuntimeError as exc:
        message = str(exc).lower()
        if "timed out" in message or "remote end closed" in message or "connection reset" in message:
            raise ApiTimeout(str(exc))
        raise


def create_contract_rate(api_base: str, session_id: str, contract_id: str, product_id: str) -> Dict:
    payload = {
        "brmObjects": {
            "ContractID": contract_id,
            "ProductID": product_id,
            "RateOrder": 1,
            "Status": "Active",
        }
    }
    base_url = api_base.rstrip("/") + "/"
    url = urljoin(base_url, "CONTRACT_RATE")
    try:
        return http_post_json(url, payload, headers={"sessionId": session_id})
    except RuntimeError as exc:
        message = str(exc).lower()
        if "timed out" in message or "remote end closed" in message or "connection reset" in message:
            raise ApiTimeout(str(exc))
        raise


def create_pricing(
    api_base: str,
    session_id: str,
    contract_rate_id: str,
    currency_code: str,
    effective_date_iso: str,
    lower_band: str,
    upper_band: str,
    rate: str,
    end_date_iso: Optional[str] = None,
    rerate_flag: str = "0",
) -> Dict:
    brm_objects = {
        "CurrencyCode": currency_code,
        "ContractRateId": contract_rate_id,
        "EffectiveDate": effective_date_iso,
        "LowerBand": lower_band,
        "Rate": rate,
        "RerateFlag": rerate_flag,
        "UpperBand": upper_band,
        "EndDate": end_date_iso
    }
    # if end_date_iso:
    #     brm_objects["EndDate"] = end_date_iso

    payload = {"brmObjects": brm_objects}
    base_url = api_base.rstrip("/") + "/"
    url = urljoin(base_url, "PRICING")
    return http_post_json(url, payload, headers={"sessionId": session_id})


def create_pricing_batch(
    api_base: str,
    session_id: str,
    pricing_entries: List[Dict[str, str]],
) -> Dict:
    payload = {"brmObjects": pricing_entries}
    base_url = api_base.rstrip("/") + "/"
    url = urljoin(base_url, "PRICING")
    return http_post_json(url, payload, headers={"sessionId": session_id})


def find_account_product(api_base: str, session_id: str, contract_id: str, product_id: str) -> List[Dict]:
    sql = (
        "SELECT Id, ContractID, ProductID FROM ACCOUNT_PRODUCT "
        f"WHERE ContractID = '{contract_id}' AND ProductID = '{product_id}'"
    )
    response = perform_lookup(api_base, session_id, sql)
    return response.get("queryResponse", []) or []


def find_contract_rate(api_base: str, session_id: str, contract_id: str, product_id: str) -> List[Dict]:
    sql = (
        "SELECT Id, ContractID, ProductID, RateOrder FROM CONTRACT_RATE "
        f"WHERE ContractID = '{contract_id}' AND ProductID = '{product_id}'"
    )
    response = perform_lookup(api_base, session_id, sql)
    return response.get("queryResponse", []) or []


def find_pricing_entries(api_base: str, session_id: str, contract_rate_id: str) -> List[Dict]:
    sql = (
        "SELECT Id, ContractRateId, LowerBand, UpperBand, Rate, CurrencyCode "
        f"FROM PRICING WHERE ContractRateId = '{contract_rate_id}'"
    )
    try:
        response = perform_lookup(api_base, session_id, sql)
    except RuntimeError as exc:
        if "404" in str(exc):
            LOGGER.info(
                "No existing pricing entries found for contract rate %s (404 response treated as empty).",
                contract_rate_id,
            )
            return []
        raise
    return response.get("queryResponse", []) or []


def lookup_account_id_by_name(api_base: str, session_id: str, account_name: str) -> Optional[str]:
    if not account_name:
        return None
    try:
        response = perform_lookup(api_base, session_id, build_account_query(account_name))
    except Exception as exc:
        LOGGER.error("Account lookup failed for '%s': %s", account_name, exc)
        return None
    records = response.get("queryResponse", []) or []
    if not records:
        return None
    return records[0].get("Id") or records[0].get("id")


def lookup_contract_id_by_name(
    api_base: str, session_id: str, contract_name: str, account_id: str, cpq_contract_id: Optional[str] = None
) -> Optional[str]:
    if not account_id:
        return None

    if cpq_contract_id:
        escaped_cpq = sanitize_value(cpq_contract_id)
        sql = (
            "SELECT Id FROM CONTRACT "
            f"WHERE C_CPQContractId = '{escaped_cpq}' "
            f"AND AccountId = '{account_id}'"
        )
    else:
        escaped_contract = sanitize_value(contract_name)
        sql = (
            "SELECT Id FROM CONTRACT "
            f"WHERE ContractNumber = '{escaped_contract}' "
            f"AND AccountId = '{account_id}'"
        )

    response = perform_lookup(api_base, session_id, sql)
    records = response.get("queryResponse", []) or []
    if records:
        return records[0].get("Id") or records[0].get("id")
    return None


def lookup_contract_rate_id(api_base: str, session_id: str, contract_id: str, product_id: str) -> Optional[str]:
    sql = (
        "SELECT Id FROM CONTRACT_RATE "
        f"WHERE ContractID = '{contract_id}' AND ProductID = '{product_id}'"
    )
    response = perform_lookup(api_base, session_id, sql)
    rows = response.get("queryResponse", []) or []
    if not rows:
        return None

    chosen = rows[0]
    return chosen.get("Id") or chosen.get("id")


def fetch_pricing_for_contract_rate(
    api_base: str, session_id: str, contract_rate_id: str, currency_code: str
) -> List[Dict]:
    sql = (
        "SELECT Id, EffectiveDate, EndDate, LowerBand, UpperBand, Rate "
        "FROM PRICING "
        f"WHERE ContractRateId = '{contract_rate_id}' "
        f"AND CurrencyCode = '{currency_code}' "
        "ORDER BY EffectiveDate ASC"
    )
    response = perform_lookup(api_base, session_id, sql)
    return response.get("queryResponse", []) or []


def update_pricing_record(api_base: str, session_id: str, payload: Dict) -> Dict:
    wrapped_payload = payload if "brmObjects" in payload else {"brmObjects": payload}
    base_url = api_base.rstrip("/") + "/"
    url = urljoin(base_url, "PRICING")
    return http_put_json(url, wrapped_payload, headers={"sessionId": session_id})


def delete_pricing_batch(api_base: str, session_id: str, ids: List[str]) -> Dict:
    base_url = api_base.rstrip("/") + "/"
    url = urljoin(base_url, "delete/PRICING")
    payload = {"brmObjects": [{"Id": pid} for pid in ids]}
    return http_delete_json(url, payload, headers={"sessionId": session_id})


def lookup_contract_id_by_cpq_id(
    api_base: str,
    session_id: str,
    cpq_contract_id: str,
    account_id: str,
) -> Tuple[Optional[str], Optional[str]]:
    if not cpq_contract_id:
        return None, None
    escaped_cpq_id = sanitize_value(cpq_contract_id)
    sql = (
        "SELECT Id, StartDate FROM CONTRACT "
        f"WHERE C_CPQContractId = '{escaped_cpq_id}' "
        f"AND AccountId = '{account_id}'"
    )
    response = perform_lookup(api_base, session_id, sql)
    records = response.get("queryResponse", []) or []
    if records:
        record = records[0]
        contract_id = record.get("Id") or record.get("id")
        start_date_value = (
            record.get("StartDate")
            or record.get("startdate")
            or record.get("Startdate")
        )
        return contract_id, start_date_value
    return None, None


def get_account_product(api_base: str, session_id: str, account_product_id: str) -> List[Dict]:
    sql = (
        "SELECT Id, Status "
        f"FROM ACCOUNT_PRODUCT WHERE Id = '{account_product_id}'"
    )
    response = perform_lookup(api_base, session_id, sql)
    return response.get("queryResponse", []) or []


def update_account_product(api_base: str, session_id: str, payload: Dict) -> Dict:
    wrapped_payload = payload if "brmObjects" in payload else {"brmObjects": payload}
    base_payload = wrapped_payload.get("brmObjects") or {}
    ap_id = base_payload.get("Id") or base_payload.get("id")
    base_url = api_base.rstrip("/") + "/"
    url_path = f"ACCOUNT_PRODUCT/{ap_id}" if ap_id else "ACCOUNT_PRODUCT"
    url = urljoin(base_url, url_path)
    return http_put_json(url, wrapped_payload, headers={"sessionId": session_id})


def extract_first_value(response: Dict, field: str) -> Optional[str]:
    records = response.get("queryResponse")
    if not records:
        return None
    first = records[0]
    return first.get(field) or first.get(field.lower())


def main() -> None:
    parser = argparse.ArgumentParser(description="BillingPlatform contract workflow")
    parser.add_argument("--input", required=True, help="Path to the CSV file containing source data.")
    parser.add_argument("--account-column", default="account_name", help="Column with the BillingPlatform Account Name.")
    parser.add_argument("--product-column", default="product_name", help="Column with the BillingPlatform Product Name.")
    parser.add_argument("--quantity-column", default="quantity", help="Column containing quantity (default 1).")
    parser.add_argument("--currency-column", default="currency_code", help="Column containing contract currency (e.g. USD).")
    parser.add_argument("--rate-column", default="rate", help="Column containing the contract rate.")
    parser.add_argument("--start-date-column", default="start_date", help="Column containing start date (YYYY-MM-DD). Optional.")
    parser.add_argument("--effective-date-column", default="effective_date", help="Column containing rate effective date (YYYY-MM-DD). Optional.")
    parser.add_argument("--end-date-column", default="end_date", help="Column containing rate end date (YYYY-MM-DD). Optional.")
    parser.add_argument("--contract-status-column", default="contract_status", help="Column for contract status (default Terminated).")
    parser.add_argument("--account-product-status-column", default="account_product_status", help="Column for account product status (default Active).")
    parser.add_argument("--env-file", default=".env", help="Path to the environment file with credentials and endpoints.")
    parser.add_argument(
        "--billing-identifier-product-name",
        default="Billing Identifier",
        help="Optional product added per contract with BillIdent set to the contract number. Provide an empty string to skip.",
    )
    parser.add_argument(
        "--contract-group-column",
        default="contract",
        help="Column containing the logical contract grouping identifier. Rows with the same value share one contract.",
    )
    parser.add_argument(
        "--tiers-column",
        default="pricing_tiers",
        help=(
            "Legacy column defining pricing tiers per product (semicolon separated upper:rate entries). "
            "If structured tier columns (tier{n}_from_qty/tier{n}_to_qty/tier{n}_rate) are present, they take precedence."
        ),
    )
    parser.add_argument(
        "--rate-only-column",
        default="contract_rate_only",
        help="Column that flags rows to skip account product creation and only create contract rate/pricing (values like 'true', 'yes', '1').",
    )
    parser.add_argument(
        "--pricing-only-column",
        default="pricing_only",
        help="Column that flags rows to only create pricing records (no account product or contract rate creation).",
    )
    parser.add_argument(
        "--bundle-component-column",
        default="bundle_component",
        help="Column that flags rows whose account products should be created without contract rate or pricing.",
    )
    parser.add_argument(
        "--cpq-contract-id-column",
        default="CPQ_Contractid",
        help="Column containing CPQ contract id used for downstream amendments.",
    )
    parser.add_argument(
        "--action-column",
        default="action",
        help="Column determining row behavior: Create, Quantity Change, Price Change (future).",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    env_path = Path(args.env_file).expanduser().resolve()
    if not env_path.exists():
        raise SystemExit(f"Env file not found: {env_path}")
    env_vars = load_env(env_path)

    login_url = env_vars.get("BP_LOGIN_URL")
    api_base = env_vars.get("BP_API_BASE_URL")
    username = env_vars.get("BP_USERNAME") or os.environ.get("BP_USERNAME")
    password = env_vars.get("BP_PASSWORD") or os.environ.get("BP_PASSWORD")

    required = {"BP_LOGIN_URL": login_url, "BP_API_BASE_URL": api_base, "BP_USERNAME": username, "BP_PASSWORD": password}
    missing = [key for key, value in required.items() if not value]
    if missing:
        raise SystemExit(f"Missing required env variables: {', '.join(missing)}")

    session_id = login(login_url, username, password)
    LOGGER.info("Login succeeded. SessionID obtained.")
    configure_product_lookup(api_base, session_id)

    csv_path = Path(args.input).expanduser().resolve()
    if not csv_path.exists():
        raise SystemExit(f"Input CSV not found: {csv_path}")

    rows = list(load_rows(csv_path))
    if not rows:
        LOGGER.info("Input CSV %s contained no data rows.", csv_path)
        return

    actions = {(row.get(args.action_column) or "").strip().lower() for row in rows}
    create_actions = {"", "create"}
    amend_actions = {"quantity change", "price change", "quantity and price change"}

    if (actions & create_actions) and (actions & amend_actions):
        raise SystemExit(
            "Mixed 'Create' and 'Quantity Change'/'Price Change' rows in same file. "
            "Separate these into different CSVs. Failing fast."
        )

    is_amendment_file = all(
        (row.get(args.action_column) or "").strip().lower()
        in {"quantity change", "price change", "quantity and price change"}
        for row in rows
    )
    if is_amendment_file:
        return process_amendments(rows, args, api_base, session_id)

    contract_column = args.contract_group_column
    rate_only_column = args.rate_only_column

    contract_groups: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        contract_key = (row.get(contract_column) or "").strip()
        if not contract_key:
            fallback_key = f"{row.get(args.account_column, '').strip()}:{row.get(args.product_column, '').strip()}"
            contract_key = fallback_key or f"row-{len(contract_groups) + 1}"
            LOGGER.warning("Row missing contract identifier; defaulting to '%s'", contract_key)
        contract_groups.setdefault(contract_key, []).append(row)

    for contract_key, grouped_rows in contract_groups.items():
        first_row = grouped_rows[0]
        account_name = first_row.get(args.account_column)
        if not account_name:
            LOGGER.warning("Skipping contract '%s' because account name is missing.", contract_key)
            continue
        currency_code = first_row.get(args.currency_column)
        if not currency_code:
            LOGGER.warning("Skipping contract '%s' because currency code is missing.", contract_key)
            continue

        LOGGER.info("Processing contract group '%s' for account '%s'", contract_key, account_name)

        LOGGER.info("Looking up account '%s'", account_name)
        try:
            account_lookup = perform_lookup(api_base, session_id, build_account_query(account_name))
        except Exception as exc:
            LOGGER.error("Account lookup failed for '%s': %s", account_name, exc)
            continue

        account_records = account_lookup.get("queryResponse", []) or []
        account_record: Dict[str, Any] = account_records[0] if account_records else {}
        account_id = (account_record or {}).get("Id") or (account_record or {}).get("id")
        billing_profile_id = (
            (account_record or {}).get("BillableBillingProfileId")
            or (account_record or {}).get("billablebillingprofileid")
        )

        account_creation_response: Optional[Dict] = None
        if not account_id:
            LOGGER.info("Account '%s' not found; creating new account", account_name)
            try:
                account_creation_response = create_account(api_base, session_id, account_name)
            except ApiTimeout as timeout_exc:
                LOGGER.warning("Account creation timed out; verifying record: %s", timeout_exc)
            except Exception as exc:
                LOGGER.error("Account creation failed for '%s': %s", account_name, exc)
                continue
            else:
                create_resp = account_creation_response.get("createResponse") or []
                if create_resp:
                    account_id = create_resp[0].get("Id") or create_resp[0].get("id")
        if not account_id:
            try:
                account_lookup = perform_lookup(api_base, session_id, build_account_query(account_name))
            except Exception as exc:
                LOGGER.error("Account lookup after creation failed for '%s': %s", account_name, exc)
                continue
            account_records = account_lookup.get("queryResponse", []) or []
            account_record = account_records[0] if account_records else {}
            account_id = (account_record or {}).get("Id") or (account_record or {}).get("id")
            billing_profile_id = (
                (account_record or {}).get("BillableBillingProfileId")
                or (account_record or {}).get("billablebillingprofileid")
            )
        if not account_id:
            LOGGER.error("Unable to determine account Id for '%s' after creation attempt.", account_name)
            continue
        if account_creation_response:
            LOGGER.info("Account '%s' created with Id %s", account_name, account_id)

        contract_status = first_row.get(args.contract_status_column) or "Terminated"
        start_date_value = parse_date(first_row.get(args.start_date_column))
        cpq_contract_id = (first_row.get(args.cpq_contract_id_column) or "").strip()
        bill_to_value = (
            first_row.get("bill_to")
            or first_row.get("bill_to_name")
            or first_row.get("BillTo")
            or first_row.get("Bill_To")
            or account_name
        )

        LOGGER.info("Ensuring billing profile exists for account '%s'", account_name)
        existing_profiles = find_billing_profiles(api_base, session_id, account_id)
        if existing_profiles:
            profile_record = existing_profiles[0] or {}
            billing_profile_id = profile_record.get("Id") or profile_record.get("id")
        elif billing_profile_id:
            profile_records = get_billing_profile(api_base, session_id, billing_profile_id)
            profile_record = profile_records[0] if profile_records else {}
            profile_account_id = (
                (profile_record or {}).get("AccountID")
                or (profile_record or {}).get("accountid")
                or (profile_record or {}).get("AccountId")
                or (profile_record or {}).get("accountid")
            )
            if not profile_account_id or str(profile_account_id) != str(account_id):
                LOGGER.debug(
                    "Billing profile %s is associated with account '%s', expected '%s'; treating as missing.",
                    billing_profile_id,
                    profile_account_id,
                    account_id,
                )
                billing_profile_id = None

        if not billing_profile_id:
            LOGGER.info("No billing profile found for account '%s'; creating one.", account_name)
            billing_profile_response: Optional[Dict] = None
            try:
                billing_profile_response = create_billing_profile(
                    api_base,
                    session_id,
                    account_id,
                    account_name,
                    currency_code,
                    start_date_value,
                    bill_to_value,
                )
            except ApiTimeout as timeout_exc:
                LOGGER.warning("Billing profile creation timed out; verifying record: %s", timeout_exc)
            except Exception as exc:
                LOGGER.error("Billing profile creation failed for account '%s': %s", account_name, exc)
                continue
            else:
                create_resp = billing_profile_response.get("createResponse") or []
                profile_entry = create_resp[0] if create_resp else {}
                error_code = str(profile_entry.get("ErrorCode") or profile_entry.get("errorcode") or "0").strip()
                if error_code and error_code != "0":
                    LOGGER.error(
                        "Billing profile creation returned error code %s for account '%s': %s",
                        error_code,
                        account_name,
                        profile_entry.get("ErrorText") or profile_entry.get("errortext") or "",
                    )
                    LOGGER.debug("Billing profile creation response: %s", json.dumps(billing_profile_response))
                    continue
                billing_profile_id = profile_entry.get("Id") or profile_entry.get("id")
                if billing_profile_id:
                    LOGGER.info("Billing profile %s created for account '%s'", billing_profile_id, account_name)
                    LOGGER.debug("Billing profile create response: %s", json.dumps(billing_profile_response))
                    poll_attempts = 0
                    while poll_attempts < 5:
                        profiles = find_billing_profiles(api_base, session_id, account_id)
                        if profiles:
                            profile_record = profiles[0] or {}
                            resolved_id = profile_record.get("Id") or profile_record.get("id")
                            if resolved_id:
                                billing_profile_id = resolved_id
                                break
                        poll_attempts += 1
                        LOGGER.debug(
                            "Waiting for billing profile %s to become queryable (attempt %d)",
                            billing_profile_id,
                            poll_attempts,
                        )
                        time.sleep(1)
            if not billing_profile_id:
                existing_profiles = find_billing_profiles(api_base, session_id, account_id)
                if existing_profiles:
                    profile_record = existing_profiles[0] or {}
                    billing_profile_id = profile_record.get("Id") or profile_record.get("id")

        if not billing_profile_id:
            LOGGER.error(
                "Unable to determine billing profile Id for account '%s'. Skipping contract group '%s'.",
                account_name,
                contract_key,
            )
            continue

        try:
            update_response = update_account_billing_profile(
                api_base,
                session_id,
                account_id,
                account_name,
                billing_profile_id,
                account_record,
            )
        except ApiTimeout as timeout_exc:
            LOGGER.warning("Account update for billing profile timed out; verifying record: %s", timeout_exc)
        except Exception as exc:
            LOGGER.error(
                "Failed to update account '%s' with billing profile %s: %s",
                account_name,
                billing_profile_id,
                exc,
            )
            continue
        else:
            update_entries = (
                update_response.get("updateResponse")
                or update_response.get("saveResponse")
                or update_response.get("createResponse")
                or []
            )
            if update_entries:
                update_entry = update_entries[0] or {}
                error_code = str(update_entry.get("ErrorCode") or update_entry.get("errorcode") or "0").strip()
                if error_code and error_code != "0":
                    LOGGER.error(
                        "Updating account '%s' with billing profile %s returned error code %s: %s",
                        account_name,
                        billing_profile_id,
                        error_code,
                        update_entry.get("ErrorText") or update_entry.get("errortext") or "",
                    )
                    LOGGER.debug("Account update response payload: %s", json.dumps(update_response))
                    continue

        try:
            account_lookup = perform_lookup(api_base, session_id, build_account_query(account_name))
        except Exception as exc:
            LOGGER.error("Account lookup after billing profile update failed for '%s': %s", account_name, exc)
            continue
        account_records = account_lookup.get("queryResponse", []) or []
        account_record = account_records[0] if account_records else {}
        verified_profile_id = (
            (account_record or {}).get("BillableBillingProfileId")
            or (account_record or {}).get("billablebillingprofileid")
        )
        LOGGER.debug("Account record after update: %s", account_record)
        if not verified_profile_id:
            LOGGER.error(
                "Unable to verify billing profile association for account '%s'. Skipping contract group '%s'.",
                account_name,
                contract_key,
            )
            continue
        billing_profile_id = verified_profile_id

        LOGGER.info("Account '%s' associated with billing profile %s", account_name, billing_profile_id)
        time.sleep(2)

        try:
            contract_number = next_contract_number(api_base, session_id, start_date_value)
        except Exception as exc:
            LOGGER.error(
                "Unable to determine next contract number for contract '%s' (account '%s'): %s",
                contract_key,
                account_name,
                exc,
            )
            continue

        LOGGER.info(
            "Creating contract %s for account %s (contract group '%s')",
            contract_number,
            account_id,
            contract_key,
        )
        try:
            contract_response = create_contract(
                api_base,
                session_id,
                account_id,
                contract_number,
                start_date_value,
                contract_status,
                cpq_contract_id,
            )
        except Exception as exc:
            LOGGER.error(
                "Contract creation failed for group '%s' (account '%s'): %s",
                contract_key,
                account_name,
                exc,
            )
            continue

        create_resp = contract_response.get("createResponse") or []
        contract_id = None
        if create_resp:
            contract_id = create_resp[0].get("Id")
        if not contract_id:
            LOGGER.error(
                "Contract creation returned no Id for group '%s': %s",
                contract_key,
                contract_response,
            )
            continue

        created_contract_currencies: Dict[str, Dict] = {}
        LOGGER.info("Creating contract currency %s for contract %s", currency_code, contract_id)
        try:
            currency_response = create_contract_currency(api_base, session_id, contract_id, currency_code)
            created_contract_currencies[currency_code] = currency_response
        except Exception as exc:
            LOGGER.error(
                "Contract currency creation failed for group '%s' currency '%s': %s",
                contract_key,
                currency_code,
                exc,
            )
            continue

        billing_identifier_response: Optional[Dict] = None
        billing_identifier_name = (args.billing_identifier_product_name or "").strip()
        if billing_identifier_name:
            billing_product_id = get_product_id(billing_identifier_name)
            if billing_product_id:
                LOGGER.info(
                    "Creating billing identifier account product for contract %s (%s)",
                    contract_id,
                    billing_identifier_name,
                )
                try:
                    billing_identifier_response = create_account_product(
                        api_base,
                        session_id,
                        account_id,
                        contract_id,
                        billing_product_id,
                        start_date_value,
                        1,
                        first_row.get(args.account_product_status_column) or "Active",
                        extra_fields={"BillIdent": contract_number},
                    )
                except ApiTimeout as timeout_exc:
                    LOGGER.warning(
                        "Billing identifier account product creation timed out; verifying record: %s",
                        timeout_exc,
                    )
                    records = find_account_product(api_base, session_id, contract_id, billing_product_id)
                    if records:
                        billing_identifier_response = {"queryResponse": records}
                    else:
                        LOGGER.error(
                            "Billing identifier account product not found after timeout for contract '%s'",
                            contract_key,
                        )
                except Exception as exc:
                    LOGGER.error(
                        "Billing identifier account product creation failed for contract '%s': %s",
                        contract_key,
                        exc,
                    )
            else:
                LOGGER.warning(
                    "Billing identifier product '%s' not found; skipping for contract '%s'",
                    billing_identifier_name,
                    contract_key,
                )

        product_summaries: List[str] = []
        processed_currencies: Set[str] = set(created_contract_currencies.keys())

        pricing_only_column = args.pricing_only_column
        bundle_component_column = args.bundle_component_column
        product_groups: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
        for row in grouped_rows:
            product_name = row.get(args.product_column)
            row_currency_code = row.get(args.currency_column) or currency_code
            if not product_name:
                LOGGER.warning("Skipping row in contract '%s' with missing product name: %s", contract_key, row)
                continue
            key = (product_name, row_currency_code)
            product_groups.setdefault(key, []).append(row)

        for (product_name, product_currency_code), product_rows in product_groups.items():
            product_id = get_product_id(product_name)
            if not product_id:
                continue

            if product_currency_code not in processed_currencies:
                LOGGER.info("Creating additional contract currency %s for contract %s", product_currency_code, contract_id)
                try:
                    currency_response = create_contract_currency(api_base, session_id, contract_id, product_currency_code)
                    created_contract_currencies[product_currency_code] = currency_response
                    processed_currencies.add(product_currency_code)
                except Exception as exc:
                    LOGGER.error(
                        "Contract currency creation failed for contract '%s' currency '%s': %s",
                        contract_key,
                        product_currency_code,
                        exc,
                    )
                    continue

            bundle_rows = [r for r in product_rows if parse_bool(r.get(bundle_component_column))]
            pricing_driving_rows = [r for r in product_rows if not parse_bool(r.get(bundle_component_column))]
            non_pricing_only_rows = [r for r in pricing_driving_rows if not parse_bool(r.get(pricing_only_column))]
            contract_rate_response: Optional[Dict] = None
            contract_rate_id: Optional[str] = None

            if not pricing_driving_rows:
                LOGGER.info(
                    "All rows for product '%s' in contract '%s' are bundle components; skipping contract rate and pricing.",
                    product_name,
                    contract_key,
                )
            elif non_pricing_only_rows:
                LOGGER.info("Creating contract rate for contract %s, product %s", contract_id, product_id)
                try:
                    contract_rate_response = create_contract_rate(api_base, session_id, contract_id, product_id)
                except ApiTimeout as timeout_exc:
                    LOGGER.warning(
                        "Contract rate creation for '%s' timed out; verifying record: %s",
                        product_name,
                        timeout_exc,
                    )
                    records = find_contract_rate(api_base, session_id, contract_id, product_id)
                    if not records:
                        LOGGER.error(
                            "Contract rate not found after timeout for product '%s' in contract '%s'",
                            product_name,
                            contract_key,
                        )
                    else:
                        contract_rate_response = {"queryResponse": records}
                        contract_rate_id = records[0].get("Id") or records[0].get("id")
                except Exception as exc:
                    LOGGER.error(
                        "Contract rate creation failed for product '%s' in contract '%s': %s",
                        product_name,
                        contract_key,
                        exc,
                    )
                else:
                    rate_create_resp = contract_rate_response.get("createResponse") or []
                    if rate_create_resp:
                        contract_rate_id = rate_create_resp[0].get("Id")
                    if not contract_rate_id:
                        LOGGER.error(
                            "Contract rate creation returned no Id for product '%s' in contract '%s': %s",
                            product_name,
                            contract_key,
                            contract_rate_response,
                        )
            else:
                existing_rates = find_contract_rate(api_base, session_id, contract_id, product_id)
                contract_rate_response = {"queryResponse": existing_rates}
                contract_rate_id = existing_rates[0].get("Id") or existing_rates[0].get("id") if existing_rates else None
                if not contract_rate_id:
                    LOGGER.error(
                        "Pricing-only rows for product '%s' but no contract rate exists (contract '%s'). Skipping pricing creation.",
                        product_name,
                        contract_key,
                    )
                    continue

            if pricing_driving_rows and not contract_rate_id:
                LOGGER.error(
                    "No contract rate Id resolved for product '%s' in contract '%s'; skipping product.",
                    product_name,
                    contract_key,
                )
                continue

            # Account products for non-pricing-only rows (rate-only still skips).
            account_product_responses: List[Dict] = []
            for row in non_pricing_only_rows:
                row_start_date = parse_date(row.get(args.start_date_column), fallback=start_date_value)
                account_product_status = row.get(args.account_product_status_column) or "Active"
                quantity_value = parse_quantity(row.get(args.quantity_column))
                rate_only_mode = parse_bool(row.get(rate_only_column))
                if rate_only_mode:
                    LOGGER.info(
                        "Skipping account product creation for '%s' in contract %s (rate-only row)",
                        product_name,
                        contract_id,
                    )
                    account_product_responses.append(
                        {"skipped": True, "reason": "rate_only_mode", "product": product_name, "row": row}
                    )
                    continue

                LOGGER.info(
                    "Creating account product '%s' for contract %s (account %s)",
                    product_name,
                    contract_id,
                    account_id,
                )
                try:
                    response = create_account_product(
                        api_base,
                        session_id,
                        account_id,
                        contract_id,
                        product_id,
                        row_start_date,
                        quantity_value,
                        account_product_status,
                    )
                    account_product_responses.append(response)
                except ApiTimeout as timeout_exc:
                    LOGGER.warning(
                        "Account product creation for '%s' timed out; verifying record: %s",
                        product_name,
                        timeout_exc,
                    )
                    records = find_account_product(api_base, session_id, contract_id, product_id)
                    if records:
                        account_product_responses.append({"queryResponse": records})
                    else:
                        LOGGER.error(
                            "Account product '%s' not found after timeout for contract '%s'",
                            product_name,
                            contract_key,
                        )
                except Exception as exc:
                    LOGGER.error(
                        "Account product creation failed for '%s' in contract '%s': %s",
                        product_name,
                        contract_key,
                        exc,
                    )

            # Account products for bundle component rows (always create; no contract rate/pricing).
            for row in bundle_rows:
                row_start_date = parse_date(row.get(args.start_date_column), fallback=start_date_value)
                account_product_status = row.get(args.account_product_status_column) or "Active"
                quantity_value = parse_quantity(row.get(args.quantity_column))
                LOGGER.info(
                    "Creating bundle component account product '%s' for contract %s (account %s); skipping rate/pricing.",
                    product_name,
                    contract_id,
                    account_id,
                )
                try:
                    response = create_account_product(
                        api_base,
                        session_id,
                        account_id,
                        contract_id,
                        product_id,
                        row_start_date,
                        quantity_value,
                        account_product_status,
                    )
                    account_product_responses.append({"bundle_component": True, "response": response})
                except ApiTimeout as timeout_exc:
                    LOGGER.warning(
                        "Bundle component account product creation for '%s' timed out; verifying record: %s",
                        product_name,
                        timeout_exc,
                    )
                    records = find_account_product(api_base, session_id, contract_id, product_id)
                    if records:
                        account_product_responses.append({"bundle_component": True, "queryResponse": records})
                    else:
                        LOGGER.error(
                            "Bundle component account product '%s' not found after timeout for contract '%s'",
                            product_name,
                            contract_key,
                        )
                except Exception as exc:
                    LOGGER.error(
                        "Bundle component account product creation failed for '%s' in contract '%s': %s",
                        product_name,
                        contract_key,
                        exc,
                    )

            existing_pricing_entries = find_pricing_entries(api_base, session_id, contract_rate_id) if contract_rate_id else []
            existing_pricing_index: Dict[Tuple[str, Optional[Decimal], Optional[Decimal]], Dict] = {}
            pricing_payload_entries: List[Dict[str, str]] = []
            skipped_existing: List[Dict[str, Any]] = []
            canonical_tiers: List[Dict[str, Decimal]] = []

            if contract_rate_id:
                for entry in existing_pricing_entries:
                    entry_currency = entry.get("CurrencyCode") or entry.get("currencycode") or product_currency_code
                    lower_band_value = parse_pricing_band(entry.get("LowerBand") or entry.get("lowerband"))
                    upper_band_value = parse_pricing_band(entry.get("UpperBand") or entry.get("upperband"))
                    key = (entry_currency, lower_band_value, upper_band_value)
                    existing_pricing_index[key] = entry

                pricing_payload_entries, canonical_tiers, skipped_existing = build_pricing_payloads_from_rows(
                    contract_rate_id,
                    product_currency_code,
                    pricing_driving_rows,
                    args,
                    fallback_start_date=start_date_value,
                    existing_pricing_index=existing_pricing_index,
                    product_name=product_name,
                )

            pricing_responses: List[Dict] = []
            if pricing_payload_entries:
                LOGGER.info(
                    "Creating %d pricing records for product '%s' in a single batch (contract %s)",
                    len(pricing_payload_entries),
                    product_name,
                    contract_id,
                )
                try:
                    pricing_responses.append(
                        create_pricing_batch(api_base, session_id, pricing_payload_entries)
                    )
                except Exception as exc:
                    LOGGER.error(
                        "Batch pricing creation failed for product '%s' in contract '%s': %s",
                        product_name,
                        contract_key,
                        exc,
                    )

            tier_descriptions = []
            for idx, tier in enumerate(canonical_tiers):
                lower_text = format_decimal(tier["lower"])
                upper_text = "unlimited" if tier["upper"] is None else format_decimal(tier["upper"])
                tier_descriptions.append(
                    f"    Tier {idx + 1}: from {lower_text} to {upper_text} at rate {format_decimal(tier['rate'])}"
                )
            tiers_summary = "\n".join(tier_descriptions) if tier_descriptions else "    (no tiers defined or bundle component only)"

            product_summary = (
                f"Product '{product_name}' (currency {product_currency_code}):\n"
                f"  Contract rate Id: {contract_rate_id}\n"
                f"  Tier configuration (last processed row):\n{tiers_summary}\n"
                f"  Account product responses: {json.dumps(account_product_responses, indent=2)}\n"
                f"  Contract rate response: {json.dumps(contract_rate_response, indent=2)}\n"
                f"  Pricing responses: {json.dumps(pricing_responses or skipped_existing, indent=2)}"
            )
            product_summaries.append(product_summary)

        summary_lines = [
            f"\nContract group '{contract_key}' summary:",
            f"  Contract response: {json.dumps(contract_response, indent=2)}",
            f"  Contract currency responses: {json.dumps(created_contract_currencies, indent=2)}",
        ]
        if billing_identifier_response is not None:
            summary_lines.append(
                f"  Billing identifier response: {json.dumps(billing_identifier_response, indent=2)}"
            )
        summary_lines.extend(product_summaries)
        print("\n".join(summary_lines))


def process_amendments(rows: List[Dict[str, str]], args, api_base: str, session_id: str) -> None:
    """
    Dispatch amendment rows by action type.

    Supports:
      - 'Quantity Change'
      - 'Price Change'
      - 'Quantity and Price Change'  (does both, quantity first)
    """
    quantity_rows: List[Dict[str, str]] = []
    price_rows: List[Dict[str, str]] = []

    for row in rows:
        action = (row.get(args.action_column) or "").strip().lower()

        if action == "quantity change":
            quantity_rows.append(row)

        elif action == "price change":
            price_rows.append(row)

        elif action == "quantity and price change":
            LOGGER.info("Row flagged for both quantity and price change.")
            quantity_rows.append(row)
            price_rows.append(row)

        else:
            LOGGER.error("Unexpected action '%s' in amendment file; row skipped.", action)

    # Quantity first, then price.
    if quantity_rows:
        process_quantity_change_amendments(quantity_rows, args, api_base, session_id)

    if price_rows:
        process_price_change_amendments(price_rows, args, api_base, session_id)


def process_quantity_change_amendments(
    quantity_rows: List[Dict[str, str]], args, api_base: str, session_id: str
) -> None:
    for idx, row in enumerate(quantity_rows, start=1):
        handle_quantity_change(row, args, api_base, session_id, idx)


def process_price_change_amendments(price_rows: List[Dict[str, str]], args, api_base: str, session_id: str) -> None:
    groups: Dict[Tuple[str, str, str, str, str], List[Dict[str, str]]] = defaultdict(list)

    for row in price_rows:
        contract_name = (row.get(args.contract_group_column) or "").strip()
        account_name = (row.get(args.account_column) or "").strip()
        product_name = (row.get(args.product_column) or "").strip()
        currency_code = (row.get(args.currency_column) or "").strip()
        cpq_contract_id = (row.get(args.cpq_contract_id_column) or "").strip()

        key = (contract_name, account_name, product_name, currency_code, cpq_contract_id)
        groups[key].append(row)

    for key, rows_for_group in groups.items():
        apply_price_change_for_group(key, rows_for_group, args, api_base, session_id)


def apply_price_change_for_group(
    key: Tuple[str, str, str, str, str],
    rows_for_group: List[Dict[str, str]],
    args,
    api_base: str,
    session_id: str,
) -> None:
    contract_name, account_name, product_name, currency_code, cpq_contract_id = key

    def _sort_key(row: Dict[str, str]) -> date:
        sortable = row.get(args.effective_date_column) or row.get(args.start_date_column) or ""
        return parse_iso_to_date(sortable) or date.max

    rows_for_group = sorted(rows_for_group, key=_sort_key)

    if not currency_code:
        LOGGER.error("Price Change: currency missing for group %s. Skipping.", key)
        return

    if not cpq_contract_id:
        LOGGER.error("Price Change: CPQ contract id missing for group %s. Skipping.", key)
        return

    account_id = lookup_account_id_by_name(api_base, session_id, account_name)
    if not account_id:
        LOGGER.error("Price Change: account '%s' not found. Group %s skipped.", account_name, key)
        return

    contract_id = lookup_contract_id_by_name(api_base, session_id, contract_name, account_id, cpq_contract_id)
    if not contract_id:
        LOGGER.error(
            "Price Change: contract with CPQ id '%s' not found for account '%s'. Group %s skipped.",
            cpq_contract_id,
            account_name,
            key,
        )
        return

    product_id = get_product_id(product_name)
    if not product_id:
        LOGGER.error("Price Change: product '%s' not found. Group %s skipped.", product_name, key)
        return

    contract_rate_id = lookup_contract_rate_id(api_base, session_id, contract_id, product_id)
    if not contract_rate_id:
        LOGGER.error(
            "Price Change: no Contract Rate found for contract '%s' and product '%s'. Group %s skipped.",
            contract_name,
            product_name,
            key,
        )
        return

    apply_price_changes_to_contract_rate(
        contract_rate_id, currency_code, rows_for_group, args, api_base, session_id, key
    )


def apply_price_changes_to_contract_rate(
    contract_rate_id: str,
    currency_code: str,
    rows_for_group: List[Dict[str, str]],
    args,
    api_base: str,
    session_id: str,
    key: Tuple[str, str, str, str, str],
) -> None:
    contract_name, account_name, product_name, group_currency, cpq_contract_id = key
    if not rows_for_group:
        return

    first_row = rows_for_group[0]
    first_effective_str = first_row.get(args.effective_date_column) or first_row.get(args.start_date_column)
    first_effective_date = parse_iso_to_date(first_effective_str)
    if not first_effective_date:
        LOGGER.error(
            "Price Change: invalid effective date '%s' for group %s. Skipping group.",
            first_effective_str,
            key,
        )
        return

    cut_date = first_effective_date - timedelta(days=1)

    existing_pricing = fetch_pricing_for_contract_rate(api_base, session_id, contract_rate_id, currency_code)

    current_row: Optional[Dict[str, Any]] = None
    for pr in existing_pricing:
        eff = parse_iso_to_date(pr.get("EffectiveDate") or pr.get("effectivedate"))
        end = parse_iso_to_date(pr.get("EndDate") or pr.get("enddate"))
        if eff and eff <= first_effective_date and (end is None or end >= first_effective_date):
            current_row = pr
            break

    if current_row:
        eff = parse_iso_to_date(current_row.get("EffectiveDate") or current_row.get("effectivedate"))
        if eff and eff < first_effective_date:
            update_payload = {
                "Id": current_row.get("Id") or current_row.get("id"),
                "EndDate": f"{cut_date.isoformat()}T00:00:00.000Z",
            }
            try:
                update_pricing_record(api_base, session_id, update_payload)
                LOGGER.info(
                    "Price Change: shortened pricing %s to end on %s.",
                    update_payload["Id"],
                    cut_date.isoformat(),
                )
            except Exception as exc:
                LOGGER.error("Price Change: failed to shorten pricing %s: %s", update_payload["Id"], exc)
                return

    to_delete_ids: List[str] = []
    for pr in existing_pricing:
        eff = parse_iso_to_date(pr.get("EffectiveDate") or pr.get("effectivedate"))
        if eff and eff >= first_effective_date:
            pid = pr.get("Id") or pr.get("id")
            if pid:
                to_delete_ids.append(pid)

    if to_delete_ids:
        try:
            delete_pricing_batch(api_base, session_id, to_delete_ids)
            LOGGER.info(
                "Price Change: deleted %d future pricing rows for contract rate %s from %s onward.",
                len(to_delete_ids),
                contract_rate_id,
                first_effective_date.isoformat(),
            )
        except Exception as exc:
            LOGGER.error(
                "Price Change: failed to delete future pricing rows for contract rate %s: %s",
                contract_rate_id,
                exc,
            )
            return

    new_payloads, _, _ = build_pricing_payloads_from_rows(
        contract_rate_id,
        currency_code,
        rows_for_group,
        args,
        fallback_start_date=first_effective_date,
        existing_pricing_index=None,
        product_name=key[2],
    )

    if new_payloads:
        try:
            create_pricing_batch(api_base, session_id, new_payloads)
            LOGGER.info(
                "Price Change: created %d new pricing rows for (%s, %s, %s, %s).",
                len(new_payloads),
                contract_name,
                account_name,
                product_name,
                currency_code,
            )
        except Exception as exc:
            LOGGER.error(
                "Price Change: failed to create pricing rows for %s: %s",
                key,
                exc,
            )
    else:
        LOGGER.warning("Price Change: no tiers found in CSV for %s; nothing created.", key)


def handle_quantity_change(row, args, api_base, session_id, row_number):
    account_name = row.get(args.account_column, "").strip()
    product_name = row.get(args.product_column, "").strip()
    cpq_contract_id = row.get(args.cpq_contract_id_column, "").strip()
    quantity_str = row.get(args.quantity_column, "").strip()

    try:
        new_quantity = Decimal(quantity_str)
    except Exception:
        LOGGER.error("Row %s: invalid quantity '%s'. Skipping row.", row_number, quantity_str)
        return

    try:
        account_lookup = perform_lookup(api_base, session_id, build_account_query(account_name))
    except Exception as exc:
        LOGGER.error("Row %s: account lookup failed for '%s': %s", row_number, account_name, exc)
        return
    account_records = account_lookup.get("queryResponse", [])
    if not account_records:
        LOGGER.error("Row %s: account '%s' not found. Skipping row.", row_number, account_name)
        return
    account_id = account_records[0].get("Id")

    if not cpq_contract_id:
        LOGGER.error("Row %s: missing CPQ contract id. Skipping row.", row_number)
        return

    contract_id, contract_start_date = lookup_contract_id_by_cpq_id(api_base, session_id, cpq_contract_id, account_id)
    if not contract_id:
        LOGGER.error("Row %s: contract with CPQ id '%s' not found. Skipping row.", row_number, cpq_contract_id)
        return
    if not contract_start_date:
        LOGGER.error(
            "Row %s: contract '%s' missing start date; cannot set ContractModificationDate. Skipping row.",
            row_number,
            contract_id,
        )
        return

    product_id = get_product_id(product_name)
    if not product_id:
        LOGGER.error("Row %s: product '%s' not found. Skipping row.", row_number, product_name)
        return

    ap_records = find_account_product(api_base, session_id, contract_id, product_id)
    if not ap_records:
        LOGGER.warning(
            "Row %s: no account product found for contract CPQ id '%s' product '%s'. Skipping.",
            row_number,
            cpq_contract_id,
            product_name,
        )
        return
    ap_id = ap_records[0]["Id"]

    ap_details = get_account_product(api_base, session_id, ap_id)
    if not ap_details:
        LOGGER.warning("Row %s: account product '%s' not found. Skipping.", row_number, ap_id)
        return
    ap_detail = ap_details[0]
    status = ap_detail.get("Status") or ap_detail.get("status")
    if str(status).upper() != "ACTIVE":
        LOGGER.warning(
            "Row %s: AP '%s' not ACTIVE (Status=%s). Skipping.",
            row_number,
            ap_id,
            status,
        )
        return

    payload = {
        "Id": ap_id,
        "Quantity": str(new_quantity),
        "ContractModificationDate": contract_start_date,
    }
    base_url = api_base.rstrip("/") + "/"
    update_url = urljoin(base_url, f"ACCOUNT_PRODUCT/{ap_id}")
    LOGGER.info(
        "Row %s: sending quantity update call to %s with payload %s",
        row_number,
        update_url,
        {"brmObjects": payload},
    )
    try:
        update_response = update_account_product(api_base, session_id, payload)
    except Exception as exc:
        LOGGER.error("Row %s: quantity update failed for AP %s: %s", row_number, ap_id, exc)
        return
    LOGGER.info("Row %s: update response for AP %s: %s", row_number, ap_id, update_response)

    LOGGER.info("Row %s: Updated quantity for AccountProduct %s to %s", row_number, ap_id, new_quantity)


if __name__ == "__main__":
    main()
