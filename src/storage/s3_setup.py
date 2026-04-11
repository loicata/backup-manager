"""S3 Object Lock provisioning — bucket creation and configuration.

Used by the setup wizard to create a new S3 bucket with:
- Object Lock enabled (must be set at bucket creation time)
- Default Compliance retention
- Lifecycle rule for automatic cleanup after lock expiration

All operations use boto3 and require valid IAM credentials with
permissions for s3:CreateBucket, s3:PutObjectLockConfiguration,
s3:PutLifecycleConfiguration, and s3:PutBucketVersioning.
"""

import logging

logger = logging.getLogger(__name__)

# Minimum IAM policy required for Object Lock setup + backup operations.
# Displayed in the wizard so the user can create a dedicated IAM user.
REQUIRED_IAM_POLICY = """\
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:PutBucketVersioning",
        "s3:PutBucketObjectLockConfiguration",
        "s3:GetBucketObjectLockConfiguration",
        "s3:PutLifecycleConfiguration",
        "s3:GetLifecycleConfiguration",
        "s3:ListBucket",
        "s3:ListAllMyBuckets",
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:PutObject",
        "s3:PutObjectRetention",
        "s3:GetObjectRetention",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::*",
        "arn:aws:s3:::*/*"
      ]
    }
  ]
}"""

# AWS S3 Glacier Instant Retrieval pricing per GB/month by region (USD).
# Source: https://aws.amazon.com/s3/pricing/ (indicative, April 2026).
GLACIER_IR_PRICE_PER_GB: dict[str, float] = {
    "us-east-1": 0.004,
    "us-east-2": 0.004,
    "us-west-1": 0.0044,
    "us-west-2": 0.004,
    "eu-west-1": 0.004,
    "eu-west-2": 0.0044,
    "eu-west-3": 0.0044,
    "eu-central-1": 0.0044,
    "eu-central-2": 0.0048,
    "eu-north-1": 0.004,
    "eu-south-1": 0.0046,
    "eu-south-2": 0.0046,
    "ap-southeast-1": 0.005,
    "ap-southeast-2": 0.005,
    "ap-northeast-1": 0.005,
    "ap-northeast-2": 0.0048,
    "ap-south-1": 0.0045,
    "ca-central-1": 0.0044,
    "sa-east-1": 0.0068,
    "me-south-1": 0.0054,
    "af-south-1": 0.0054,
}

# Retention durations offered in the wizard.
# (label, months, days)
RETENTION_OPTIONS: list[tuple[str, int, int]] = [
    ("1 month", 1, 30),
    ("4 months", 4, 120),
    ("13 months", 13, 395),
    ("7 years", 84, 2555),
    ("13 years", 156, 4745),
]


# Timezone → AWS region mapping for auto-detection fallback.
_TZ_TO_REGION: dict[str, str] = {
    # Europe
    "Europe/Dublin": "eu-west-1",
    "Europe/London": "eu-west-2",
    "Europe/Paris": "eu-west-3",
    "Europe/Berlin": "eu-central-1",
    "Europe/Zurich": "eu-central-2",
    "Europe/Stockholm": "eu-north-1",
    "Europe/Rome": "eu-south-1",
    "Europe/Madrid": "eu-south-2",
    "Europe/Amsterdam": "eu-west-3",
    "Europe/Brussels": "eu-west-3",
    "Europe/Vienna": "eu-central-1",
    "Europe/Warsaw": "eu-central-1",
    "Europe/Prague": "eu-central-1",
    "Europe/Lisbon": "eu-west-1",
    "Europe/Helsinki": "eu-north-1",
    "Europe/Oslo": "eu-north-1",
    "Europe/Copenhagen": "eu-north-1",
    "Europe/Bucharest": "eu-central-1",
    "Europe/Athens": "eu-south-1",
    # US
    "America/New_York": "us-east-1",
    "America/Chicago": "us-east-2",
    "America/Denver": "us-west-1",
    "America/Los_Angeles": "us-west-2",
    "US/Eastern": "us-east-1",
    "US/Central": "us-east-2",
    "US/Mountain": "us-west-1",
    "US/Pacific": "us-west-2",
    # Asia-Pacific
    "Asia/Tokyo": "ap-northeast-1",
    "Asia/Seoul": "ap-northeast-2",
    "Asia/Singapore": "ap-southeast-1",
    "Australia/Sydney": "ap-southeast-2",
    "Asia/Kolkata": "ap-south-1",
    "Asia/Calcutta": "ap-south-1",
    # Americas
    "America/Toronto": "ca-central-1",
    "America/Sao_Paulo": "sa-east-1",
    # Middle East / Africa
    "Asia/Dubai": "me-south-1",
    "Africa/Johannesburg": "af-south-1",
}

# UTC offset (hours) → AWS region fallback when timezone name is unknown.
_UTC_OFFSET_TO_REGION: dict[int, str] = {
    -8: "us-west-2",
    -7: "us-west-1",
    -6: "us-east-2",
    -5: "us-east-1",
    -4: "ca-central-1",
    -3: "sa-east-1",
    0: "eu-west-1",
    1: "eu-west-3",
    2: "eu-central-1",
    3: "me-south-1",
    4: "me-south-1",
    5: "ap-south-1",
    8: "ap-southeast-1",
    9: "ap-northeast-1",
    10: "ap-southeast-2",
}


def detect_nearest_region() -> str:
    """Detect the nearest AWS region based on IP geolocation or timezone.

    Tries IP geolocation first (requires internet), falls back to
    the local system timezone if the network call fails.

    Returns:
        AWS region code (e.g. "eu-west-1"). Defaults to "eu-west-1"
        if detection fails entirely.
    """
    # Try IP geolocation first
    region = _detect_region_by_ip()
    if region:
        return region

    # Fallback to timezone
    return _detect_region_by_timezone()


def _detect_region_by_ip() -> str:
    """Detect region via free IP geolocation API.

    Returns:
        AWS region code, or empty string on failure.
    """
    import json
    import urllib.request

    try:
        req = urllib.request.Request(
            "http://ip-api.com/json/?fields=lat,lon,timezone",
            headers={"User-Agent": "BackupManager"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        # Try timezone from response
        tz = data.get("timezone", "")
        if tz and tz in _TZ_TO_REGION:
            logger.info(
                "Region detected by IP geolocation timezone: %s → %s", tz, _TZ_TO_REGION[tz]
            )
            return _TZ_TO_REGION[tz]

        # Try latitude-based mapping
        lat = data.get("lat", 0)
        lon = data.get("lon", 0)
        region = _region_from_coords(lat, lon)
        if region:
            logger.info("Region detected by IP coordinates: (%.1f, %.1f) → %s", lat, lon, region)
            return region
    except Exception as e:
        logger.debug("IP geolocation failed: %s", e)

    return ""


def _region_from_coords(lat: float, lon: float) -> str:
    """Map latitude/longitude to the nearest AWS region.

    Args:
        lat: Latitude in degrees.
        lon: Longitude in degrees.

    Returns:
        AWS region code, or empty string if unmappable.
    """
    # Simple geographic zones
    if 35 <= lat <= 72 and -12 <= lon <= 40:
        # Europe
        if lon < 0:
            return "eu-west-1"  # Western Europe
        if lon < 5:
            return "eu-west-3"  # France/Benelux
        if lon < 20:
            return "eu-central-1"  # Central Europe
        return "eu-north-1"  # Eastern/Northern Europe
    if 25 <= lat <= 50 and -130 <= lon <= -60:
        # North America
        if lon < -100:
            return "us-west-2"
        if lon < -85:
            return "us-east-2"
        return "us-east-1"
    if -35 <= lat <= 10 and -80 <= lon <= -35:
        return "sa-east-1"  # South America
    if 0 <= lat <= 40 and 60 <= lon <= 145:
        # Asia-Pacific
        if lon > 120:
            return "ap-northeast-1"
        if lon > 95:
            return "ap-southeast-1"
        return "ap-south-1"
    if -45 <= lat <= -10 and 110 <= lon <= 180:
        return "ap-southeast-2"  # Australia
    return ""


def _detect_region_by_timezone() -> str:
    """Detect region from the local system timezone.

    Returns:
        AWS region code. Defaults to "eu-west-1" if unknown.
    """
    import time

    # Try to get IANA timezone name
    try:
        tz_name = time.tzname[0]
        # On Windows, tzname is like "Romance Standard Time" — not IANA.
        # Try datetime to get UTC offset instead.
    except Exception:
        tz_name = ""

    # Check IANA name first
    for tz, region in _TZ_TO_REGION.items():
        if tz_name and tz_name in tz:
            logger.info("Region detected by timezone name: %s → %s", tz_name, region)
            return region

    # Fall back to UTC offset
    try:
        offset_seconds = -time.timezone if time.daylight == 0 else -time.altzone
        offset_hours = round(offset_seconds / 3600)
        region = _UTC_OFFSET_TO_REGION.get(offset_hours, "eu-west-1")
        logger.info("Region detected by UTC offset: %+d → %s", offset_hours, region)
        return region
    except Exception:
        return "eu-west-1"


def detect_local_currency() -> tuple[str, float]:
    """Detect local currency and USD exchange rate.

    Uses free API to get the exchange rate from USD to local currency.

    Returns:
        (currency_symbol, rate) e.g. ("EUR", 0.92) or ("$", 1.0) on failure.
    """
    import json
    import urllib.request

    # Try to detect currency from locale
    currency_map = {
        "EUR": "\u20ac",
        "GBP": "\u00a3",
        "JPY": "\u00a5",
        "CNY": "\u00a5",
        "KRW": "\u20a9",
        "INR": "\u20b9",
        "BRL": "R$",
        "CAD": "CA$",
        "AUD": "AU$",
        "CHF": "CHF",
    }

    try:
        # Detect currency via IP geolocation
        req = urllib.request.Request(
            "http://ip-api.com/json/?fields=currency",
            headers={"User-Agent": "BackupManager"},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        currency_code = data.get("currency", "USD")

        if currency_code == "USD":
            return "$", 1.0

        # Get exchange rate
        rate_req = urllib.request.Request(
            "https://open.er-api.com/v6/latest/USD",
            headers={"User-Agent": "BackupManager"},
        )
        with urllib.request.urlopen(rate_req, timeout=5) as resp:
            rate_data = json.loads(resp.read().decode("utf-8"))

        rates = rate_data.get("rates", {})
        rate = rates.get(currency_code, 1.0)
        symbol = currency_map.get(currency_code, currency_code)

        logger.info("Currency detected: %s (rate: %.4f)", currency_code, rate)
        return symbol, rate

    except Exception as e:
        logger.debug("Currency detection failed: %s", e)
        return "$", 1.0


def _fmt_number(n: int) -> str:
    """Format an integer with space as thousands separator (e.g. 1 000)."""
    s = str(n)
    result = []
    for i, ch in enumerate(reversed(s)):
        if i > 0 and i % 3 == 0:
            result.append(" ")
        result.append(ch)
    return "".join(reversed(result))


def format_cost(usd_amount: float, local_symbol: str, local_rate: float) -> str:
    """Format a USD cost with optional local currency.

    Uses space as thousands separator. No decimals.

    Args:
        usd_amount: Cost in USD.
        local_symbol: Local currency symbol.
        local_rate: Exchange rate from USD.

    Returns:
        Formatted string, e.g. "$53" or "$53 (~49 EUR)".
    """
    usd_rounded = round(usd_amount)
    usd_str = f"${_fmt_number(usd_rounded)}"
    if local_rate == 1.0 or local_symbol == "$":
        return usd_str
    local_amount = round(usd_amount * local_rate)
    return f"{usd_str} (~{_fmt_number(local_amount)} {local_symbol})"


def estimate_total_cost(
    data_gb: float,
    region: str,
    retention_months: int,
) -> float:
    """Estimate total cost over the full retention period.

    Models the progressive accumulation of backups with all AWS costs:

    Storage:
    - Each month: 1 new full backup (size = data_gb)
    - Each day: 1 differential backup (cumulative since last full)
    - Diffs grow linearly: day 1 ~2%, day 30 ~30%, average ~15%
    - Diffs are locked 1 month, so ~30 diffs stored at any time
    - Fulls accumulate over the retention period

    Glacier IR minimum storage (90 days):
    - Diffs deleted after 30 days are billed for 90 days minimum
    - Effective diff cost multiplier: 3x

    PUT requests:
    - ~5000 files per backup (realistic for personal data)
    - 31 backups/month × 5000 files = 155,000 PUTs/month
    - $0.02 per 1000 PUT requests

    LIST requests:
    - Verification and rotation: ~10,000 LISTs/month
    - $0.02 per 1000 LIST requests

    Args:
        data_gb: Size of source data in gigabytes.
        region: AWS region for pricing lookup.
        retention_months: How many months data is retained.

    Returns:
        Estimated total cost in USD over the retention period.
    """
    price_per_gb = GLACIER_IR_PRICE_PER_GB.get(region, 0.004)

    # Differential backups: cumulative changes since last full.
    # Pessimistic estimate: day 1 ~2%, day 30 ~50% modified.
    # Average diff size ~25% of data_gb over a 30-day cycle.
    avg_diff_gb = data_gb * 0.25
    # 30 diffs stored at any time, but Glacier IR 90-day minimum
    # means each diff is billed for 3 months even if deleted after 1.
    glacier_ir_min_storage_multiplier = 3
    effective_diffs_gb = avg_diff_gb * 30 * glacier_ir_min_storage_multiplier

    # PUT request costs: ~5000 files per backup, 31 backups/month
    files_per_backup = 5000
    backups_per_month = 31
    put_cost_per_month = backups_per_month * files_per_backup * 0.02 / 1000

    # GET request costs: verification reads back file metadata
    # ~5000 GETs per backup verification × 31 backups/month
    get_cost_per_month = backups_per_month * files_per_backup * 0.001 / 1000

    # LIST request costs: verification + rotation
    list_cost_per_month = 10 * 0.02 / 1000  # ~10,000 LISTs

    api_cost_per_month = put_cost_per_month + get_cost_per_month + list_cost_per_month

    total_cost = 0.0
    for month in range(1, retention_months + 1):
        # Fulls accumulate: month 1 = 1 full, month 2 = 2 fulls, ...
        nb_fulls = month
        full_gb = data_gb * nb_fulls

        # Total storage this month = accumulated fulls + effective diffs
        month_storage_gb = full_gb + effective_diffs_gb
        month_cost = month_storage_gb * price_per_gb + api_cost_per_month
        total_cost += month_cost

    return total_cost


class S3ObjectLockSetup:
    """Handles S3 bucket creation and Object Lock configuration.

    Each method returns (success: bool, message: str) for UI feedback.
    """

    def __init__(self, access_key: str, secret_key: str, region: str):
        if not access_key or not secret_key:
            raise ValueError("AWS credentials are required")
        if not region:
            raise ValueError("AWS region is required")
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region

    def _get_client(self):
        """Create a boto3 S3 client."""
        import boto3
        from botocore.config import Config

        config = Config(
            connect_timeout=30,
            read_timeout=60,
            retries={"max_attempts": 3, "mode": "adaptive"},
        )
        return boto3.client(
            "s3",
            region_name=self._region,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            config=config,
        )

    def validate_credentials(self) -> tuple[bool, str]:
        """Test IAM credentials by listing buckets.

        Returns:
            (True, success_message) or (False, error_message).
        """
        try:
            client = self._get_client()
            resp = client.list_buckets()
            count = len(resp.get("Buckets", []))
            return True, f"Credentials valid — {count} bucket(s) found"
        except Exception as e:
            logger.warning("Credential validation failed: %s", e)
            return False, f"Invalid credentials: {e}"

    def create_bucket(self, bucket_name: str) -> tuple[bool, str]:
        """Create an S3 bucket with Object Lock enabled.

        Object Lock can ONLY be enabled at bucket creation time.
        Versioning is automatically enabled by AWS when Object Lock
        is active.

        Args:
            bucket_name: Globally unique S3 bucket name.

        Returns:
            (True, success_message) or (False, error_message).
        """
        if not bucket_name or not bucket_name.strip():
            return False, "Bucket name is required"

        try:
            client = self._get_client()
            create_kwargs: dict = {
                "Bucket": bucket_name,
                "ObjectLockEnabledForBucket": True,
            }
            # us-east-1 does not accept LocationConstraint
            if self._region != "us-east-1":
                create_kwargs["CreateBucketConfiguration"] = {
                    "LocationConstraint": self._region,
                }
            client.create_bucket(**create_kwargs)
            logger.info(
                "Created bucket %s in %s with Object Lock",
                bucket_name,
                self._region,
            )
            return True, f"Bucket '{bucket_name}' created with Object Lock enabled"
        except Exception as e:
            logger.error("Failed to create bucket %s: %s", bucket_name, e)
            return False, f"Bucket creation failed: {e}"

    def configure_retention(
        self,
        bucket_name: str,
        mode: str,
        days: int,
    ) -> tuple[bool, str]:
        """Set default Object Lock retention on the bucket.

        Args:
            bucket_name: Target S3 bucket.
            mode: "COMPLIANCE" or "GOVERNANCE".
            days: Default retention period in days.

        Returns:
            (True, success_message) or (False, error_message).
        """
        if mode not in ("COMPLIANCE", "GOVERNANCE"):
            return False, f"Invalid mode: {mode}"
        if days < 1:
            return False, "Retention must be at least 1 day"

        try:
            client = self._get_client()
            client.put_object_lock_configuration(
                Bucket=bucket_name,
                ObjectLockConfiguration={
                    "ObjectLockEnabled": "Enabled",
                    "Rule": {
                        "DefaultRetention": {
                            "Mode": mode,
                            "Days": days,
                        },
                    },
                },
            )
            logger.info(
                "Configured %s retention (%d days) on %s",
                mode,
                days,
                bucket_name,
            )
            return True, f"{mode} retention set to {days} days"
        except Exception as e:
            logger.error("Failed to configure retention on %s: %s", bucket_name, e)
            return False, f"Retention configuration failed: {e}"

    def configure_lifecycle(
        self,
        bucket_name: str,
        expiration_days: int,
    ) -> tuple[bool, str]:
        """Set lifecycle rule to delete objects after lock expiration.

        The expiration_days should be >= the Object Lock retention
        period so objects are only deleted after unlocking.

        Args:
            bucket_name: Target S3 bucket.
            expiration_days: Days after creation to expire objects.

        Returns:
            (True, success_message) or (False, error_message).
        """
        if expiration_days < 1:
            return False, "Expiration must be at least 1 day"

        try:
            client = self._get_client()
            client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration={
                    "Rules": [
                        {
                            "ID": "backup-manager-cleanup",
                            "Status": "Enabled",
                            "Filter": {"Prefix": ""},
                            "NoncurrentVersionExpiration": {
                                "NoncurrentDays": expiration_days,
                            },
                            "Expiration": {
                                "Days": expiration_days,
                            },
                        },
                    ],
                },
            )
            logger.info(
                "Configured lifecycle expiration (%d days) on %s",
                expiration_days,
                bucket_name,
            )
            return True, f"Lifecycle rule set: auto-delete after {expiration_days} days"
        except Exception as e:
            logger.error("Failed to configure lifecycle on %s: %s", bucket_name, e)
            return False, f"Lifecycle configuration failed: {e}"

    def full_setup(
        self,
        bucket_name: str,
        retention_days: int,
        full_extra_days: int = 30,
    ) -> list[tuple[str, bool, str]]:
        """Run all provisioning steps in sequence.

        Args:
            bucket_name: Globally unique S3 bucket name.
            retention_days: Object Lock retention for diffs.
            full_extra_days: Extra retention for full backups.

        Returns:
            List of (step_name, success, message) for each step.
        """
        results: list[tuple[str, bool, str]] = []

        # Step 1: Create bucket with Object Lock
        ok, msg = self.create_bucket(bucket_name)
        results.append(("Create bucket", ok, msg))
        if not ok:
            return results

        # Step 2: Configure Compliance retention
        ok, msg = self.configure_retention(bucket_name, "COMPLIANCE", retention_days)
        results.append(("Configure retention", ok, msg))
        if not ok:
            return results

        # Step 3: Configure lifecycle for cleanup after lock expiration
        # Use the full backup lock duration (longest) + 1 day margin
        lifecycle_days = retention_days + full_extra_days + 1
        ok, msg = self.configure_lifecycle(bucket_name, lifecycle_days)
        results.append(("Configure lifecycle", ok, msg))
        if not ok:
            return results

        # Step 4: Verify connection
        try:
            client = self._get_client()
            client.head_bucket(Bucket=bucket_name)
            lock_config = client.get_object_lock_configuration(Bucket=bucket_name)
            rule = lock_config.get("ObjectLockConfiguration", {}).get("Rule", {})
            default_ret = rule.get("DefaultRetention", {})
            actual_days = default_ret.get("Days", 0)
            actual_mode = default_ret.get("Mode", "")
            results.append(
                (
                    "Verify configuration",
                    True,
                    f"Bucket ready: {actual_mode} mode, {actual_days} days retention",
                )
            )
        except Exception as e:
            results.append(("Verify configuration", False, f"Verification failed: {e}"))

        return results
