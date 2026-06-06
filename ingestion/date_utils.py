from datetime import datetime, timedelta, timezone


def get_target_date(kwargs=None):
    kwargs = kwargs or {}
    dag_run = kwargs.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    params = kwargs.get("params") or {}
    date_str = conf.get("target_date") or params.get("target_date") or kwargs.get("target_date")

    if not date_str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("target_date must use YYYY-MM-DD format, for example 2026-05-25") from exc

    return date_str


def _validate_date(date_str, field_name):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            f"{field_name} must use YYYY-MM-DD format, for example 2026-05-25"
        ) from exc


def get_target_dates(kwargs=None):
    kwargs = kwargs or {}
    dag_run = kwargs.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    params = kwargs.get("params") or {}

    start_str = (
        conf.get("target_start_date")
        or params.get("target_start_date")
        or kwargs.get("target_start_date")
    )
    end_str = (
        conf.get("target_end_date")
        or params.get("target_end_date")
        or kwargs.get("target_end_date")
    )

    if not start_str and not end_str:
        return [get_target_date(kwargs)]

    if not start_str or not end_str:
        raise ValueError(
            "target_start_date and target_end_date must be provided together "
            "for a date range"
        )

    start_date = _validate_date(start_str, "target_start_date")
    end_date = _validate_date(end_str, "target_end_date")
    if start_date > end_date:
        raise ValueError("target_start_date cannot be after target_end_date")

    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return dates


def target_datetime_iso(date_str, hour=12):
    return f"{date_str}T{hour:02d}:00:00+00:00"


def target_overpass_datetime(date_str, hour=12):
    return f"{date_str}T{hour:02d}:00:00Z"
