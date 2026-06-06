from datetime import datetime, timezone


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


def target_datetime_iso(date_str, hour=12):
    return f"{date_str}T{hour:02d}:00:00+00:00"


def target_overpass_datetime(date_str, hour=12):
    return f"{date_str}T{hour:02d}:00:00Z"
