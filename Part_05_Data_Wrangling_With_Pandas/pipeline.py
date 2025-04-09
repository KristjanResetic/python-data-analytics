"""Web data Wrangling pipeline."""

from __future__ import annotations

import ipaddress
from pathlib import Path

import pandas as pd
from user_agents import parse

MAIN_DATA_DIR = Path(__file__).parent.parent / "data" / "project"
MERGED_CSV_LOGS_PATH = MAIN_DATA_DIR / "merged_logs.csv"
IP_METADATA_PATH = MAIN_DATA_DIR / "ip_info.csv"
FINAL_DATA_PATH = MAIN_DATA_DIR / "web_logs_merged.parquet"
IP_REGEX = r"^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$"


def is_public_ip(ip: str) -> bool:
    """Check if an IP address is public."""
    try:
        return ipaddress.ip_address(ip).is_global
    except ValueError:
        return False


def extract_metadata_from_user_agent(user_agent_string: str) -> tuple[str | None, str | None, str | None, bool, bool, bool]:
    """Extract metadata from user agent string."""
    user_agent = parse(user_agent_string)
    return (user_agent.browser.family, user_agent.os.family, user_agent.device.family, user_agent.is_mobile, user_agent.is_pc, user_agent.is_bot)


def load_web_logs(path: Path) -> pd.DataFrame:
    """Load web logs from CSV file."""
    return pd.read_csv(path, parse_dates=["timestamp"])


def web_logs_remove_and_clean_invalid_rows(web_logs: pd.DataFrame) -> pd.DataFrame:
    """Remove invalid rows from web logs."""
    # src_ip cleaning
    web_logs = web_logs[web_logs["src_ip"].str.match(IP_REGEX)]
    web_logs = web_logs[web_logs["src_ip"].apply(is_public_ip)]
    # http_method cleaning
    valid_http_methods = ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"]
    web_logs = web_logs[web_logs["http_method"].isin(valid_http_methods)]
    # http_path cleaning
    web_logs["http_url_path"] = web_logs["http_path"].str.split("?").str[0]
    web_logs = web_logs.drop(columns=["http_path"])
    # referrer cleaning
    domain_regex = r"https?://([^/]+)"
    web_logs["referrer"] = web_logs["referrer"].str.extract(domain_regex)
    # user_agent cleaning
    web_logs[["browser", "os", "device", "is_mobile", "is_pc", "is_bot"]] = web_logs["user_agent"].apply(
        lambda x: pd.Series(extract_metadata_from_user_agent(x)),
    )
    return web_logs.drop(columns=["user_agent"])


def web_logs_optimize_types(web_logs: pd.DataFrame) -> pd.DataFrame:
    """Optimize data types of web logs."""
    web_logs["src_ip"] = web_logs["src_ip"].astype("string")
    web_logs["http_method"] = web_logs["http_method"].astype("category")
    web_logs["http_url_path"] = web_logs["http_url_path"].astype("string")
    web_logs["http_protocol_version"] = web_logs["http_protocol_version"].astype("category")
    web_logs["http_status_code"] = web_logs["http_status_code"].astype("category")
    web_logs["http_response_size"] = pd.to_numeric(web_logs["http_response_size"], downcast="integer", errors="raise")
    web_logs["referrer"] = web_logs["referrer"].astype("string")
    web_logs["browser"] = web_logs["browser"].astype("category")
    web_logs["os"] = web_logs["os"].astype("category")
    web_logs["device"] = web_logs["device"].astype("category")
    return web_logs


def load_and_clean_ip_metadata(path: Path) -> pd.DataFrame:
    """Load IP metadata from CSV file."""
    ip_metadata = pd.read_csv(path)
    ip_metadata["ip"] = ip_metadata["ip"].astype("string")
    ip_metadata["rir"] = ip_metadata["rir"].astype("category")
    ip_metadata["is_crawler"] = ip_metadata["is_crawler"].replace({"False": False, "True": True})
    ip_metadata["is_crawler"] = ip_metadata["is_crawler"].astype("bool")
    ip_metadata["company_abuser_level"] = ip_metadata["company_abuser_score"].str.split(r"\s\(", regex=True).str[1].str.replace(r"\)", "", regex=True).astype("category")
    ip_metadata["company_abuser_score"] = ip_metadata["company_abuser_score"].str.split(" ").str[0].astype("float")
    ip_metadata["datacenter_name"] = ip_metadata["datacenter_name"].astype("string")
    ip_metadata["company_name"] = ip_metadata["company_name"].astype("string")
    ip_metadata["company_type"] = ip_metadata["company_type"].astype("string")
    ip_metadata["country"] = ip_metadata["country"].astype("string")
    ip_metadata["city"] = ip_metadata["city"].astype("string")
    return ip_metadata


if __name__ == "__main__":
    print("Loading web logs...")
    web_logs = load_web_logs(MERGED_CSV_LOGS_PATH)
    web_logs = web_logs_remove_and_clean_invalid_rows(web_logs)
    web_logs = web_logs_optimize_types(web_logs)
    print("Web logs loaded and cleaned.")
    print("Loading IP metadata...")
    ip_metadata = load_and_clean_ip_metadata(IP_METADATA_PATH)
    # Merge web logs with IP metadata
    print("Merging web logs with IP metadata...")
    web_logs_merged = pd.merge(left=web_logs, right=ip_metadata, how="left", left_on="src_ip", right_on="ip")  # noqa: PD015
    web_logs_merged.to_parquet(MAIN_DATA_DIR / "web_logs_merged.parquet", index=False)
    print(web_logs_merged.info())
    print("Web logs merged and saved to Parquet.")
