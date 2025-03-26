"""Web data Acquisition Pipeline."""

from __future__ import annotations

import csv
import datetime
<<<<<<< HEAD
import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from loguru import logger

=======
<<<<<<<< HEAD:Part_04_Acquiring_Your_Data/pipeline.py
import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
========
import time
from pathlib import Path

import requests
>>>>>>>> 0af299c (moj2):Part_04_Acquiring_Your_Data/pipeline_moj2.py
from loguru import logger

# da daš api key v scripto in ti ga ne more na git nalozit da ti ga ukradejo
>>>>>>> 0af299c (moj2)
load_dotenv()

MAIN_DATA_DIR = Path(__file__).parent.parent / "data" / "project"
MERGED_RAW_LOGS_PATH = MAIN_DATA_DIR / "merged_raw_logs.txt"
MERGED_CSV_LOGS_PATH = MAIN_DATA_DIR / "merged_logs.csv"
<<<<<<< HEAD
IP_INFO_DATASET_PATH = MAIN_DATA_DIR / "ip_info_dataset.csv"
IP_API_IS_API_KEY = os.getenv("IP_API_IS_API_KEY")
=======
<<<<<<<< HEAD:Part_04_Acquiring_Your_Data/pipeline.py
IP_INFO_CSV = MAIN_DATA_DIR / "ip_info.csv"
IP_API_IS_API_KEY = os.getenv("IP_API_IS_API_KEY")
========
IP_INFO_DATASET_PATH = MAIN_DATA_DIR / "ip_info_dataset.csv"
IP_API_IS_API_KEY = "56f9b1e069f19feb"
>>>>>>>> 0af299c (moj2):Part_04_Acquiring_Your_Data/pipeline_moj2.py
>>>>>>> 0af299c (moj2)


def get_all_logs_paths(main_dir: Path, pattern: str) -> list[Path]:
    """Get all log files paths."""
    return list(main_dir.rglob(pattern))


def merge_logs_and_remove_empty_lines(logs_paths: list[Path], output_path: Path) -> None:
    """Merge all logs files and remove empty lines."""
    with output_path.open("w") as output_file:
        for log_path in logs_paths:
            logger.debug(f"--> Merging {log_path.name}.")
            with log_path.open("r") as log_file:
                for line in log_file:
                    if line.strip():
                        output_file.write(line)


def convert_raw_logs_to_csv(input_path: Path, output_path: Path) -> None:
    """Convert raw logs to CSV."""
    header = ["timestamp", "src_ip", "http_method", "http_path", "http_protocol_version", "http_status_code", "http_response_size", "referrer", "user_agent"]
    with input_path.open("r") as input_file, output_path.open("w", newline="") as output_file:
        csv_writer = csv.writer(output_file, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow(header)
        for line in input_file:
            try:
                line_splitted = line.split("[")
                timestamp = datetime.datetime.strptime(line_splitted[1].split("]")[0], "%d/%b/%Y:%H:%M:%S %z")
                line_splitted = line.split(" ")
                src_ip = line_splitted[0]
                http_method = line_splitted[5].replace('"', "")
                http_path = line_splitted[6]
                http_protocol_version = line_splitted[7].replace('"', "")
                http_status_code = int(line_splitted[8])
                http_response_size = int(line_splitted[9])
                line_splitted = line.split('"')
                referrer = line_splitted[3].strip()
                user_agent = line_splitted[5].strip()
            except ValueError as err:  # noqa: PERF203
                if "HTTP/1.1" not in line:
                    logger.warning(f"Malformed line, missing HTTP protocol version: {line}")
                    http_protocol_version = None
                    http_status_code = int(line_splitted[-4])
                    http_response_size = int(line_splitted[-3])
                    line_splitted = line.split('"')
                    referrer = line_splitted[3].strip()
                    user_agent = line_splitted[5].strip()
                else:
                    logger.error(f"Malformed line: {line}")
                    raise ValueError from err
            finally:
                csv_writer.writerow([timestamp, src_ip, http_method, http_path, http_protocol_version, http_status_code, http_response_size, referrer, user_agent])


def extract_unique_ips(input_path: Path) -> list[str]:
    """Extract unique IPs from logs."""
    unique_ips = set()
<<<<<<< HEAD
=======
<<<<<<<< HEAD:Part_04_Acquiring_Your_Data/pipeline.py
>>>>>>> 0af299c (moj2)
    with input_path.open("r") as input_file:
        for line in input_file:
            line_splitted = line.split(",")
            unique_ips.add(line_splitted[1])
<<<<<<< HEAD
    # print(unique_ips)
    return list(unique_ips)
=======
    return list(unique_ips)
========

    with input_path.open(newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            if len(row) >= 2:
                ip = row[1].strip()
                unique_ips.add(ip)
    # print(unique_ips)
    return sorted(unique_ips)
>>>>>>>> 0af299c (moj2):Part_04_Acquiring_Your_Data/pipeline_moj2.py
>>>>>>> 0af299c (moj2)


def generate_ip_info_dataset(unique_ips: list[str], output_path: Path) -> None:
    """Generate IP info dataset and save it to CSV."""
<<<<<<< HEAD
    # Definira glavo CSV datoteke z vsemi želenimi polji informacij o IP-jih
    header = [
=======
<<<<<<<< HEAD:Part_04_Acquiring_Your_Data/pipeline.py
    header = (
========
    # Definira glavo CSV datoteke z vsemi želenimi polji informacij o IP-jih
    header = [
>>>>>>>> 0af299c (moj2):Part_04_Acquiring_Your_Data/pipeline_moj2.py
>>>>>>> 0af299c (moj2)
        "ip",
        "rir",
        "is_mobile",
        "is_crawler",
        "is_datacenter",
        "is_tor",
        "is_proxy",
        "is_vpn",
        "is_abuser",
        "datacenter_name",
        "company_name",
        "company_abuser_score",
        "company_type",
        "country",
        "city",
        "latitude",
        "longitude",
<<<<<<< HEAD
    ]
=======
    )
    ip_data = []
    for unique_ip in unique_ips:
        try:
            url = f"https://api.ipapi.is?q={unique_ip}&key={IP_API_IS_API_KEY}"
            data = requests.get(url, timeout=15)
            if data.status_code == requests.codes.ok:
                logger.debug(f"---> Processing IP {unique_ip}.")
                data = data.json()
                ip = data.get("ip")
                if not ip:
                    logger.error(f"IP {unique_ip} not found in response.")
                    continue
                rir = data.get("rir")
                is_mobile = data.get("is_mobile")
                is_crawler = data.get("is_crawler")
                is_datacenter = data.get("is_datacenter")
                is_tor = data.get("is_tor")
                is_proxy = data.get("is_proxy")
                is_vpn = data.get("is_vpn")
                is_abuser = data.get("is_abuser")
                datacenter_name = data.get("datacenter", {}).get("datacenter")
                company_name = data.get("company", {}).get("name")
                company_abuser_score = data.get("company", {}).get("abuser_score")
                company_type = data.get("company", {}).get("type")
                country = data.get("location", {}).get("country")
                city = data.get("location", {}).get("city")
                latitude = data.get("location", {}).get("latitude")
                longitude = data.get("location", {}).get("longitude")
                ip_data.append(
                    (
                        ip,
                        rir,
                        is_mobile,
                        is_crawler,
                        is_datacenter,
                        is_tor,
                        is_proxy,
                        is_vpn,
                        is_abuser,
                        datacenter_name,
                        company_name,
                        company_abuser_score,
                        company_type,
                        country,
                        city,
                        latitude,
                        longitude,
                    ),
                )
            else:
                logger.error(f"Error while processing IP {unique_ip}: {data.status_code}. Details: {data.text}")
                continue
        except Exception as err:  # noqa: BLE001
            logger.error(f"Error while processing IP {unique_ip}: {err}")

    ip_data_df = pd.DataFrame(ip_data, columns=header)
    ip_data_df.to_csv(output_path, index=False)
>>>>>>> 0af299c (moj2)

    # Odpre datoteko za pisanje (z uporabo konteksta), s kodiranjem UTF-8
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)  # Ustvari CSV pisalnik
        writer.writerow(header)  # Zapiše glavo v prvo vrstico CSV-ja

        # Zanka po vseh podanih edinstvenih IP-jih
        for ip in unique_ips:
            # Sestavi URL za API klic z IP-jem in ključem
            url = f"https://api.ipapi.is?q={ip}&key={IP_API_IS_API_KEY}"
            logger.debug(f"Querying API for IP: {ip}")  # Zabeleži poizvedbo v dnevnik
            try:
                # Pošlje GET zahtevo na API
                response = requests.get(url, timeout=10)
                response.raise_for_status()  # Vrže napako, če status ni uspešen (2xx)
                data = response.json()  # Pretvori odgovor v slovar (JSON)

                # Izlušči potrebna polja iz odgovora
                row = [
                    data.get("ip", ""),
                    data.get("rir", ""),
                    data.get("is_mobile", ""),
                    data.get("is_crawler", ""),
                    data.get("is_datacenter", ""),
                    data.get("is_tor", ""),
                    data.get("is_proxy", ""),
                    data.get("is_vpn", ""),
                    data.get("is_abuser", ""),
                    data.get("datacenter", {}).get("datacenter"),  # Vzame ime podatkovnega centra, če obstaja
                    data.get("company", {}).get("name"),  # Ime podjetja, povezano z IP-jem
                    data.get("company", {}).get("abuser_score"),  # Ocena zlorabe podjetja
                    data.get("company", {}).get("type"),  # Tip podjetja (npr. ISP, gostitelj itd.)
                    data.get("location", {}).get("country"),  # Država iz lokacijskih podatkov
                    data.get("location", {}).get("city"),  # Mesto iz lokacijskih podatkov
                    data.get("location", {}).get("latitude"),  # Geografska širina
                    data.get("location", {}).get("longitude"),  # Geografska dolžina
                ]
                writer.writerow(row)  # Zapiše vrstico v CSV
                time.sleep(1)  # Upočasni poizvedbe (rate limiting)
            except Exception as e:
                # Če pride do napake (npr. omrežne ali JSON napake), zabeleži opozorilo
                logger.warning(f"Failed to retrieve API data for {ip}: {e}")


if __name__ == "__main__":
    logger.info("--------- Running Web Data Acquisition Pipeline ---------")
    raw_logs_paths = get_all_logs_paths(MAIN_DATA_DIR, "access.log.*")
    logger.info(f"--> Found {len(raw_logs_paths)} log files.")
    logger.info(f"--> Merging all logs into {MERGED_RAW_LOGS_PATH}.")
    merge_logs_and_remove_empty_lines(raw_logs_paths, MERGED_RAW_LOGS_PATH)
    logger.info(f"--> Converting raw logs into CSV: {MERGED_CSV_LOGS_PATH}.")
    convert_raw_logs_to_csv(MERGED_RAW_LOGS_PATH, MERGED_CSV_LOGS_PATH)
    logger.info("--> Extracting unique IPs from logs.")
    unique_ips = extract_unique_ips(MERGED_CSV_LOGS_PATH)
<<<<<<< HEAD
    logger.info(f"--> Generating IP info dataset for {len(unique_ips)} IPs.")
    generate_ip_info_dataset(unique_ips, IP_INFO_DATASET_PATH)
=======
<<<<<<<< HEAD:Part_04_Acquiring_Your_Data/pipeline.py
    logger.info(f"--> Found {len(unique_ips)} unique IPs. First 5: {unique_ips[:5]}")
    generate_ip_info_dataset(unique_ips, IP_INFO_CSV)
========
    logger.info(f"--> Generating IP info dataset for {len(unique_ips)} IPs.")
    generate_ip_info_dataset(unique_ips, IP_INFO_DATASET_PATH)
>>>>>>>> 0af299c (moj2):Part_04_Acquiring_Your_Data/pipeline_moj2.py
>>>>>>> 0af299c (moj2)
    logger.info("--------- Web Data Acquisition Pipeline Finished ---------")
