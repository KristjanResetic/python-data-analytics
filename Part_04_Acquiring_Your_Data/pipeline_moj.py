"""web data acqusition pipline"""

# če je kaj starejšega da zna python prebrati
from __future__ import annotations

import datetime
from pathlib import Path

# https://pypi.org/project/loguru/
from loguru import logger

# Path(__file__) pove lokacijo našega fila, dvakrat parent da grem dost gor po mapi in nato dol do želene mape
MAIN_DATA_DIR = Path(__file__).parent.parent / "data" / "project"
MERGED_RAW_LOGS_PATH = MAIN_DATA_DIR / "merged_raw_logs.txt"
MERGED_CSV_LOG_PATH = MAIN_DATA_DIR / "merged_logs.csv"


def get_all_logs_paths(main_dir: Path, pattern: str) -> list[Path]:
    """Get all log files path"""
    # prebere vse file, ki sldeijo tem peternu in jih vrne nazaj kot obliko list, ki vsebuje stringe. rglob posebna funkcija
    return list(main_dir.rglob(pattern))


def merge_logs_and_remove_empty_lines(logs_paths: list[Path], output_path: Path) -> None:
    """Merge all log files and remove empty lines"""
    with output_path.open("w") as output_file:
        for log_path in logs_paths:
            logger.debug(f"--> Merging {log_path.name}.")
            with log_path.open("r") as log_file:
                for line in log_file:
                    if line.strip():
                        output_file.write(line)


# korak 2 spremenimo v csv
def convert_raw_logs_to_csv(input_path: Path, output_path: Path) -> None:
    """Convert raw logs to CSV"""
    # csv in pisanje imata oba \n kar dvojno dela na enem moras dat stran
    with input_path.open("r") as input_file, output_path.open("w", newline="") as output_file:
        for line in input_file:
            line_splitted = line.split("[")
            timestamp = datetime.datetime.strptime(line_splitted[1].split("]")[0], "%d/%b/%Y:%H:%M:%S %z")
            line_splitted = line.split(" ")
            src_jp = line_splitted[0]
            http_method = line_splitted[5].replace('"', '"')
            http_path = int(line_splitted[6])
            http_protocol_version = line_splitted[7].replace('"', '"')
            http_status_code = int(line_splitted[8])
            http_response_size = int(line_splitted[9])
            line_splitted = line.split('"')

            print(line_splitted)
            print(src_jp, timestamp)
            break


# uno skripto, ki jo želimo elkspicitno zagnat, __speremn__ so posebne magic funkcije ki imajo neko svojo funkcijo
if __name__ == "__main__":
    logger.info("------------ Running web Data acquisition pipeline --------")
    raw_logs_path = get_all_logs_paths(MAIN_DATA_DIR, "access.log.*")
    logger.info(f"--> Found {len(raw_logs_path)} log files")
    logger.info(f"--> Merging all logs into {MERGED_RAW_LOGS_PATH}.")

    # print(raw_logs_path)
    # merge_logs_and_remove_empty_lines(raw_logs_path, MERGED_RAW_LOGS_PATH)
    logger.info(f"--> Converting raw logs int CSV {MERGED_CSV_LOG_PATH}.")
    convert_raw_logs_to_csv(MERGED_RAW_LOGS_PATH, MERGED_CSV_LOG_PATH)
