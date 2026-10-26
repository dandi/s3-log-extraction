import pathlib

S3_LOG_EXTRACTION_BASE_FOLDER_PATH = pathlib.Path.home() / ".s3-log-extraction"
S3_LOG_EXTRACTION_BASE_FOLDER_PATH.mkdir(exist_ok=True)
S3_LOG_EXTRACTION_CONFIG_FILE_PATH = S3_LOG_EXTRACTION_BASE_FOLDER_PATH / "config.yaml"
DOTENV_FILE_PATH = S3_LOG_EXTRACTION_BASE_FOLDER_PATH / ".env"
EXCLUDED_IPS_ENVIRONMENT_VARIABLE = "S3_LOG_EXTRACTION_EXCLUDED_IPS"
# Read by 1.11.10 only. Its presence now stops a run, so that no listed requester is silently included again
_LEGACY_EXCLUDED_IPS_FILE_PATH = S3_LOG_EXTRACTION_BASE_FOLDER_PATH / "excluded_ips.txt"

DEFAULT_CACHE_DIRECTORY = pathlib.Path.home() / ".cache" / "s3_log_extraction"
