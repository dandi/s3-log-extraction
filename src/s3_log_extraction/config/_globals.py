import pathlib

S3_LOG_EXTRACTION_BASE_FOLDER_PATH = pathlib.Path.home() / ".s3-log-extraction"
S3_LOG_EXTRACTION_BASE_FOLDER_PATH.mkdir(exist_ok=True)
S3_LOG_EXTRACTION_CONFIG_FILE_PATH = S3_LOG_EXTRACTION_BASE_FOLDER_PATH / "config.yaml"

DEFAULT_CACHE_DIRECTORY = pathlib.Path.home().parent / ".cache" / "s3_log_extraction"

# TODO: these might belong in cache
_IP_HASH_TO_REGION_FILE_PATH = S3_LOG_EXTRACTION_BASE_FOLDER_PATH / "ip-hash-to-region.yaml"
_IP_HASH_NOT_IN_SERVICES_FILE_PATH = S3_LOG_EXTRACTION_BASE_FOLDER_PATH / "ip-hash-not-in-services.yaml"
