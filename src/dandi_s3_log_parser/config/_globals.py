import pathlib

DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH = pathlib.Path.home() / ".s3-log-extractor"
DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH.mkdir(exist_ok=True)

DEFAULT_CACHE_DIRECTORY = pathlib.Path.home() / ".s3-log-extractor-cache"

_IP_HASH_TO_REGION_FILE_PATH = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "ip-hash-to-region.yaml"
_IP_HASH_NOT_IN_SERVICES_FILE_PATH = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "ip-hash-not-in-services.yaml"

DANDI_S3_LOG_PARSER_CONFIG_FILE_PATH = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "config.yaml"
