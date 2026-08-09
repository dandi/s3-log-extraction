# A view of an asset is a streaming session: a maximal run of streaming (HTTP 206) requests from one IP
# address to that asset in which no two consecutive requests are more than this many seconds apart.
# Eight hours sits in an empirical minimum-density valley of the observed inter-request gaps.
SESSION_TIMEOUT_IN_SECONDS = 28_800

# Format of the per-request timestamps written to `timestamps.txt` during extraction
TIMESTAMP_FORMAT = "%y%m%d%H%M%S"

# The by-region summary is the only one that pairs activity with requester location, so it is the only one
# that can single out an individual. Every other summary is published with its true values on every update.
#
# A by-region summary is therefore written only when the update it carries moves more than this many
# resolved regions at once, which is to say when no single requester's activity can be read off the change.
# The cost is that the by-region summary lags the others until enough regions move together.
REGION_DISCLOSURE_THRESHOLD = 5

# Columns of a by-region summary whose values decide whether a region was updated
REGION_VALUE_COLUMN_NAMES = ("bytes_sent", "number_of_requests", "number_of_downloads", "number_of_views")
