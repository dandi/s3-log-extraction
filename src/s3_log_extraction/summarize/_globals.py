SESSION_TIMEOUT_SECONDS = 28_800
"""
Maximum gap (in seconds) between two consecutive streaming requests that still belong to the same view.

A "view" of an asset is a streaming session, defined as a maximal run of streaming (HTTP 206) requests
from a single IP address to a single asset in which no two consecutive requests are more than this many
seconds apart. Full downloads (HTTP 200) are never views and are reported separately.

The default of 8 hours sits in the empirical minimum-density valley of the distribution of inter-request
gaps observed across the DANDI archive. Within-session pauses are essentially all under 2 hours and
returning visits cluster after ~21 hours, so an 8 hour gap is the rarest thing a real requester does.
At that threshold only about 1 gap in 60,000 is ambiguous, and the resulting session counts are
insensitive to any choice between roughly 2 and 8 hours.
"""

TIMESTAMP_FORMAT = "%y%m%d%H%M%S"
"""Format of the per-request timestamps written to ``timestamps.txt`` during extraction."""

PRIVACY_PROTECTED_COLUMN_NAMES = ("number_of_requests", "number_of_downloads", "number_of_views")
"""Summary columns that are derived from per-requester behavior and must be privacy-rounded."""
