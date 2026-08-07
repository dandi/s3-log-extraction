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

SUMMARY_UPDATE_INTERVAL_DAYS = 7
"""
Minimum number of days between two publications of the per-asset summaries.

Access counts are only safe to publish as an aggregate over a period of activity. Republishing them
more often lets an observer difference two consecutive releases and recover the behavior of the handful
of requesters active in between, so the summaries refuse to regenerate inside this interval.
"""

MINIMUM_REGIONS_FOR_DISCLOSURE = 3
"""
Number of distinct geographic regions that must be exceeded for an asset's row to be republished.

An update is only released for an asset when *more than* this many genuine geographic regions accessed
it during the reporting week. Cloud service, VPN, and unresolved region labels do not count, since they
are not evidence of a diverse requester population.
"""

PRIVACY_ROUNDED_COLUMN_NAMES = ("number_of_requests", "number_of_downloads")
"""
Columns of the by-day and by-region summaries that are rounded to a modulo and censored below a minimum.

The per-asset summaries are protected by the region-diversity gate instead, which releases exact values
only for assets with a demonstrably diverse requester population. That gate cannot apply to a by-region
row, which covers exactly one region by construction, nor to a by-day row, which has no asset.
"""

ASSET_METRIC_COLUMN_NAMES = ("bytes_sent", "number_of_requests", "number_of_downloads", "number_of_views")
"""Per-asset access metrics, all released or withheld together as a single row."""
