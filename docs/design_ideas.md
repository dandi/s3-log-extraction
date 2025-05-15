# Initial parsing of raw .log

First pass, only compute and store (via cache) length (or byte offset from start?) of all lines 

TODO: find maximum and minimum length of lines; perhaps validate over time (by caching result) that these do not change, or outright record them per log file?

Use config file in home to point to non-default cache

Certain fields before the URIs (most unstable part) should be deterministic offsets from these lengths

See how fast it is to just pull those out in RAM directly without having to save/load the reduced version back to disc
  - if fast (perhaps only on SSD?) then just create lazy in-memory reader of raw logs

Might be more complicated for other fields after URI (could calculate length of URI fields based on final closing quotes?
  - count number of quotes per line (always multuple of two?)

TODO: determine if /mnt/backup is SSD



# How to make mapping step iterable?
