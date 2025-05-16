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

- 1.0: for each known dandiset (somehow cache if there are new dandisets added)
  - for each published version
    - for each ID, cache asset IDs
      
    - for each asset ID
      - make softlink or note for ID, move ID to outer flat cache of blob IDs (since Zarr assets have scattered blob IDs per 'asset')
      - make manifest (cache) of the versions asset IDs
        
- 2.0: for all assets found in 1.0, count the number of occurences across DANDISETS (not versions within dandiset)
- 3.0: for all assets found in 1.0, count the number of occurences across versions within dandisets
  
- for all dandisets
  - for all versions
    - for all assets associated with that version; if result from 2.0 is 1 and 3.0 is 1
      - softlink to asset ID access data
      - use in calculating version summary
    - for all assets associated with that version; if result from 2.0 is 1 and 3.0 is greater than 1
      - under subfolder of dandiset, 'undetermined_version', softlink to asset ID access data
      - calculate undetermined summary
        
- for all dandisets
  - for all version summaries (including 'undetermined_version')
    - calculate dandiset summary
    
- for all dandisets
  - for all versions
    - for all assets
      - if result from 2.0 is greater than 1, under global subfolder 'undetermined_dandset'
        - softlink to asset ID access data
        - calculate undetermined summary
       
TODO: eventually, use URI to help determine the undetermined version/dandiset (no version may have to incorporate datetime and current default version state at that point in time)

TODO: eventually, use audit to find any asset IDs that weren't published but were uniquely associated with dandisets (draft)
  - if their filenames match existing filenames, incorporate activity into same 'asset notion'

