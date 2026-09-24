# Bot classification figures

Written by `analysis/profile_ip_behavior.py`, which defaults `--out` here so that a run lands in a tracked
folder rather than in whatever directory it started from. Regenerate them from a populated extraction cache:

```bash
python analysis/profile_ip_behavior.py \
    --cache-dir <extraction cache> --no-encryption \
    --testing-asset-file analysis/testing_blobs.txt --content-id-map
```

| File | What it shows |
| --- | --- |
| `ip_behavior.png` | Sessions, archive coverage, and download-to-stream ratio across all profiled IPs |
| `ip_behavior_distributions.png` | The four candidate axes, each checked for a valley that would justify a threshold |
| `ip_behavior_selection_plane.png` | New-asset fraction per visit against files per visit, with the archetype bands |
| `ip_behavior_saturation_plane.png` | Within-dandiset saturation against archive coverage, the axis that discriminates |
| `ip_behavior_testing_conditional.png` | Testing-asset access conditioned on the other axes |
| `ip_behavior_by_service.csv`, `ip_behavior_by_ip.csv` | The numbers behind the figures |

These carry no IP addresses. An actor appears only as a random pseudonym and a region label, and the pseudonym
means nothing without the registry, which stays on the machine that ran the profiler. They are therefore safe
to publish and to reuse in write-ups elsewhere. See `../findings.md` for what they are read to mean.
