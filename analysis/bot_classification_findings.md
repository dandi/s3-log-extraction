# Bot classification for `number_of_views`: what we measured and what we recommend

**Status:** findings for team decision. Nothing here is shipped except the GitHub Actions exclusion
(§7). §8 proposes a two-tier rule: the saturation region of §5 nominates, and each nominee is
confirmed by review before exclusion.

**Data:** the full extraction cache — 654,514 assets, 101,386 distinct IPs, 11,974,420 view sessions.
A "view session" is the shipped `number_of_views` unit: a maximal run of streaming (HTTP 206,
`download == 0`) requests from one IP to one asset with no gap over 8 hours.

**Tooling:** `analysis/profile_ip_behavior.py`. IPs are stored only as a salted keyed hash and are
shown in tables under pseudonyms drawn uniformly at random, never as addresses. The pseudonyms are
not derived from the addresses, so they carry no information about them; the registry linking the two
is held only on the machine that produced it. Actors are therefore referred to here by role rather
than by name.

---

## 1. The headline: this is a one-actor problem, not a bot-fraction problem

| top N IPs by sessions | share of ALL views |
|---|---|
| 1 | **54.0%** |
| 5 | 63.2% |
| 10 | 65.8% |
| 25 | 69.1% |
| 100 | 75.0% |

A single IP — geolocated USA/NH, called **the dominant actor** throughout — accounts for **54% of
every view in the archive**.
Its behavioral profile is unambiguous and is corroborated independently by every axis we measured:

| property | value | reading |
|---|---|---|
| view sessions | 6,463,827 | 54.0% of the archive |
| distinct assets | 56,217 (8.6% of archive) | broad but not exhaustive |
| visits (8 h-gapped) | **102** | it shows up ~100 times, total |
| files per visit | **35,978** | each visit is a bulk sweep |
| mean visit duration | 16.3 hours | long marathon runs |
| median session duration | **1.0 s** (mean 1,058 s) | touches each asset for a second |
| new-asset fraction per visit | **8.7%** | it re-takes the *same* asset set every time |
| active timespan | 1,225 days (137 active days) | running for 3.4 years |
| dominant-period fraction | **0.82** | strongly periodic cadence |
| session-gap CV | 650.7 | *not* metronomic by CV — see §4 |

That is a periodic mirror or re-indexer: it wakes roughly 100 times over three years and each time
rips through ~36,000 assets at about a second each, re-fetching material it already has.

The practical consequence: **any statement about the archive's view count is dominated by this one
actor.** Excluding it alone halves the reported total. The remaining 1,210 IPs in the same behavioral
class together account for only ~1.3%.

## 2. Where the traffic comes from

Per service label, restricted to IPs with ≥ 20 sessions:

| service | IPs | views | mean active timespan | mean active days | testing-asset share of sessions |
|---|---|---|---|---|---|
| geographic (no service label) | 1,576 | 8,507,653 | 163 d | 18 | 0.3% |
| GH-actions | 4,022 | 2,311,131 | 284 d | 23 | 4.4% |
| AWS | 1,003 | 595,557 | 7.7 d | 1.9 | 0.0% |
| VPN | 996 | 281,000 | 145 d | 29 | 13.5% |
| GCP | 580 | 48,289 | 2.3 d | 1.3 | 0.1% |

The load-bearing observation: of the sessions our systematic-behavior flags catch, **84.8% carry a
plain geographic label** — they are invisible to any filter based on IP origin. GH-actions is 11.6%,
AWS 3.1%, VPN 0.3%, GCP 0.2%. An origin-based exclusion cannot reach the actors that matter.

AWS and GCP show the clean signature of one-shot bulk operations: active for days rather than months,
one or two active days apiece.

## 3. Axes that did not yield a threshold

We held every candidate axis to one standard: a defensible cut needs a visible **gap or valley**
separating a human bulk from a bot tail — the same standard the 8-hour session boundary met. Plots
are in `ip_behavior_distributions.png`.

- **Archive coverage** (fraction of all assets touched). The CCDF is smooth across five decades, from
  the median at 0.008% to a maximum of 66.9%. There is no gap. Any cut is arbitrary; a 2% cut would
  select 26 IPs holding 69.1% of views, but only because it happens to sit above the big actors.
- **Session-gap CV** (timing irregularity). A single unimodal bulk centred near CV ≈ 1–10 with no
  valley anywhere near the candidate cut of 0.1. Only 1,096 IPs are flagged metronomic, and 81% of
  that flagged volume is the dominant actor alone.
- **Revisit rate** (sessions ÷ distinct assets). A spike at exactly 1 (touch-once is the norm) and a
  smooth tail. No structure to cut on.
- **Selection entropy.** Retracted earlier in the analysis: it is ≈ 1.0 for nearly every high-activity
  IP, because one-session-per-asset is the norm. It measures revisit-evenness, not choice randomness.
- **Regular testing-asset access.** 2,574 IPs touch the reserved testing blobs ≥ 10 times. But its
  *marginal* contribution beyond coverage/timing is only 444,338 sessions (3.71%) — it mostly re-flags
  actors already caught, so it does not earn a place as an independent axis.

## 4. The axis that partly worked, and its limits

**New-asset fraction per visit** — of the assets an IP touches in a visit, what share it had never
touched before — is the one axis with genuine multi-modal structure. It directly encodes the
principle that authentic use is idiosyncratic in *what* it chooses: a monitor re-takes the same
material (→ 0), a scanner marches through fresh material (→ 1), a human mixes the two.

It also catches what coverage and CV miss. The dominant actor has a *high* gap-CV (650) and only moderate
coverage (8.6%), so it passes both of those filters — but its 8.7% new-asset fraction exposes it
immediately.

**However, the axis is degenerate at low visit counts.** An IP with a single visit scores exactly 1.0
by construction: everything it saw was new because it had never been before. Two visits can only
score 0, 0.5 or 1. We added a `--min-visits` guard (default 3) and re-measured:

| archetype | before guard | after guard | |
|---|---|---|---|
| re-poller / monitor (new ≤ 15%) | 1,211 IPs, 55.3% | **1,211 IPs, 55.3%** | unchanged |
| mixed / human (15–85%) | 2,971 IPs, 24.6% | 2,878 IPs, 24.0% | ~unchanged |
| bulk mirror (new ≥ 85%, ≥ 500 files/visit) | 409 IPs, 12.0% | **46 IPs, 4.0%** | **89% was artifact** |
| one-shot casual (new ≥ 85%, < 500 files/visit) | 3,586 IPs, 6.2% | 858 IPs, 3.3% | 76% was artifact |

The guard excludes 3,184 of 8,177 active IPs, holding 11.5% of sessions.

Two conclusions:

1. **The re-poller half is sound.** It is mathematically untouched by the guard — you cannot score
   ≤ 15% new without revisiting. This is the band holding 55.3% of views.
2. **The all-new half was mostly an artifact** and earlier drafts of this analysis overstated it. The
   genuine bulk-mirror population is 46 IPs at 4.0% of views, not 409 at 12.0%.

Separately, the **files-per-visit** threshold (500) used to split mirrors from casual visitors is
*not* valley-justified. The only real valley in that distribution sits at ~3 files/visit, which
separates single-file actors from everyone else — not mirrors from humans. 500 is a judgement call.

## 5. Dataset saturation: the axis that discriminates

The published content-id → dandiset mapping turns the content-addressed cache into dataset
membership, which makes the decisive measurement possible at last: not how many assets an actor
touched, but **how much of each dataset it consumed, and across how many datasets**.

Neither half works alone. Broad-but-shallow is an ordinary research pattern, and deep-but-narrow is a
dataset's own author or a single bulk download. The mirror signature is high on **both at once**, and
that is the region the plane isolates (`ip_behavior_saturation_plane.png`).

| joint region | IPs | views |
|---|---|---|
| saturation ≥ 25%, dandiset coverage ≥ 10% | 91 | 71.9% |
| **saturation ≥ 50%, dandiset coverage ≥ 25%** | **12** | **62.9%** |
| saturation ≥ 50%, coverage ≥ 50% | 3 | 6.6% |
| saturation ≥ 75%, coverage ≥ 50% | **0** | 0% |

**Twelve addresses hold 62.9% of every view in the archive.** The ten of them that appear in the
top-100 table span every service label — geographic, AWS, VPN and GH-actions alike — confirming §2:
this is a behavioral population, not an origin one.

The empty bottom row is a real finding rather than a gap: nothing takes three-quarters of the datasets
it visits across half the archive. There is no perfect mirror; even the most systematic actors are
selective, so the strictest rule would catch nothing at all.

**What makes this axis worth more than archive coverage** is what it *declines* to flag. Several very
high-volume actors range extremely widely but stay shallow:

| dandisets touched | share of archive | mean saturation | views |
|---|---|---|---|
| 508 | 79% | 0.36 | 207,056 |
| 507 | 79% | 0.47 | 16,589 |
| 408 | 64% | 0.46 | 16,682 |
| 336 | 53% | 0.36 | 31,071 |

These are the "dozens of files across hundreds of datasets" profile — precisely the legitimate heavy
use that a coverage threshold cannot distinguish from a scanner. Saturation leaves them alone. The
same logic clears GCP, whose mean saturation is the highest of any service (0.70) but across just 1.5
dandisets on average: deep and narrow, not a mirror.

Three caveats, all load-bearing:

- **There is still no valley.** Saturation climbs smoothly (median 0.18, 90th 0.74, 99th 1.00). The
  cuts at 50% and 25% are better-motivated judgement calls than anything before them, but they are
  judgement calls, not discovered boundaries.
- **`max_within_dandiset_saturation` is useless** and should be ignored: it is 1.0 for 91 of the
  top 100 IPs, because fully consuming any one small dandiset saturates it. Only the *mean* carries
  information.
- **The mapping covers 61% of cached assets** (410,935 entries against 654,514). Denominators are
  true archive dandiset sizes, so the measure itself is sound, but an actor's touched-count includes
  only mapped assets, biasing saturation *downward*. That is the safe direction — it cannot invent a
  mirror that is not there.

## 6. Two corrections worth recording

We report these because both were believed true at an intermediate stage and would have shipped as
errors:

- **A "distributed VPN monitor" that did not exist.** On a 5,000-asset sample the VPN cohort looked
  like a coordinated poller (1.0 files/visit, 4.4% new, 96.1% of its sessions on testing assets). On
  the full archive those become 65.8 files/visit, 62.2% new, 13.5% testing — ordinary mixed behavior.
  The sample had biased the measurement: with few assets in scope, any IP touching one of them looks
  like a pure re-poller. **There is no behavioral case for excluding VPN traffic.**
- **The inflated scanner population**, described in §4.

The general lesson: measure on the full archive before drawing population conclusions, and check
every ratio for a degenerate denominator.

## 7. What is already shipped

**GitHub Actions views are excluded from `number_of_views`.** The `GitHub` service label was split
into `GH-actions` (the `actions*` ranges) and `GitHub` (everything else), and only `GH-actions` is
dropped. The reasoning: a GitHub Codespace streaming an NWB file is a legitimate view, while an
Actions runner is unambiguously CI. Measured impact ≈ 19% of views; non-Actions GitHub traffic is
5 views archive-wide, so the split costs nothing and removes the ambiguity.

Cloud/VPN/CI exclusion as a whole would have been ~24–27% of views, but §6 shows VPN does not belong
in that set, and AWS/GCP remain an open question (§9).

## 8. Recommendation

**Nominate by rule, exclude by review.** Two tiers, because the evidence supports exactly that much
and no more.

**Tier 1 — the saturation region nominates.** An actor with mean within-dandiset saturation ≥ 50%
across ≥ 25% of dandisets is taking most of most of the archive, which is not a research pattern. That
selects **12 addresses holding 62.9% of all views** (§5). This is a genuine rule: it encodes a
behavioral claim, it is computed from published data, and it demonstrably spares the broad-but-shallow
heavy users that every coverage-based alternative would have swept up.

**Tier 2 — each nominee is confirmed individually** against its row in `ip_behavior_by_ip.csv` before
exclusion. The cuts have no valley behind them (§5), so the rule is a well-aimed filter rather than a
proof, and 12 cases is a reviewable number. The dominant actor (§1) is the clearest of them and can be
excluded on its own evidence today.

Then:

1. Exclude the dominant actor first. One address, 54% of all views, corroborated by every axis
   measured. Highest-value single correction available, defensible in isolation.
2. Review the remaining 11 Tier-1 nominees against their evidence rows.
3. Keep the GH-actions filter (§7).
4. Publish the exclusion list and the per-IP evidence alongside the statistics, so the number is
   reproducible and every judgement call is auditable.
5. Re-run the profiler periodically. The population is not static, and the registry keeps pseudonyms
   stable across runs so an actor can be tracked over time.

**What this deliberately does not do** is run as an automated classifier. No axis has a valley, so
every threshold remains a choice; 12 nominations a quarter is a review burden the team can carry,
and a wrong exclusion is far more costly than a missed one. The looser region — 91 IPs at 71.9% — is
where the marginal cases live and is worth reading, but it should not drive exclusions unreviewed.

## 9. Open questions

- **Dandiset coverage for the 39% of assets outside the mapping.** Saturation is measured only over
  mapped assets, which biases it downward (§5). Closing that gap would tighten the axis.
- **AWS and GCP one-shot bulk operations** (595,557 and 48,289 views) have a clean mirror signature
  but may include legitimate cloud-hosted analysis. Not resolved.
- **The 46 surviving bulk mirrors** (4.0%, §4) overlap the saturation nominees only partly; the
  difference between the two selections is worth understanding before either is relied on alone.
- **Object size is not in the cache**, so we cannot compute a full-read fraction — the direct test for
  a checksum scanner versus a metadata skim.
