# Bot classification for `number_of_views`: what we measured and what we recommend

**Status:** findings for team decision. Nothing here is shipped except the GitHub Actions exclusion
(§6). No behavioral rule is proposed for production yet, and §5 explains why.

**Data:** the full extraction cache — 654,514 assets, 101,386 distinct IPs, 11,974,420 view sessions.
A "view session" is the shipped `number_of_views` unit: a maximal run of streaming (HTTP 206,
`download == 0`) requests from one IP to one asset with no gap over 8 hours.

**Tooling:** `analysis/profile_ip_behavior.py`. IPs are stored only as a salted keyed hash and are
shown in tables under stable pseudonyms (e.g. `ProudVireo22`), never as addresses.

---

## 1. The headline: this is a one-actor problem, not a bot-fraction problem

| top N IPs by sessions | share of ALL views |
|---|---|
| 1 | **54.0%** |
| 5 | 63.2% |
| 10 | 65.8% |
| 25 | 69.1% |
| 100 | 75.0% |

A single IP (`ProudVireo22`, geolocated USA/NH) accounts for **54% of every view in the archive**.
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
  that flagged volume is `ProudVireo22` alone.
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

It also catches what coverage and CV miss. `ProudVireo22` has a *high* gap-CV (650) and only moderate
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

## 5. Two corrections worth recording

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

## 6. What is already shipped

**GitHub Actions views are excluded from `number_of_views`.** The `GitHub` service label was split
into `GH-actions` (the `actions*` ranges) and `GitHub` (everything else), and only `GH-actions` is
dropped. The reasoning: a GitHub Codespace streaming an NWB file is a legitimate view, while an
Actions runner is unambiguously CI. Measured impact ≈ 19% of views; non-Actions GitHub traffic is
5 views archive-wide, so the split costs nothing and removes the ambiguity.

Cloud/VPN/CI exclusion as a whole would have been ~24–27% of views, but §5 shows VPN does not belong
in that set, and AWS/GCP remain an open question (§8).

## 7. Recommendation

**Do not ship a threshold classifier.** No axis has the distributional structure to justify one, and
the two axes that looked most promising both required retraction or heavy qualification.

**Do ship a named extreme-outlier exclusion**, justified per IP by the behavioral evidence table
rather than by a population threshold. Concretely:

1. Exclude `ProudVireo22`. One actor, 54% of all views, with five independent lines of evidence
   (§1). This is the single highest-value correction available and it is defensible in isolation.
2. Review the next 24 by hand against their profile rows. Top-25 exclusion would remove 69.1% of
   views; each case should be argued individually, not by rule.
3. Keep the GH-actions filter (§6).
4. Publish the exclusion list and the per-IP evidence alongside the statistics, so the number is
   reproducible and the judgement calls are auditable.

This is deliberately conservative: it privileges a small number of defensible, documented decisions
over an automated rule whose error modes we cannot characterise.

## 8. Open questions

- **Dataset saturation is still unmeasured.** The strongest discriminator identified — high saturation
  over *both* files-within-a-dataset and number-of-datasets — needs a content-id → dandiset mapping
  that the content-addressed cache does not carry. A legitimate power user revisits dozens of files
  across hundreds of datasets; a bot saturates both. We cannot currently compute this.
- **AWS and GCP one-shot bulk operations** (595,557 and 48,289 views) have a clean mirror signature
  but may include legitimate cloud-hosted analysis. Not resolved.
- **The 46 surviving bulk mirrors** (4.0%) warrant individual review on the same basis as the top-25.
- **Object size is not in the cache**, so we cannot compute a full-read fraction — the direct test for
  a checksum scanner versus a metadata skim.
