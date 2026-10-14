# Byte-weighted structure: findings

Exploratory. Not yet folded into `WHITEPAPER.md`.

## Sample

The walk opened each file with `remfile` and `h5py` over its S3 blob URL. LINDI and the DANDI API were both
unreachable from the environment that ran it, so neither was used.

The committed `access_structure.csv` is a snapshot of 4,493 files from 253 dandisets. The `dandi-cache` structural
caches have since grown about 25 times, so this sample is drawn from a refreshed join of the current caches instead.
That join holds 112,445 files with access counts across 423 dandisets, and every file of the old snapshot is in it.
It is rebuilt with `build_dataset.py` and kept in the gitignored cache directory, so the whitepaper's snapshot is
untouched. The sample is stratified at most 10 files per dandiset, random with seed 0.

- Sampled 3,584 files from 423 dandisets. Walked **3,557 files from 422 dandisets**.
- **27 files could not be walked.** They are among the largest in the sample, with a median of 25 GB and a maximum
  of 938 GB, and they come from 10 dandisets. Each timed out on a first pass that also counted chunks (15 minutes per
  file), then again on a second pass that skipped the chunk count and allowed an hour per file, because traversing
  their chunk index over HTTP takes longer than that. That second pass recovered 50 of the 77 first-pass failures,
  including every file killed for memory. The 27 are listed in `byte_weighted_failures.csv`.
- One dandiset, 1413, has no walked file. Nine more are missing some of their sampled files.
- The size tail is still well represented. Files over 10 GB make up 8.7% of the walked sample against 3.7% of the
  full table, because capping each dandiset at 10 files gives big-file dandisets equal weight. The largest file
  walked holds 726 GB.
- 6,244 datasets across 303 files had zero allocated storage. They are excluded from `D0` and from the shares.

Every statistic below is reported for "files", meaning per-file values capped at 10 per dandiset, and for
"dandisets", meaning one median per dandiset (n = 422). With this many dandisets the two now agree closely.

## Figures

**1. `D1` against `D0`.** The points do not hug the `D1 = D0` line. The median file has 63 nonzero datasets but an
effective number of only 3.0, with a median evenness of 0.030. More than a third of files (36.5%) hold over 90% of
their bytes in a single dataset. So `D1` is not object count restated, but it is still strongly ordered by it, as
figures 2 and 6 show.

**2. `D1` and evenness against bytes.** Holding size fixed, `D1` stays tied to count, with partial Spearman
`D1 ~ D0 | bytes` = +0.81 for both files and dandisets. Holding count fixed, larger files concentrate their bytes,
with partial Spearman `D1 ~ bytes | D0` = −0.54 for files and −0.53 for dandisets. Evenness has little relation to
count (Spearman +0.06 for files, +0.09 for dandisets). It falls with size instead: Spearman −0.40 for files and −0.39
for dandisets, and partial `E ~ bytes | D0` = −0.41 and −0.40.

**3. ECDF of evenness.** About three quarters sit below 0.1, 75% of files and 75% of dandiset medians. The two curves
coincide, so the cap is not hiding a different picture.

**4. Lorenz curves.** These are eight files at evenly spaced evenness quantiles. At the low end, a single dataset
carries almost everything. Only the top quantiles approach the diagonal.

**5. Section composition.** Bytes live in acquisition for 251 of 422 dandisets (median share > 0.5) and in processing
for 97. Stimulus, units and general take the bulk only in a minority. The CLR-PCA of section shares has its first
component at 45% and its second at 20%. The first component is the acquisition-versus-processing axis.

**6. Validation panel.** Every metric is mostly a dandiset property, with between-dandiset ICC(1) of 0.87–0.93: log
`D0` 0.93, log bytes 0.92, log cophenetic 0.92, log `D1` 0.92, log `D2` 0.92, evenness 0.87. Log cophenetic is count
again (Spearman with log `D0` 0.98). `D1` and `D2` are mostly count, at 0.74 and 0.69 for files and 0.75 and 0.71 for
dandisets, with a negative size component of −0.28 and −0.27. Evenness is nearly uncorrelated with count (0.06 for
files, 0.09 for dandisets) and moderately anti-correlated with size (−0.40 for files, −0.39 for dandisets).
The panel also carries r2 and r*, the next two successive ratios. Each is mostly a dandiset property (ICC 0.92 and
0.87), and both are tied to count rather than size (see the section on successive ratios below).

**7. Access, at dandiset level only (n = 422, medians).** Log bytes remains the strongest correlate of download
fraction (Spearman −0.74), followed by log `D0` (−0.48). `D1` (−0.18) and evenness (+0.16) are weak. With log `D0`
and log bytes both controlled, `D1` keeps a partial Spearman of −0.25 with download fraction. That is a small residual
signal, not a new predictor. No per-file access model was fitted.

**8. Ratio composition (`8_ratio_composition.png`).** One row per group from the clustering below, in blocks by
recording family and ordered by r1 within each block. A group's family is the most common family among its files'
modality suffixes: ecephys (with spike sorting), icephys, ophys (with fiber photometry and widefield), multimodal
for files recording more than one of those, then image, behavior, other, and unlabeled for the 14% of files with no
suffix. The blocks hold 134, 60, 93, 5, 52, 34, 3 and 66 groups. The left panel shows how many decades each group
falls from D0 to D∞ and through which ratio; the right panel shows the same drop as shares. Red ticks mark clusters
of split dandisets, which land in different blocks when they were split by modality. The median group falls 1.58
decades, and in 60% of groups r1 does more than 90% of that drop. icephys stands apart with two populations, one of
them collapsing mostly through r2, a long tail of mid-sized sweeps.

`8b_ratio_composition_tree.png` shows the same bars ordered instead by a Ward tree on each dandiset's mean (log r1,
log r2, log r*), drawn down the left like a clustered heatmap. There, a split dandiset's clusters sit together under
a red clade.

**9. Split dandisets (`9_split_dandisets.png`).** The 48 clusters of the 23 split dandisets, banded by dandiset and
labeled with each cluster's split basis, file count and modality suffixes, so every split can be checked against
its files.

## Successive Hill ratios

Hill numbers never increase with their order, D0 ≥ D1 ≥ D2 ≥ D∞, where D∞ = 1 / (largest share). So the profile is
carried exactly by one scale and the successive ratios

    r1 = D1/D0,   r2 = D2/D1,   r* = D∞/D2,   each in (0, 1],   with   D∞ = D0 · r1 · r2 · r*.

r1 is the evenness and falls with the metadata dust. r2 falls when a long tail of mid-sized datasets holds real mass.
r* falls when even the leading datasets are unequal. All three are in `byte_weighted.csv`, and the identity above
holds in every file. Their medians are 0.030, 0.92 and 0.92, so almost all of the drop happens at the first step.

r2 and r* agree closely (Spearman +0.91), so orders above 2 add almost nothing, and (D0, r1, r2) carries essentially
the whole profile. r1 and r2 are only weakly related (−0.13), so they are two genuinely different shape axes. They
**split the confound between them**. r1 is nearly free of count (+0.06) but tied to size (−0.40). r2 is nearly free
of size (+0.17) but tied to count (−0.60): files with more datasets have heavier tails. Neither escapes both.

## Clustering within dandisets

`byte_weighted_structure.py cluster` groups each dandiset's files by their (log r1, log r2, log r*), each log scaled by
its spread across all files. It tries Ward clusterings of 2 to 4 clusters and takes the best candidate with every
cluster at least 2 files, a mean silhouette of at least 0.5, and its closest centroids at least one global spread
apart. 74 of 422 dandisets have such a candidate.

Clear clusters in ratio space are not by themselves different file types, because r1 falls with file size: one file
type recorded for minutes and for hours separates cleanly. So each candidate is classified by what distinguishes its
clusters. A candidate splits by **modality** when each cluster's most common modality suffix (`_ecephys`,
`_behavior+ophys`) is named and distinct. It splits by **schema** when even the most alike pair of clusters has a
Jaccard similarity below 0.7 between their majority schemas. A file's schema is its set of dataset paths with digits
folded to `#`, so `sweep_0012` and `sweep_0013` count as one path. Otherwise the split is labeled **size** (median
storage differs at least 3 times) or **shape**.

| Basis | Candidates | Median schema Jaccard | Split? |
|---|---|---|---|
| modality | 12 | 0.45 | yes |
| schema | 11 | 0.45 | yes |
| size | 20 | 0.90 | no |
| shape | 31 | 0.92 | no |

The schema test is calibrated on the modality splits, the ones the uploaders themselves labeled as different file
types: their median similarity is 0.45 against about 0.9 for the size and shape candidates. An earlier version tested
a 1.5-fold difference in dataset count instead. It wrongly admitted intracellular recordings that differ only in
sweep count, which is length again, measured in datasets rather than bytes. Two modality splits, 000037 and 001942,
have near-identical schemas (0.81 and 0.85), and 000037's clusters also mix suffixes. `--split-on schema` would keep
them whole.

The result is 447 groups: 399 whole dandisets and 48 clusters from 23 split ones (21 into two, 2 into three).
`byte_weighted_ratios_by_file.csv` gives every file its group, its ratio-space cluster and that cluster's basis,
whether or not the split was kept. `byte_weighted_ratios_by_group.csv` carries each group's mean r1, r2 and r*, file
count, median D0 and storage, recording family, modality suffixes, split basis, silhouette and schema similarity. With at most 10
files per dandiset, a file type with only one sampled file cannot form a cluster, so minority file types are
undercounted.

## What changed from the first sample

The first pass walked 1,221 files from the 4,493-file snapshot. The conclusions hold, with two refinements. First,
evenness is less tied to size than it looked. Its Spearman with bytes went from −0.53 to −0.40, so it now shares
about 16% of its rank variance with log bytes rather than about 28%. Second, the acquisition-versus-processing axis
is stronger, with PC1 rising from 39% to 45%. The per-file and dandiset-level numbers also converged, which is what
the larger dandiset count should do.

## Plain statement

Neither metric is a new complexity score. `D1` is largely object count, modulated by size, because larger files
concentrate their bytes in fewer datasets. Evenness is the one quantity close to independent of count. It still
carries a moderate size component, though weaker than the smaller sample suggested, so it only partly escapes the
"structure vs. size" collapse. What the byte-weighted view does add is where the bytes sit. Section composition,
acquisition-dominated versus processing-dominated, is a real, strong, dandiset-level distinction that the
unlabeled-tree metrics cannot express. If anything here earns a place in the whitepaper, it is that, and not `D1` or
evenness as a complexity score. Taking the profile apart into successive ratios sharpens the negative result. The
byte-weighted shape does not escape the count-versus-size collapse; it decomposes along it, into r1 tied to size and
r2 tied to count.

## Follow-up

- The 27 unwalked large files. Each needs either a longer timeout or a size estimate that avoids the chunk index.
- Zarr assets. This pass is HDF5 only.
- A walk without the per-dandiset cap. The largest dandiset alone holds 14,181 files, so that would need a different
  weighting scheme rather than just more time.
