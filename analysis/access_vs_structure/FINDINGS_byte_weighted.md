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

- Sampled 3,584 files from 423 dandisets. Walked **3,519 files from 418 dandisets**.
- **65 files are still pending.** They are the largest in the sample, with a median of 27 GB and a maximum of
  938 GB, and they come from 14 dandisets. All timed out at 15 minutes while traversing their chunk index, or were
  killed for memory. A second pass that skips the chunk count and allows an hour per file is retrying them. They are
  listed in `byte_weighted_failures.csv`.
- Five dandisets have no walked file yet: 692, 776, 1371, 1413 and 1535. Nine more are partially walked.
- The size tail is still well represented. Files over 10 GB make up 8.0% of the walked sample against 3.7% of the
  full table, because capping each dandiset at 10 files gives big-file dandisets equal weight. The largest file
  walked holds 726 GB.
- 6,244 datasets across 303 files had zero allocated storage. They are excluded from `D0` and from the shares.

Every statistic below is reported for "files", meaning per-file values capped at 10 per dandiset, and for
"dandisets", meaning one median per dandiset (n = 418). With this many dandisets the two now agree closely.

## Figures

**1. `D1` against `D0`.** The points do not hug the `D1 = D0` line. The median file has 62 nonzero datasets but an
effective number of only 3.0, with a median evenness of 0.031. More than a third of files (36.5%) hold over 90% of
their bytes in a single dataset. So `D1` is not object count restated, but it is still strongly ordered by it, as
figures 2 and 6 show.

**2. `D1` and evenness against bytes.** Holding size fixed, `D1` stays tied to count, with partial Spearman
`D1 ~ D0 | bytes` = +0.81 for both files and dandisets. Holding count fixed, larger files concentrate their bytes,
with partial Spearman `D1 ~ bytes | D0` = −0.54 for files and −0.53 for dandisets. Evenness has little relation to
count (Spearman +0.07 for files, +0.10 for dandisets). It falls with size instead: Spearman −0.39 for files and −0.38
for dandisets, and partial `E ~ bytes | D0` = −0.40 and −0.39.

**3. ECDF of evenness.** About three quarters sit below 0.1, 74% of files and 75% of dandiset medians. The two curves
coincide, so the cap is not hiding a different picture.

**4. Lorenz curves.** These are eight files at evenly spaced evenness quantiles. At the low end, a single dataset
carries almost everything. Only the top quantiles approach the diagonal.

**5. Section composition.** Bytes live in acquisition for 248 of 418 dandisets (median share > 0.5) and in processing
for 96. Stimulus, units and general take the bulk only in a minority. The CLR-PCA of section shares has its first
component at 45% and its second at 20%. The first component is the acquisition-versus-processing axis.

**6. Validation panel.** Every metric is mostly a dandiset property, with between-dandiset ICC(1) of 0.87–0.94: log
`D0` 0.94, log bytes 0.91, log cophenetic 0.93, log `D1` 0.92, log `D2` 0.92, evenness 0.87. Log cophenetic is count
again (Spearman with log `D0` 0.98). `D1` and `D2` are mostly count, at 0.74 and 0.70 for files and 0.75 and 0.71 for
dandisets, with a negative size component of −0.29 and −0.28. Evenness is nearly uncorrelated with count (0.07 for
files, 0.10 for dandisets) and moderately anti-correlated with size (−0.39 for files, −0.38 for dandisets).

**7. Access, at dandiset level only (n = 418, medians).** Log bytes remains the strongest correlate of download
fraction (Spearman −0.74), followed by log `D0` (−0.47). `D1` (−0.18) and evenness (+0.15) are weak. With log `D0`
and log bytes both controlled, `D1` keeps a partial Spearman of −0.26 with download fraction. That is a small residual
signal, not a new predictor. No per-file access model was fitted.

## What changed from the first sample

The first pass walked 1,221 files from the 4,493-file snapshot. The conclusions hold, with two refinements. First,
evenness is less tied to size than it looked. Its Spearman with bytes went from −0.53 to −0.39, so it now shares
about 15% of its rank variance with log bytes rather than about 28%. Second, the acquisition-versus-processing axis
is stronger, with PC1 rising from 39% to 45%. The per-file and dandiset-level numbers also converged, which is what
the larger dandiset count should do.

## Plain statement

Neither metric is a new complexity score. `D1` is largely object count, modulated by size, because larger files
concentrate their bytes in fewer datasets. Evenness is the one quantity close to independent of count. It still
carries a moderate size component, though weaker than the smaller sample suggested, so it only partly escapes the
"structure vs. size" collapse. What the byte-weighted view does add is where the bytes sit. Section composition,
acquisition-dominated versus processing-dominated, is a real, strong, dandiset-level distinction that the
unlabeled-tree metrics cannot express. If anything here earns a place in the whitepaper, it is that, and not `D1` or
evenness as a complexity score.

## Follow-up

- The 65 large files still being retried. Each needs either a longer timeout or a chunk-index-free size estimate.
- Zarr assets. This pass is HDF5 only.
- A walk without the per-dandiset cap. The largest dandiset alone holds 14,181 files, so that would need a different
  weighting scheme rather than just more time.
