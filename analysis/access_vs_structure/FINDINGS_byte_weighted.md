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

**7. Access, at dandiset level only (n = 422, medians).** Log bytes remains the strongest correlate of download
fraction (Spearman −0.74), followed by log `D0` (−0.48). `D1` (−0.18) and evenness (+0.16) are weak. With log `D0`
and log bytes both controlled, `D1` keeps a partial Spearman of −0.25 with download fraction. That is a small residual
signal, not a new predictor. No per-file access model was fitted.

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
evenness as a complexity score.

## Follow-up

- The 27 unwalked large files. Each needs either a longer timeout or a size estimate that avoids the chunk index.
- Zarr assets. This pass is HDF5 only.
- A walk without the per-dandiset cap. The largest dandiset alone holds 14,181 files, so that would need a different
  weighting scheme rather than just more time.
