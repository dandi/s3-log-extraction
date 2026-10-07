# Byte-weighted structure: findings

Exploratory. Not yet folded into `WHITEPAPER.md`.

## Sample

The file was walked with `remfile` and `h5py` over the S3 blob URL. LINDI and the DANDI API were both unreachable from
the environment that ran it, so neither was used. The sample is stratified, at most 10 files per dandiset, random with
seed 0, drawn from the 4,493 files in `access_structure.csv`.

- Sampled 1,224 files from 253 dandisets. Walked **1,221 files from all 253 dandisets**.
- Three files failed. Each timed out after an hour of traversing a chunk index too large to read over HTTP:
  `21702bde-fa75-4cb4-b36a-1cd6732fe50e` (dandiset 1371, 236 GB), `38e53e31-103c-4639-b611-4abc58675a29`
  (552, 147 GB) and `42b9f9ad-a677-49f8-a887-21de353af178` (114, 49 GB). Nine other large files first timed out at
  15 minutes and were recovered by a second pass that skipped the chunk count. The largest file walked holds 438 GB.
  The sample therefore reaches the size tail, but its three largest unwalked files are missing.
- 2,768 datasets across 125 files had zero allocated storage. They are excluded from `D0` and from the shares.

Every statistic below is reported twice. "Files" means the per-file values, capped at 10 per dandiset. "Dandisets"
means one median per dandiset (n = 253).

## Figures

**1. `D1` against `D0`.** The points do not hug the `D1 = D0` line. The median file has 68 nonzero datasets but an
effective number of only 4.1, with a median evenness of 0.035. A third of files (32%) hold over 90% of their bytes in
a single dataset. So `D1` is not object count restated, but it is still strongly ordered by it, as figures 2 and 6
show.

**2. `D1` and evenness against bytes.** Holding size fixed, `D1` stays tied to count, with partial Spearman
`D1 ~ D0 | bytes` = +0.82 for both files and dandisets. Holding count fixed, larger files concentrate their bytes, with
partial Spearman `D1 ~ bytes | D0` = −0.64 for files and −0.54 for dandisets. Evenness has essentially no relation to
count (Spearman +0.06 for files, +0.02 for dandisets). It falls with size instead: Spearman −0.53 for files and −0.43
for dandisets, and partial `E ~ bytes | D0` = −0.54 and −0.43.

**3. ECDF of evenness.** Most files and most dandisets sit below 0.1. The dandiset-median curve tracks the file curve,
so the cap is not hiding a different picture.

**4. Lorenz curves.** These are eight files at evenly spaced evenness quantiles. At the low end, a single dataset
carries almost everything. Only the top quantiles approach the diagonal, and those are small files with few datasets.

**5. Section composition.** Bytes live in acquisition for 149 of 253 dandisets (median share > 0.5) and in processing
for 58. Stimulus, units and general take the bulk only in a minority. The CLR-PCA of section shares has its first two
components at 39% and 21%, and they separate along the acquisition-versus-processing axis.

**6. Validation panel.** Every metric is mostly a dandiset property, with between-dandiset ICC(1) of 0.81–0.90: log `D0`
0.84, log bytes 0.90, log cophenetic 0.82, log `D1` 0.82, log `D2` 0.81, evenness 0.87. Log cophenetic is count again
(Spearman with log `D0` 0.98). `D1` and `D2` are mostly count (0.71 and 0.67 for files, 0.75 and 0.71 for dandisets)
with a negative size component (−0.33 for files, −0.25 for dandisets). Evenness is uncorrelated with count (0.06 for
files, 0.02 for dandisets) and moderately anti-correlated with size (−0.53 for files, −0.43 for dandisets).

**7. Access, at dandiset level only (n = 253, medians).** Log bytes remains the strongest correlate of download fraction
(Spearman −0.73), followed by log `D0` (−0.51). `D1` (−0.21) and evenness (+0.25) are weak. With log `D0` and log
bytes both controlled, `D1` keeps a partial Spearman of −0.22 with download fraction. That is a small residual signal,
not a new predictor. No per-file access model was fitted.

## Plain statement

Neither metric separates cleanly. `D1` is largely object count, modulated by size: larger files concentrate their bytes
in fewer datasets. Evenness is the one quantity that is independent of count. But it is not independent of size,
because roughly a quarter of its rank variance is shared with log bytes. So it does not escape the
"structure vs. size" collapse either. It trades the count axis for a partial size axis. What the byte-weighted view
does add is where the bytes sit. Section composition, acquisition-dominated versus processing-dominated, is a real
and dandiset-level distinction that the unlabeled-tree metrics cannot express. If anything here earns a place in the
whitepaper, it is that, and not `D1` or evenness as a complexity score.

## Follow-up

- Zarr assets were not walked. This pass is HDF5 only.
- Each failed file needs either a longer timeout or a chunk-index-free size estimate.
- The full 4,493 files would let the dandiset-level numbers use more than 10 files per dandiset.
