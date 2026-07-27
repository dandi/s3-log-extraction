# Access vs. structure analysis

Exploratory analysis relating the **internal structure** and **size** of valid
NWB files on the DANDI Archive to how much they are **accessed** on the web.

It answers: *do structural-complexity metrics (group count, dataset count, Sackin
tree-imbalance index) predict web access, and how does asset size compare?*

## Data sources (all public, fetched over HTTPS)

| Quantity | Source | Key |
|---|---|---|
| groups / datasets / Sackin index | `dandi-cache/valid-nwb-file-to-*` caches (`dist` branch) | content ID |
| content ID → (dandiset, asset path) | `dandi-cache/content-id-to-nwb-file` cache (`dist` branch) | content ID |
| requests / downloads | `dandi/access-summaries` `content/summaries/<dandiset>/by_asset.tsv` | (dandiset, asset path) |
| asset size (bytes) | S3 `HEAD` on `dandiarchive.s3.amazonaws.com/blobs/<c[:3]>/<c[3:6]>/<content_id>` | content ID |

The DANDI REST API is **not** used (it is frequently firewalled); asset sizes are
read straight from S3 object headers instead.

## Usage

```bash
pip install requests tqdm numpy pandas matplotlib

# 1. Build the joined table (network-heavy: fetches caches, per-dandiset TSVs,
#    and one S3 HEAD per file). Produces access_structure.csv.
python build_dataset.py --out access_structure.csv --workers 20

# 2. Render figures + print the correlation summary.
python plot_relationships.py --data access_structure.csv --out-dir figures/
```

## Output columns (`access_structure.csv`)

`content_id, dandiset_id, asset_path, groups, datasets, sackin_index,
size_bytes, number_of_requests, number_of_downloads, requests_censored`

`number_of_requests` / `number_of_downloads` are privacy-rounded at the source;
values reported as `"<N"` are decoded to `N/2` and marked in `requests_censored`.
Streaming is derived as `number_of_requests - number_of_downloads`.

## Findings (snapshot, ~4,965 files across ~265 dandisets)

- **The three structural metrics are one axis.** Groups, datasets, and Sackin
  index are rank-correlated 0.83–0.94; Sackin is a nonlinear restatement of size
  (object count), saturating toward 0 for large files.
- **Structural complexity barely predicts access volume** (Spearman ≈ 0.08–0.18
  vs. total or streaming requests; log-log ≈ 0.36–0.41). It is a poor
  access-normalizer.
- **Complexity predicts consumption *mode*, not volume.** The download *fraction*
  falls with complexity (Spearman ≈ −0.30): complex files are read via partial
  range requests ("scrubbed"), simple files are downloaded whole.
- **Asset size is the dominant predictor of streaming** (Spearman ≈ +0.74,
  log-log ≈ +0.67) — far stronger than any structural metric.
- **Size and structural complexity are nearly orthogonal** (Spearman ≈ −0.07),
  and complexity still adds an independent signal after controlling for size
  (partial r ≈ +0.49): at equal size, more complex files are streamed more.

Numbers will drift as the caches and access summaries update; re-run to refresh.
