# hospital-registry (ADF + ADLS Gen2 + Databricks, CSV, Managed Identity only)

A small, job-relevant Azure data engineering project using **public healthcare CSV data**, **ADLS Gen2**, **Azure Data Factory**, and **Azure Databricks**.

✅ **No secret keys / no PATs / no connection strings** — use **Managed Identity** end-to-end:
- **ADF** uses its **system-assigned managed identity** to orchestrate and (optionally) write to ADLS (RBAC).
- **ADF → Databricks** uses **MSI authentication** for the Databricks linked service (no PAT token).
- **Databricks → ADLS** uses a **managed identity** via an **Access Connector for Azure Databricks** + **Unity Catalog external location** (recommended approach for “no secrets”).

---

## 1) Use case 

**“Public healthcare provider registry ingestion + quality-ready curated layer.”**

1. Ingest public CMS hospital registry dataset, land it in a lake (Bronze)
2. Clean and standardize it (Silver)
3. Publish analyst-ready aggregates (Gold)
   - hospitals by state (counts, % with emergency services)
   - rating distribution
   - hospitals by type

No PHI. This is safe portfolio data.

---

## 2) External Data CSV Links

The CMS Provider Data Catalog often rotates the underlying **download file URL** behind a dataset. 

**NOTE** don’t hard-code the CSV file URL.
- Resolve the **current** download URL at runtime using the Provider Data Catalog **metastore API** and the dataset’s stable id:
- Dataset page (stable id): `xubh-q36u` (Hospital General Information)
- Metastore API pattern:
  - `https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/<DATASET_ID>?show-reference-ids=false`
  - `https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/xubh-q36u?show-reference-ids=false`

---

## 3) Architecture

**Services**
- **ADLS Gen2**: `bronze/`, `silver/`, `gold/`
- **Azure Data Factory**: orchestration + run notebook (optionally: Web+Copy)
- **Azure Databricks**: download + PySpark transforms + Delta outputs

**Flow (default, most reliable)**
1. ADF triggers Databricks notebook
2. Notebook resolves the current CMS download URL (metastore API)
3. Notebook downloads CSV → ADLS **Bronze** (partitioned by run date)
4. Notebook writes **Silver** Delta + **Gold** aggregates

```
[CMS Provider Data Catalog] --(metastore API)-> [Databricks]
                                       |-> download CSV -> [ADLS Bronze]
                                       |-> transform     -> [ADLS Silver (Delta)]
                                       |-> aggregate     -> [ADLS Gold (Delta)]
            ADF (MSI)  -----------------+-> orchestrate notebook run
```

---

## 4) Repo structure

```
hospital-registry/
├── README.md
├── .gitignore
├── notebooks/
│   └── 01_hospital_general_info_medallion.py
├── adf/
│   └── Hospital_Gen_Info.md
└── docs/
    └── managed-identity-notes.md
```

---

## 5) Step-by-step (Azure Portal friendly)

### Step 0 — Naming (keep it simple)
- Resource group: `rg-hospital-registry`
- Storage account: `storehospitalregistry` (must be globally unique)
- ADF: `adf-hospital-registry`
- Databricks: `adb-hospital-registry`

### Step 1 — Create ADLS Gen2 (HNS ON)
1. Create Storage account
2. Advanced → **Enable hierarchical namespace = ON**
3. Create containers:
   - `bronze`
   - `silver`
   - `gold`

Target layout:
- `bronze/hospital_general_info/run_date=YYYY-MM-DD/Hospital_General_Info.csv`
- `silver/hospital_general_info/` (Delta)
- `gold/hospital_general_info/hospitals_by_state/` (Delta)
- `gold/hospital_general_info/rating_distribution/` (Delta)
- `gold/hospital_general_info/hospitals_by_type/` (Delta)

### Step 2 — Create ADF + Managed Identity (no secrets)
1. Create Data Factory
2. ADF resource → **Identity** → System assigned → **On**
3. Storage account → **Access Control (IAM)** → Add role assignment:
   - Role: **Storage Blob Data Contributor**
   - Assign to: **ADF managed identity**

### Step 3 — Setup Secure Connection for Databricks (DB <----- ADLS)
  #### Create a Credential (to be used for an external location)
  1. `Catalog` → `External Data` → `Credential` → `Create credential`
  2. Credential type: `Azure Managed Identity`
  3. Credential name: `hospital-cred`
  4. Access connector ID: `/subscriptions/ca8b577e-..accessConnectors/unity-catalog-access-connector` (**FOUND BELOW**)
     - Azure portal → `rg-hospital-registry`(resource group) → `adb-hospital-registry` (db resource) →  (in middle of screen) Managed Resource Group: `databricks-rg-adb-hospital-registry-ay3xi2s24t262` → `unity-catalog-access-connector` (link at bottom)
     - <img width="552" height="179" alt="image" src="https://github.com/user-attachments/assets/ee42ad7b-6353-4255-a2a1-6144470dbe40" />
     - <img width="1145" height="473" alt="image" src="https://github.com/user-attachments/assets/3ccf46f6-37d8-463c-b315-ffd44ecd40fd" />
     - <img width="699" height="79" alt="image" src="https://github.com/user-attachments/assets/47bd0f05-ff82-469a-ae69-2d0d19a195ef" /> 
     - COPY Resource ID : `/subscriptions/ca8b577e-9fe../accessConnectors/unity-catalog-access-connector`
     </br>
  5. Grant Privledges `ALL PRIVLEDGES` to Credential
     - Under Credential `hospital-cred` → `Permissions`  → `Grant`
     - Grant on hospital-cred (screen)
        - Principals: <USER_ACC>
        - Privledges: `ALL PRIVLEDGES`
        - click `Confirm`
     - <img width="800" height="306" alt="image" src="https://github.com/user-attachments/assets/9c523519-641e-42d3-8f48-ed9a1aef646f" />
     </br>
  6. Grant RBAC Role on storage account
     - Azure Portal → Storage account (storehospitalregistry) → Access Control (IAM) → Add role assignment:
     - Give the credential’s identity: `Storage Blob Data Contributor` (this is the big one: read + write + list)
     - <img width="1125" height="670" alt="image" src="https://github.com/user-attachments/assets/a66e9d02-37c5-4755-a368-44fac9647b10" />
      </br>

  #### Create External Locations
  1. `Catalog` → `External Data` → `Create external location`
  2. Create 3 External Locations for all Medallion Stages
     1. Bronze
        1. External location name: `bronze`
        2. External location name: `abfss://bronze@storehospitalregistry .dfs.core.windows.net/` (**endpoint to ADLS container**)
        3. Storage Credential: `earthqual-cred` (from 5.3)
     2. Silver
        1. External location name: `silver`
        2. External location name: `abfss://silver@storehospitalregistry .dfs.core.windows.net/` (**endpoint to ADLS container**)
        3. Storage Credential: `earthqual-cred` (from 5.3)
     3. Gold
        1. External location name: `gold`
        2. External location name: `abfss://gold@storehospitalregistry .dfs.core.windows.net/` (**endpoint to ADLS container**)
        3. Storage Credential: `earthqual-cred` (from 5.3)



### Step 4 — Databricks Deployment
  - Launch Databricks workspace `earthquake-db`
  - What each Tab does:
    - `Workspace`: Store/Create Notebooks
    - `Catalog`:   Connect ADLS storage to be maniupulate
    - `Compute`:   Notebooks run here
   ### Create Compute instance
   1. Click create
   2. Policy: `Unrestricted` - `Single Node`
   3. Access mode: `Single user`
   4. Performance
      - [ ] Use Photon Acceleration (Not needed)
      - Node type: `General purpose` lowest (ie 14 GB Memory, 4 Cores)
      - [X] Terminate after 20 minutes
   5. Click `Create`

   <img width="921" height="580" alt="image" src="https://github.com/user-attachments/assets/cde542e9-a1c6-4592-bcec-77edeecd5e14" />

 ### Step 4.1 — Import the notebook
   - Upload `notebooks/01_hospital_general_info_medallion.py` to your Databricks workspace.

```python
"""
01_hospital_general_information_medallion.py

End-to-end (no-secrets) pipeline:
  1) Fetch the *current* CMS Provider Data Catalog download URL for a dataset_id
  2) Download CSV to ADLS Gen2 Bronze (run-date partition)
  3) Bronze (CSV) -> Silver (Delta) -> Gold (Delta aggregates)

Why this exists:
- CMS often rotates the underlying "file" URL for a dataset, so hard-coded CSV links break.
- The stable way is: call the Provider Data Catalog "metastore" API using the dataset_id,
  then read the downloadURL from the JSON response.

Parameters (ADF passes these):
- storage_account: e.g., sthcmedallion01
- run_date:        yyyy-MM-dd (partition)
- dataset_id:      CMS Provider Data Catalog dataset id (default: xubh-q36u)
- download_url_override: optional; if provided, notebook uses it instead of metastore

Assumptions:
- Your Databricks workspace can access ADLS using Managed Identity (recommended:
  Access Connector for Azure Databricks + Unity Catalog external locations/volumes).

Security:
- No keys, no secrets, no PATs.
"""

import json
import re
from datetime import datetime

import requests
from pyspark.sql import functions as F

# ---- widgets / parameters ----
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("dataset_id", "xubh-q36u")
dbutils.widgets.text("download_url_override", "")

storage_account = dbutils.widgets.get("storage_account").strip()
run_date = dbutils.widgets.get("run_date").strip()
dataset_id = dbutils.widgets.get("dataset_id").strip() or "xubh-q36u"
download_url_override = dbutils.widgets.get("download_url_override").strip()

if not storage_account or not run_date:
    raise ValueError("Missing required widgets: storage_account and run_date")

bronze_dir = (
    f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
    f"hospital_general_information/run_date={run_date}"
)
bronze_path = f"{bronze_dir}/hospital_general_information.csv"

silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/hospital_general_information"
gold_root = f"abfss://gold@{storage_account}.dfs.core.windows.net/hospital_general_information"

print("Dataset ID:", dataset_id)
print("Bronze:", bronze_path)
print("Silver:", silver_path)
print("Gold root:", gold_root)

# ---- helpers ----
def to_snake(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def normalize_nulls(df):
    nullish = ["Not Available", "NOT AVAILABLE", ""]
    for c in df.columns:
        df = df.withColumn(c, F.when(F.col(c).isin(nullish), None).otherwise(F.col(c)))
    return df


def resolve_cms_download_url(dataset_id: str) -> str:
    """Resolve the current download URL for a CMS Provider Data Catalog dataset.

    Tries to read distribution[*].data.downloadURL from the metastore response.
    If it can't find it, raises a ValueError with enough info to debug.
    """

    # NOTE: show-reference-ids=false keeps response cleaner (no internal IDs where possible)
    meta_url = (
        "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/"
        f"{dataset_id}?show-reference-ids=false"
    )

    r = requests.get(meta_url, timeout=60)
    r.raise_for_status()
    meta = r.json()

    dist = meta.get("distribution")

    # distribution can be a dict or list depending on dataset/schema
    candidates = []
    if isinstance(dist, dict):
        candidates = [dist]
    elif isinstance(dist, list):
        candidates = dist

    for d in candidates:
        if not isinstance(d, dict):
            continue
        data = d.get("data") if isinstance(d.get("data"), dict) else {}
        url = (
            data.get("downloadURL")
            or data.get("downloadUrl")
            or d.get("downloadURL")
            or d.get("downloadUrl")
        )
        if url and isinstance(url, str) and url.lower().startswith("http"):
            return url

    # If no downloadURL, surface what we *did* find, because CMS also exposes a SQL API.
    # Many examples use distribution.identifier (a UUID) + /api/1/datastore/sql.
    identifier = None
    if isinstance(dist, dict):
        identifier = dist.get("identifier")
    elif isinstance(dist, list) and dist and isinstance(dist[0], dict):
        identifier = dist[0].get("identifier")

    raise ValueError(
        "Could not find a downloadURL in metastore response. "
        f"dataset_id={dataset_id}. "
        f"Try opening the dataset page and/or inspect the metastore JSON. "
        f"distribution.identifier={identifier}"
    )


def download_csv_to_bronze(url: str, bronze_path: str):
    """Download CSV via HTTPS to a local temp file, then copy to ADLS (abfss)."""

    # Ensure directory exists
    dbutils.fs.mkdirs(bronze_dir)

    tmp_file = f"/tmp/hospital_general_information_{run_date}_{datetime.utcnow().strftime('%H%M%S')}.csv"

    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(tmp_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    # Overwrite bronze file (single file per run date)
    try:
        dbutils.fs.rm(bronze_path, True)
    except Exception:
        pass

    dbutils.fs.cp(f"file:{tmp_file}", bronze_path, True)

    print("✅ Downloaded to Bronze:", bronze_path)


# ---- 1) Resolve the current CSV URL (or use override) ----
if download_url_override:
    download_url = download_url_override
else:
    download_url = resolve_cms_download_url(dataset_id)

print("Resolved download URL:", download_url)

# ---- 2) Land in Bronze ----
download_csv_to_bronze(download_url, bronze_path)

# ---- 3) Bronze read ----
raw = (
    spark.read.option("header", True)
    .option("inferSchema", False)  # portfolio: enforce/convert explicitly below
    .csv(bronze_path)
)

# rename columns
for c in raw.columns:
    raw = raw.withColumnRenamed(c, to_snake(c))

df = normalize_nulls(raw)

# ---- 4) Standardize a few common columns (best-effort) ----
# CMS schema can evolve; if something breaks, print df.columns and update mapping.

# provider_id is common; sometimes it appears as "facility_id".
if "provider_id" not in df.columns:
    if "facility_id" in df.columns:
        df = df.withColumnRenamed("facility_id", "provider_id")
    else:
        raise ValueError(f"Expected 'provider_id' or 'facility_id' not found. Columns: {df.columns}")

# De-dup
_df = df.filter(F.col("provider_id").isNotNull()).dropDuplicates(["provider_id"])

# ZIP cleanup
if "zip_code" in _df.columns:
    _df = _df.withColumn(
        "zip_code",
        F.when(F.col("zip_code").isNull(), None).otherwise(
            F.lpad(F.regexp_extract(F.col("zip_code").cast("string"), r"(\\d+)", 1), 5, "0")
        ),
    )

# Phone digits only
if "phone_number" in _df.columns:
    _df = _df.withColumn("phone_number", F.regexp_replace(F.col("phone_number"), r"[^0-9]", ""))

# Overall rating to int
if "hospital_overall_rating" in _df.columns:
    _df = _df.withColumn("hospital_overall_rating", F.col("hospital_overall_rating").cast("int"))

# Emergency services boolean-ish
if "emergency_services" in _df.columns:
    _df = _df.withColumn(
        "has_emergency_services",
        F.when(F.upper(F.col("emergency_services")) == F.lit("YES"), F.lit(True))
        .when(F.upper(F.col("emergency_services")) == F.lit("NO"), F.lit(False))
        .otherwise(F.lit(None)),
    )

# lineage
_df = _df.withColumn("run_date", F.lit(run_date)).withColumn("ingested_at_utc", F.current_timestamp())

# ---- 5) Write Silver (append) ----
(
    _df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(silver_path)
)

print("✅ Wrote Silver Delta:", silver_path)

# ---- 6) Gold aggregates (overwrite each run for simplicity) ----
silver = spark.read.format("delta").load(silver_path)

# 1) hospitals by state
if "state" in silver.columns:
    by_state = (
        silver.groupBy("state")
        .agg(
            F.countDistinct("provider_id").alias("hospital_count"),
            F.avg("hospital_overall_rating").alias("avg_overall_rating"),
            (F.avg(F.col("has_emergency_services").cast("double")) * 100).alias(
                "pct_with_emergency_services"
            ),
        )
    )
    by_state.write.format("delta").mode("overwrite").save(f"{gold_root}/hospitals_by_state")
    print("✅ Wrote Gold: hospitals_by_state")
else:
    print("⚠️ 'state' column not found; skipping hospitals_by_state")

# 2) rating distribution
if "hospital_overall_rating" in silver.columns:
    rating_dist = (
        silver.groupBy("hospital_overall_rating")
        .agg(F.countDistinct("provider_id").alias("hospital_count"))
        .orderBy("hospital_overall_rating")
    )
    rating_dist.write.format("delta").mode("overwrite").save(f"{gold_root}/rating_distribution")
    print("✅ Wrote Gold: rating_distribution")
else:
    print("⚠️ 'hospital_overall_rating' column not found; skipping rating_distribution")

# 3) hospitals by type
if "hospital_type" in silver.columns:
    by_type = (
        silver.groupBy("hospital_type")
        .agg(
            F.countDistinct("provider_id").alias("hospital_count"),
            F.avg("hospital_overall_rating").alias("avg_overall_rating"),
        )
        .orderBy(F.desc("hospital_count"))
    )
    by_type.write.format("delta").mode("overwrite").save(f"{gold_root}/hospitals_by_type")
    print("✅ Wrote Gold: hospitals_by_type")
else:
    print("⚠️ 'hospital_type' column not found; skipping hospitals_by_type")

# ---- quick validation (optional) ----
print("\n--- Quick validation sample ---")
try:
    (
        spark.read.format("delta")
        .load(f"{gold_root}/hospitals_by_state")
        .orderBy(F.desc("hospital_count"))
        .show(10, False)
    )
except Exception as e:
    print("Validation skipped:", e)

```




### Step 5 — Build the ADF pipeline (orchestrate notebook)
Follow `adf/Hospital_Gen_Info.md`.

Default parameters:
- `dataset_id = xubh-q36u`
- `run_date = today`

---

## 6) Run it
1. Trigger the pipeline manually in ADF
2. Verify Bronze file exists in ADLS
3. Verify Silver/Gold Delta folders are created
4. In Databricks, check the printed “Quick validation sample” output

---

## 7) Cost control (do this)
- Use a **single-node** jobs cluster
- **Auto-terminate** at 10–15 minutes
- Don’t leave clusters running
- Run ADF trigger daily or manual

---

## 8) What this proves to employers
- Orchestration with ADF + parameters
- Medallion layering (Bronze/Silver/Gold)
- Real PySpark cleaning + standardization + aggregation
- Delta Lake outputs
- Managed Identity security patterns (no secrets)

---

## Notes
- CMS dataset schema can evolve. This notebook is defensive (casts + “Not Available” handling).
- If the provider id column name changes, inspect columns and update the mapping section.
