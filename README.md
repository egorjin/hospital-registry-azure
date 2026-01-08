# healthcare-hospitals-medallion01 (ADF + ADLS Gen2 + Databricks, CSV, Managed Identity only)

## 1) Use case (what you say in interviews)
**“Public healthcare provider registry ingestion + quality-ready curated layer.”**

We ingest a public CMS/Medicare hospital registry CSV, land it in a data lake (Bronze), clean and standardize it (Silver), and publish analyst-ready aggregates (Gold) such as:
- hospitals by state (counts, % with emergency services)
- rating distribution
- hospitals by type

No PHI, no secrets, no keys. Access is enforced using **Managed Identity** end-to-end.

Data source (public):
- Hospital General Information CSV (CMS/Medicare open data):  
  `https://data.medicare.gov/api/views/xubh-q36u/rows.csv?accessType=DOWNLOAD` :contentReference[oaicite:1]{index=1}

---

## 2) Architecture
**Services**
- Azure Data Lake Storage Gen2 (ADLS) – storage for Bronze/Silver/Gold
- Azure Data Factory (ADF) – orchestration + ingestion (HTTP → ADLS) + triggers
- Azure Databricks – transformations (PySpark) + Delta outputs

**Flow**
1. ADF copies public CSV → ADLS Bronze (partitioned by run date)
2. ADF runs Databricks notebook
3. Databricks reads Bronze CSV → writes Silver Delta
4. Databricks creates Gold Delta aggregates

ASCII diagram:

[Public CMS CSV] --(ADF Copy Activity)--> [ADLS Bronze]
                                   |
                                   +--(ADF Databricks Activity)--> [Databricks Notebook]
                                                                  |-> [ADLS Silver (Delta)]
                                                                  |-> [ADLS Gold (Delta)]

---

## 3) What you will build (deliverables)
- ADLS containers: `bronze`, `silver`, `gold`
- ADF pipeline:
  - Copy Activity: HTTP source → ADLS Bronze sink
  - Databricks Notebook Activity: transforms Bronze→Silver→Gold
- Databricks notebook (PySpark):
  - standardize column names (snake_case)
  - fix data types (rating to int, zip to string)
  - clean “Not Available” values
  - deduplicate by provider_id
  - write Delta outputs

---

## 4) Step-by-step setup (Azure Portal friendly)

### Step 0 — Naming (keep it simple)
- Resource group: `rg-healthcare-hospitals-medallion01`
- Storage account: `sthcmedallion01` (must be globally unique)
- ADF: `adf-healthcare-hospitals01`
- Databricks: `adb-healthcare-hospitals01`

---

### Step 1 — Create Resource Group
1. Azure Portal → Resource groups → Create
2. Name: `rg-healthcare-hospitals-medallion01`
3. Region: pick one and keep **everything in the same region**

---

### Step 2 — Create ADLS Gen2 storage account (HNS ON)
1. Azure Portal → Storage accounts → Create
2. Name: `sthcmedallion01`
3. Performance: Standard (cheap)
4. Redundancy: LRS (cheap)
5. **Advanced tab → Enable hierarchical namespace = ON** (this makes it ADLS Gen2)

Create containers:
- `bronze`
- `silver`
- `gold`

Folder layout (you’ll see these after first run):
- `bronze/hospital_general_information/run_date=YYYY-MM-DD/hospital_general_information.csv`
- `silver/hospital_general_information/`
- `gold/hospital_general_information/`

---

### Step 3 — Create Azure Data Factory
1. Azure Portal → Data factories → Create
2. Name: `adf-healthcare-hospitals01`

Enable ADF System-assigned Managed Identity:
1. Open ADF resource → **Identity**
2. System assigned → Status: **On** → Save

Grant ADF access to ADLS (RBAC):
1. Open Storage account → Access Control (IAM) → Add role assignment
2. Role: **Storage Blob Data Contributor**
3. Assign access to: **Managed identity**
4. Select: your Data Factory (`adf-healthcare-hospitals01`) → Review + assign

This is the identity ADF will use for the Bronze landing.

ADF Managed Identity basics: :contentReference[oaicite:2]{index=2}

---

### Step 4 — Create Azure Databricks + “no secrets” storage access (Managed Identity)
To access ADLS from Databricks **without secrets**, the clean approach is:
- Create an **Access Connector for Azure Databricks** (Managed Identity)
- Grant it Storage RBAC
- Use Unity Catalog storage credential/external location/volume (recommended)

Microsoft’s guidance on using managed identities with Unity Catalog (no secret rotation): :contentReference[oaicite:3]{index=3}

**4.1 Create Access Connector**
1. Azure Portal → Create resource → search: **Access Connector for Azure Databricks**
2. Create it in the same resource group/region
3. Managed Identity tab → System-assigned: **On**
4. Create, then copy the connector **Resource ID**

**4.2 Grant connector access to Storage**
1. Storage account → IAM → Add role assignment
2. Role: **Storage Blob Data Contributor**
3. Assign access to: Managed identity
4. Select: the **Access Connector** you created → assign

**4.3 Create / enable Unity Catalog (lightweight)**
- In Databricks account console, ensure Unity Catalog is enabled for the workspace.
- Create a metastore rooted in a dedicated container/path (can be small).
- Create a Storage Credential using the Access Connector
- Create an External Location pointing at your `abfss://silver@...` and `abfss://gold@...` paths
- (Optional) Create Volumes for nicer paths

If your workspace doesn’t have Unity Catalog available, you’ll either need to enable it or accept a legacy auth pattern (which usually involves secrets). This project is intentionally **managed identity only**, so Unity Catalog is the right move.

---

### Step 5 — Configure ADF → Databricks with Managed Identity (no PAT token)
ADF can run Databricks activities using **Managed Identity authentication** (no Personal Access Token). :contentReference[oaicite:4]{index=4}

High level:
1. In Databricks workspace: grant ADF managed identity permission (Databricks Access Control)
2. In ADF: create Databricks linked service with Authentication type = **Managed service identity (MSI)**

Microsoft ADF blog describes the MSI-based Databricks linked service and that it removes secrets/tokens. :contentReference[oaicite:5]{index=5}

---

## 5) ADF build (pipeline)

### Linked Services
Create these in ADF Studio:
1) **HTTP Linked Service**
- No auth needed
- Base URL can be blank, you can put full URL in dataset

2) **ADLS Gen2 Linked Service**
- Auth: Managed Identity (System-assigned MI)
- Point to your storage account

3) **Azure Databricks Linked Service**
- Auth: **Managed service identity (MSI)** :contentReference[oaicite:6]{index=6}
- Workspace resource id: your Databricks workspace ARM id
- Cluster: choose a jobs cluster or existing cluster (keep it single-node + auto-terminate for cost)

### Datasets
1) HTTP dataset (DelimitedText)
- URL: `https://data.medicare.gov/api/views/xubh-q36u/rows.csv?accessType=DOWNLOAD` :contentReference[oaicite:7]{index=7}
- First row as header: true

2) ADLS dataset (DelimitedText)
- Container: `bronze`
- Directory: `hospital_general_information/run_date=@{pipeline().parameters.run_date}`
- File name: `hospital_general_information.csv`

### Pipeline: `pl_hospital_general_information_to_medallion`
Parameters:
- `run_date` (String) default: `@formatDateTime(utcNow(),'yyyy-MM-dd')`
- `source_url` (String) default: the CMS CSV URL

Activities:
1) **Copy data** (HTTP → ADLS Bronze)
- Source: HTTP dataset (use `source_url` if you parameterize)
- Sink: ADLS dataset (folder uses `run_date`)
- Enable “Skip incompatible row” = off (fail fast for portfolio clarity)

2) **Databricks Notebook Activity**
- Notebook path: `/Repos/.../notebooks/01_hospital_general_information_medallion`
- Base parameters:
  - `storage_account`: `sthcmedallion01`
  - `run_date`: `@{pipeline().parameters.run_date}`

---

## 6) Databricks notebook (PySpark) — Bronze → Silver → Gold
Create a notebook named:
`01_hospital_general_information_medallion`

### Widgets / parameters
```python
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("run_date", "")
storage_account = dbutils.widgets.get("storage_account")
run_date = dbutils.widgets.get("run_date")

bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/hospital_general_information/run_date={run_date}/hospital_general_information.csv"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/hospital_general_information"
gold_path   = f"abfss://gold@{storage_account}.dfs.core.windows.net/hospital_general_information"
