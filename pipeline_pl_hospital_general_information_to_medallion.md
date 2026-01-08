# ADF Pipeline: pl_hospital_general_information_to_medallion

## Goal
- Copy public CMS CSV (HTTP) into ADLS **Bronze**
- Trigger Databricks notebook to produce **Silver** and **Gold** Delta outputs

**Public CSV used**
- https://data.cms.gov/provider-data/sites/default/files/resources/092256becd267d9eeccf73bf7d16c46b_1689206722/Hospital_General_Information.csv

---

## 1) Linked Services

### 1.1 HTTP
- Type: HTTP
- Auth: Anonymous (no auth)

### 1.2 ADLS Gen2
- Type: Azure Data Lake Storage Gen2
- Authentication: **Managed Identity**
- Storage account: your ADLS account

**RBAC required**
- Storage account IAM:
  - Role: Storage Blob Data Contributor
  - Principal: ADF **system-assigned managed identity**

### 1.3 Azure Databricks
- Type: Azure Databricks
- Authentication: **Managed service identity (MSI)** (no PAT token)
- Workspace: select your Databricks workspace

---

## 2) Datasets

### 2.1 HTTP dataset (DelimitedText)
- URL:
  - `https://data.cms.gov/provider-data/sites/default/files/resources/092256becd267d9eeccf73bf7d16c46b_1689206722/Hospital_General_Information.csv`
- First row as header: True
- Column delimiter: comma
- Encoding: UTF-8

### 2.2 ADLS Bronze dataset (DelimitedText)
- Linked service: ADLS Gen2 (MI)
- Container: `bronze`
- Directory:
  - `hospital_general_information/run_date=@{pipeline().parameters.run_date}`
- File name:
  - `hospital_general_information.csv`

---

## 3) Pipeline

Pipeline name:
- `pl_hospital_general_information_to_medallion`

### 3.1 Parameters
- `run_date` (String)
  - Default: `@formatDateTime(utcNow(),'yyyy-MM-dd')`
- `source_url` (String)
  - Default: `https://data.cms.gov/provider-data/sites/default/files/resources/092256becd267d9eeccf73bf7d16c46b_1689206722/Hospital_General_Information.csv`
- `storage_account` (String)
  - Default: your storage account name (e.g., `sthcmedallion01`)

### 3.2 Activities

#### Activity 1 — Copy data (HTTP → ADLS Bronze)
- Source: HTTP dataset (or parameterize URL from `source_url`)
- Sink: ADLS Bronze dataset (uses `run_date` in output path)

#### Activity 2 — Databricks Notebook
- Linked service: Databricks (MSI)
- Notebook path:
  - `/Workspace/Repos/<your_repo>/notebooks/01_hospital_general_information_medallion`
  - or upload notebook directly under Workspace
- Base parameters:
  - `storage_account`: `@{pipeline().parameters.storage_account}`
  - `run_date`: `@{pipeline().parameters.run_date}`

---

## 4) Verification checklist
- ADLS Bronze has:
  - `bronze/hospital_general_information/run_date=YYYY-MM-DD/hospital_general_information.csv`
- ADLS Silver has:
  - `silver/hospital_general_information/`
- ADLS Gold has:
  - `gold/hospital_general_information/hospitals_by_state/`
  - `gold/hospital_general_information/rating_distribution/`
  - `gold/hospital_general_information/hospitals_by_type/`
