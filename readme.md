# Mock Integration Runner

This utility drives the BillingPlatform mock integration using `contract_loader.py`. It now supports multi-product contracts, structured pricing tiers, multiple pricing segments per product, account products without contract rates, quantity amendments, price amendments, and repeatable reruns against an existing org.

## Prerequisites
- Python 3.10+ available on your PATH.
- BillingPlatform sandbox credentials that can create Contracts, Contract Currencies, Account Products, Contract Rates, and Pricing tiers.
- `.env` file in this directory containing:
  ```
  BP_LOGIN_URL=https://<tenant>/rest/2.0/login
  BP_API_BASE_URL=https://<tenant>/rest/2.0/
  BP_USERNAME=<username>
  BP_PASSWORD=<password>
  ```

## Input CSV
Each row represents a single product to load on a contract. Rows that share the same `contract` value are placed on the same BillingPlatform contract number and inherit the contract start date from the first row in that group.

| Column | Required | Description |
| --- | --- | --- |
| `action` | yes | Describes the order action to be performed on the row. For new sale, use 'Create'. For quantity amendment, use 'Quantity Change'. For price amendment, use 'Price Change'.| 
| `contract` | recommended | Logical contract grouping key; rows with the same value share one BillingPlatform contract. If omitted the script synthesizes a key per row. |
| `CPQ_Contractid` | recommended | Use if you are using a contract. During amendments, part of the query criteria to find the correct account product, contract rate, and pricing is the C_CPQContractid field.|
| `account_name` | yes | BillingPlatform Account Name. |
| `product_name` | yes | BillingPlatform Product Name. |
| `bundle_component` | yes | If TRUE, the script will only create an account product for this product, no contract rate. If anything but TRUE, a contract rate is created as well | 
| `currency_code` | yes | Currency for the contract and pricing tiers (e.g. `USD`). |
| `quantity` | no | Account product quantity (defaults to `1`). |
| `start_date` | no | Contract start date (`YYYY-MM-DD`); defaults to today when blank. |
| `effective_date` | no | Pricing effective date; defaults to the product start date when blank. |
| `rate` | no | Legacy single-rate fallback when no tier columns are present. |
| `contract_status` | no | Contract status (default `Terminated`). |
| `account_product_status` | no | Account product status (default `Active`). |
| `tierN_from_qty` | conditional | Structured tier lower bound for tier *N* (1-based). Leave blank to auto-chain from the previous tier. |
| `tierN_to_qty` | conditional | Structured tier upper bound for tier *N*. Use `-1` for unlimited. |
| `tierN_rate` | conditional | Structured tier rate for tier *N*. |
| `pricing_tiers` | optional | Legacy semicolon format (`upper:rate;...`) used only when no structured columns exist. |
| `contract_rate_only` | optional | When truthy (`true`, `yes`, `1`) the row skips account product creation and only creates/updates the contract rate and pricing tiers. |
| `pricing_only` | optional | Use if a given account product will have multiple unique pricing segments. See amendment section below. 

## New Sale

Example snippet:
```
contract,account_name,product_name,currency_code,quantity,start_date,effective_date,tier1_from_qty,tier1_to_qty,tier1_rate,tier2_to_qty,tier2_rate,contract_rate_only
test-001,Pat 2,Subscription - Linked Overage,USD,900,2025-09-30,2025-09-30,0,900,1.65,-1,0,
test-001,Pat 2,Usage - Node - Tiered,USD,900,2025-09-30,2025-09-30,0,2,0,-1,55,true
```

## Amendments

### Quantity Change
To change the quantity on one of the existing account products, use a structured row like so: 
|action         |contract  |CPQ_Contractid|account_name|product_name     |bundle_component|currency_code|quantity|start_date|contract_status|account_product_status|rate|effective_date|end_date|tier1_from_qty|tier1_to_qty|tier1_rate|tier2_from_qty|tier2_to_qty|tier2_rate|contract_rate_only|pricing_only|
|---------------|----------|--------------|------------|-----------------|----------------|-------------|--------|----------|---------------|----------------------|----|--------------|--------|--------------|------------|----------|--------------|------------|----------|------------------|------------|
|Quantity Change|Contract-A|1             |Neven 1.16  |UEM N-Sight Basic|                |USD          |1000    |2025-09-01|Terminated     |Active                |    |              |        |              |            |          |              |            |          |                  |            |


### Price Change

### Tier Rules
- Tiers are evaluated in the order provided; the script posts the unlimited (`-1`) tier first to satisfy aggregate pricing grid constraints.
- When `tierN_from_qty` is empty the script automatically sets it to the previous `tier(N-1)_to_qty + 0.0000000001`.
- All quantities are managed with high-precision decimals (`0.0000000001` step) to ensure BillingPlatform accepts the ranges.
- Include an unlimited tier if usage may exceed the last explicit band; otherwise BillingPlatform may reject later updates.

## Execution Flow
For each contract group the workflow performs:
1. **Login** - Authenticates and captures a `SessionID`.
2. **Account Resolution** -
   - Queries BillingPlatform for the account name; if missing, creates the account using the default attribute set.
   - Validates the `BillableBillingProfileId`. When the referenced profile is missing or points to another account, the loader creates a new billing profile with a unique `HostedPaymentPageExternalId`, waits for it to become queryable, and links it back via `PUT /ACCOUNT`.
3. **Contract Numbering** - Computes the next `YYYY-MM-DD_nn` value for the contract.
4. **Contract Create** - `POST /CONTRACT` using group-level details.
5. **Contract Currency** - `POST /CONTRACT_CURRENCY` for each unique currency encountered in the group.
6. **Billing Identifier Product** - Optionally adds the `Billing Identifier` product (`BillIdent` set to the contract number) so downstream integrations can locate the contract quickly.
7. **Product Loop per Row**
   - Account product creation (`POST /ACCOUNT_PRODUCT`) with timeout recovery, unless the row is marked `contract_rate_only`.
   - Contract rate creation (`POST /CONTRACT_RATE`) with automatic retry/lookup on connection drops.
   - Pricing tiers (`POST /PRICING`) honoring the tier definitions; existing tiers are detected via `GET /query` and skipped so reruns remain idempotent.
8. **Summary Output** - Prints the contract, currency responses, tier definitions, and API outcomes per product.
## CLI Options of Interest
- `--contract-group-column` (default `contract`) to target a different grouping field.
- `--tiers-column` (default `pricing_tiers`) to keep using the legacy semicolon format when structured columns are not available.
- `--billing-identifier-product-name` to customize or disable the helper account product.
- `--verbose` to enable DEBUG logging for API payloads.

Run the loader from this directory (or project root):
```
python contract_loader.py --input orders.csv --env-file .env
```

## Troubleshooting
- **Duplicate upper bands** - Ensure tier ranges do not overlap; the summary block shows the exact `from`/`to` values submitted. The script skips tiers that already exist on the contract rate, so remove or edit the tier in BillingPlatform if you need to change it.
- **404 during pricing lookup** - Expected when a new contract rate has no pricing yet; the script logs the event and proceeds.
- **Timeouts** - Account product creation automatically retries by querying the record. Other HTTP errors fail fast so you can resolve data or permission issues.
- **Billing profile errors** - If you see "Restriction on create" or "Packages cannot be added because the account has no billing profile," rerun after the loader provisions a dedicated billing profile. The script now generates a fresh profile and re-links the account automatically.
## Next Steps
- Extend pricing ingestion to handle multiple effective dates per product, allowing tier refreshes over time.
- Add helpers to bulk-tag rate-only rows or import pricing updates directly from historical contract rate exports.

Document last updated: 2025-12-12.






