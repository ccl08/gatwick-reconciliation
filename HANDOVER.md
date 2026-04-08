# Gatwick Affiliate Reconciliation — Handover Guide

## Live App
https://gatwick-reconciliation-nak6ms6cmq-nw.a.run.app/

## GitHub Repository
https://github.com/chriscespedes/gatwick-reconciliation

## How to use the app
Step-by-step instructions for a non-technical user:
1. Open the live app link above in your browser
2. Paste your Rakuten inquiry data (full sheet or raw Order IDs) into the input field
3. Click Run Reconciliation
4. Wait for the summary to appear in the browser (usually under 30 seconds)
5. Open the Google Sheet to find your results in a new tab named Output_MonthYear
   Sheet: https://docs.google.com/spreadsheets/d/1ir7kqEqlsnWyDkIEQGoCVqzXjMRhdd8PCAVl9rYXJEY

## Output
- Each run creates a new tab in the Google Sheet (e.g. Output_April2026)
- Each row is marked Matched or Unmatched
- If the Sheets write fails, a CSV is saved locally on the server as fallback

## BigQuery
- Project: spa-link-audit-305308
- Monthly inquiry tables are preserved (e.g. inquiry_202603_march) — do not delete these
- Used for dispute resolution

## Who to contact for access
- GCP Project: request access to spa-link-audit-305308 via Google Cloud Console
- Google Sheet: request edit access to Sheet ID 1ir7kqEqlsnWyDkIEQGoCVqzXjMRhdd8PCAVl9rYXJEY
- Cloud Run: managed under GCP project spa-link-audit-305308, region europe-west2

## Known limitations
- First load after inactivity may take 5–10 seconds (Cloud Run cold start)
- credentials.json is required locally for local runs — never commit this file
- DFP-prefixed Order IDs are flagged for manual review and not reconciled automatically

## Handover notes
[TODO — add any additional context for your successor here]
