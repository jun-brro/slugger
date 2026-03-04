# Google Sheets Setup

## 1. Create a Google Cloud Project

1. Go to [Google Cloud Console](https://console.cloud.google.com)
2. Select project dropdown at top → **New Project** → enter name (e.g. `slugger`) → Create

## 2. Enable APIs

1. Left menu → **APIs & Services** → **Library**
2. Search `Google Sheets API` → click **Enable**
3. Search `Google Drive API` → click **Enable** (required for spreadsheet access)

> Direct link: https://console.cloud.google.com/apis/library/sheets.googleapis.com

## 3. Create Service Account + Download JSON Key

1. **APIs & Services** → **Credentials** → **+ Create Credentials** → **Service Account**
2. Name: `slugger` → Create → Done
3. Click the created service account → **Keys** tab → **Add Key** → **Create new key** → JSON → Download

> Downloaded file: `slugger-xxxxxx.json`

## 4. Create Spreadsheet + Share

1. Go to [Google Sheets](https://sheets.google.com) → create a new spreadsheet
2. Click **Share** (top right)
3. Open the downloaded JSON file and copy the `client_email` value (e.g. `slugger@project-id.iam.gserviceaccount.com`)
4. Add that email as an **Editor**

## 5. Link to Slugger

```bash
slugger login
```

```
Path to service account JSON: ~/Downloads/slugger-xxxxxx.json
Spreadsheet URL or ID: https://docs.google.com/spreadsheets/d/1BxiM.../edit
```

Config saved to `~/.slugger/config.toml`.

## Verify

```bash
slugger submit test.sh    # check that a row appears in the spreadsheet
```

## Troubleshooting

| Error | Fix |
|-------|-----|
| `403 Forbidden` | Share the spreadsheet with the service account email as **Editor** |
| `404 Not Found` | Check the spreadsheet ID (the part between `/d/` and `/edit` in the URL) |
| `Google Sheets API has not been enabled` | Enable both Sheets API and Drive API in Cloud Console |
| `credentials.json not found` | Re-run `slugger login` |
