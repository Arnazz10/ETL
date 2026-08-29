# Netflix ETL Frontend

Static dashboard for the generated ETL outputs.

## Run Locally

From the project root:

```bash
python3 -m http.server 8000 --directory frontend
```

Open:

```text
http://127.0.0.1:8000
```

## Refresh Data

Run the ETL pipeline first:

```bash
source .venv/bin/activate
python main.py
```

Then copy the latest dashboard exports:

```bash
bash scripts/sync_frontend_data.sh
```

## Deploy To Vercel

Set the Vercel project root directory to:

```text
frontend
```

No build command is required for this static version.
