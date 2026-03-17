# Real-Time Inventory Dashboard (FastAPI + SQLite)

## Run the backend

This project uses a local virtual environment in `venv/`.

```powershell
cd "C:\Users\venka\OneDrive\Desktop\Hakathon Project"
.\venv\Scripts\activate
uvicorn main:app --reload
```

Backend: `http://127.0.0.1:8000`

## Open the dashboard UI

- Open `http://127.0.0.1:8000/` (served by FastAPI)

The UI fetches live data from `GET /stock`.

## Dataset / database

- Source dataset: `retail_dataset.xlsx`
- SQLite DB: `inventory.db`

If you change/replace the Excel dataset, you can reload the DB:

```powershell
.\venv\Scripts\python load_data.py
.\venv\Scripts\python create_inventory.py
```

## Quick DB sanity check (optional)

```powershell
.\venv\Scripts\python inspect_db.py
```

