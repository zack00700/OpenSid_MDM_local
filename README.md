# O.S MDM V2 — OpenSID Master Data Management

MDM générique avec module maritime optionnel.

## Démarrage rapide

```bash
python3 start.py
```

Ouvre http://127.0.0.1:3000 — Login : `admin@osmdm.local` / `admin123`

## Structure

```
os-mdm-v2/
├── backend/
│   ├── app.py          # API Flask — MDM générique (2175 lignes)
│   └── maritime.py     # Module maritime optionnel (1364 lignes)
├── frontend/
│   ├── server.py       # Proxy Flask → port 3000
│   ├── static/js/
│   │   ├── app.js      # JS principal MDM
│   │   └── maritime.js # JS module maritime
│   └── templates/
│       └── index.html  # Interface complète
├── data/               # SQLite DB (auto-créée)
├── uploads/            # Fichiers importés
├── start.py            # Lanceur unique (backend + frontend)
├── requirements.txt
└── .gitignore
```

## Fonctionnalités

**MDM Générique :** Import CSV/Excel, détection doublons (exact + fuzzy), Golden Records, règles de fusion, workflow validation, commentaires, audit trail, reporting/BI, export CSV.

**Connecteurs :** Connexions DB (SQL Server, PostgreSQL, MySQL, Oracle), Connecteurs API/ERP (MarineTraffic, Salesforce, SAP, Odoo, REST custom), Write-back avec dry-run.

**Maritime :** Navires (IMO/MMSI validation), Armateurs (normalisation), Ports (LOCODE), Escales, vue 360°, dashboard qualité.
