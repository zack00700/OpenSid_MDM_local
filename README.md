# O.S MDM V2 — OpenSID Master Data Management

MDM générique avec module maritime optionnel. Import multi-sources, détection doublons, Golden Records, connecteurs API/ERP, write-back, reporting BI.

## Démarrage rapide

### Option 1 : Python (développement)
```bash
pip install -r requirements.txt
python3 start.py
```

### Option 2 : Docker (recommandé pour la production)
```bash
cp .env.example .env
# Éditez .env avec vos valeurs (surtout MDM_SECRET)
docker compose up -d
```

Ouvrez **http://127.0.0.1:3000**

> **Premier lancement** : un compte admin est créé automatiquement (`admin@osmdm.local` / `admin123`). **Changez le mot de passe immédiatement** via Profil > Mot de passe.

## Configuration

Toute la configuration se fait via **variables d'environnement** (voir `.env.example`) :

| Variable | Description | Obligatoire |
|----------|-------------|:-----------:|
| `MDM_SECRET` | Clé secrète JWT (32+ caractères) | **Oui (prod)** |
| `MDM_CORS_ORIGINS` | Origines autorisées (séparées par `,`) | Non |
| `MDM_ENCRYPT_KEY` | Clé de chiffrement des credentials | Non |
| `MDM_DEBUG` | Mode debug (`true`/`false`) | Non |
| `MDM_DB_ENGINE` | `sqlite` (défaut) ou `postgresql` | Non |
| `MDM_PG_URL` | URL PostgreSQL | Si postgresql |

## Structure

```
os-mdm-v2/
├── backend/
│   ├── app.py            # API Flask — MDM générique
│   └── maritime.py       # Module maritime optionnel
├── frontend/
│   ├── server.py         # Proxy Flask → port 3000
│   ├── static/js/        # JS (app.js + maritime.js)
│   └── templates/        # index.html
├── tests/
│   ├── test_api.py       # Tests pytest (40+ tests)
│   └── conftest.py       # Configuration tests
├── data/                 # SQLite DB (auto-créée)
├── uploads/              # Fichiers importés
├── .github/workflows/    # CI/CD GitHub Actions
├── Dockerfile            # Build Docker
├── docker-compose.yml    # Orchestration Docker
├── .env.example          # Template variables d'environnement
├── requirements.txt      # Dépendances Python
├── pytest.ini            # Configuration pytest
└── start.py              # Lanceur unique
```

## Fonctionnalités

### MDM Générique
- **Import** : CSV, Excel, bases de données externes (PostgreSQL, MySQL, MSSQL, Oracle)
- **Connecteurs API/ERP** : MarineTraffic, Salesforce, SAP, Odoo, REST custom
- **Détection doublons** : exact + fuzzy (thefuzz), blocking keys pour optimiser
- **Golden Records** : fusion configurable (most_complete, non_empty, source_priority)
- **Workflow** : draft → review → validated → published (contrôle par rôles)
- **Commentaires** : discussion par entité
- **Audit trail** : toutes les actions tracées
- **Reporting/BI** : KPIs, graphiques, tableaux croisés dynamiques
- **Export** : CSV (entités + Golden Records)
- **Write-back** : repousser les GR vers les sources (DB + API), dry-run

### Module Maritime
- **Navires** : validation IMO (checksum), MMSI, normalisation
- **Armateurs** : normalisation noms, classification
- **Ports** : UN/LOCODE, coordonnées, fonctions
- **Escales** : ETA/ETD, cargo, statuts
- **Vue 360°** : fiche complète navire avec relations
- **Dashboard qualité** : score de confiance, alertes

### Sécurité
- Hachage bcrypt (migration auto depuis SHA-256)
- JWT avec secret obligatoire en production
- Rate limiting (5 tentatives/min sur login)
- Chiffrement des credentials DB stockés
- CORS configurable et restrictif
- Security headers (CSP, X-Frame-Options, etc.)
- Validation des identifiants SQL (anti-injection)

## Tests

```bash
# Installer pytest
pip install pytest pytest-cov

# Lancer les tests
pytest tests/ -v

# Avec couverture
pytest tests/ -v --cov=backend --cov-report=html
```

## API

| Méthode | Endpoint | Description |
|---------|----------|-------------|
| `GET` | `/api/health` | Health check (sans auth) |
| `POST` | `/api/auth/login` | Authentification |
| `GET` | `/api/dashboard/stats` | Statistiques dashboard |
| `GET/POST` | `/api/entities` | CRUD entités |
| `POST` | `/api/import/csv` | Import fichier |
| `POST` | `/api/duplicates/detect` | Détection doublons |
| `POST` | `/api/golden-records/merge` | Fusion → Golden Record |
| `GET` | `/api/reporting/overview` | KPIs et tendances |
| `GET` | `/api/export/csv` | Export CSV |
| `POST` | `/api/writeback/push` | Write-back vers sources |

## Licence

Propriétaire — OpenSID. Tous droits réservés.
