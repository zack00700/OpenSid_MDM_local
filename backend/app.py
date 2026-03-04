"""
O.S MDM — Master Data Management — Backend V2
Nouvelles fonctionnalités :
  - Connexion DB externe (PostgreSQL, MySQL, MSSQL, Oracle)
  - Reporting / BI (KPIs, graphiques, tableaux croisés)
  - Règles de fusion Golden Record configurables
  - Audit trail
"""

import os, json, uuid, hashlib, hmac, csv, sqlite3, time
import jwt, pandas as pd
from datetime import datetime, timedelta, timezone
from functools import wraps
from flask import Flask, request, jsonify, g, Response
from io import StringIO, BytesIO
from maritime import maritime_bp, MARITIME_SCHEMA

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.path.join(BASE_DIR, '..', 'data', 'mdm.db')
UPLOAD_DIR = os.path.join(BASE_DIR, '..', 'uploads')
SECRET_KEY = os.environ.get('MDM_SECRET', 'os-mdm-v2-secret-change-in-prod')

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
app.config['DB_PATH'] = DB_PATH

# ── §6.3 CACHE SIMPLE ────────────────────────
_cache = {}
def cached(key, ttl=30):
    if key in _cache and time.time() - _cache[key]['t'] < ttl:
        return _cache[key]['v']
    return None
def set_cache(key, val, ttl=30):
    _cache[key] = {'v': val, 't': time.time()}
    return val
app.register_blueprint(maritime_bp, url_prefix='/api/maritime')

@app.after_request
def cors(r):
    origin = request.headers.get('Origin', '*')
    r.headers['Access-Control-Allow-Origin']  = origin
    r.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
    r.headers['Access-Control-Allow-Methods'] = 'GET,POST,PUT,DELETE,OPTIONS'
    r.headers['Access-Control-Allow-Credentials'] = 'true'
    return r

@app.before_request
def options_handler():
    if request.method == 'OPTIONS':
        r = Response(); r.status_code = 204
        origin = request.headers.get('Origin', '*')
        r.headers['Access-Control-Allow-Origin']  = origin
        r.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
        r.headers['Access-Control-Allow-Methods'] = 'GET,POST,PUT,DELETE,OPTIONS'
        r.headers['Access-Control-Allow-Credentials'] = 'true'
        return r

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ── PASSWORD ──────────────────────────────────
def hash_pw(pwd):
    salt = os.urandom(16).hex()
    h = hashlib.sha256((salt+pwd).encode()).hexdigest()
    return f"{salt}:{h}"

def check_pw(pwd, stored):
    try:
        salt, h = stored.split(':', 1)
        return hmac.compare_digest(h, hashlib.sha256((salt+pwd).encode()).hexdigest())
    except: return False

# ── DATABASE ──────────────────────────────────
# §6.1 — Abstraction DB : PostgreSQL-ready
DB_ENGINE = os.environ.get('MDM_DB_ENGINE', 'sqlite')  # 'sqlite' ou 'postgresql'
PG_URL = os.environ.get('MDM_PG_URL', '')  # ex: postgresql://user:pass@host:5432/mdm

def sql_now():
    """Retourne la fonction SQL pour datetime courante (compatible SQLite et PostgreSQL)"""
    return "datetime('now')" if DB_ENGINE == 'sqlite' else "NOW()"

def sql_date_sub(days):
    """Retourne SQL pour 'il y a N jours'"""
    if DB_ENGINE == 'sqlite':
        return f"date('now','-{days} days')"
    return f"CURRENT_DATE - INTERVAL '{days} days'"

def sql_placeholder():
    """Retourne le placeholder SQL (? pour SQLite, %s pour PostgreSQL)"""
    return '?' if DB_ENGINE == 'sqlite' else '%s'

def get_db():
    if 'db' not in g:
        if DB_ENGINE == 'postgresql' and PG_URL:
            import psycopg2, psycopg2.extras
            g.db = psycopg2.connect(PG_URL)
            g.db.autocommit = False
        else:
            g.db = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
            g.db.row_factory = sqlite3.Row
            g.db.execute("PRAGMA journal_mode=WAL")
            g.db.execute("PRAGMA foreign_keys=ON")
    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop('db', None)
    if db: db.close()

def init_db():
    db = sqlite3.connect(DB_PATH)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY, email TEXT UNIQUE NOT NULL,
            password TEXT, name TEXT, role TEXT DEFAULT 'viewer',
            avatar TEXT, provider TEXT DEFAULT 'local',
            provider_id TEXT, active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now')),
            last_login TEXT
        );
        CREATE TABLE IF NOT EXISTS entities (
            id TEXT PRIMARY KEY, mdm_id TEXT UNIQUE, status TEXT DEFAULT 'active',
            source TEXT DEFAULT 'manual', data TEXT NOT NULL,
            -- §6.4 — Colonnes extraites pour requêtes directes sans parser JSON
            entity_name TEXT,
            entity_type TEXT,
            country TEXT,
            validation_status TEXT DEFAULT 'draft',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS duplicates (
            id TEXT PRIMARY KEY, entity1_id TEXT, entity2_id TEXT,
            score REAL DEFAULT 0, status TEXT DEFAULT 'pending',
            method TEXT DEFAULT 'exact',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS import_logs (
            id TEXT PRIMARY KEY, filename TEXT, source_label TEXT,
            total_rows INTEGER DEFAULT 0, imported INTEGER DEFAULT 0,
            errors INTEGER DEFAULT 0, status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS golden_records (
            id TEXT PRIMARY KEY, mdm_id TEXT UNIQUE, data TEXT,
            source_ids TEXT, rules_applied TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS db_connections (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, db_type TEXT NOT NULL,
            host TEXT, port INTEGER, database_name TEXT,
            username TEXT, password TEXT, status TEXT DEFAULT 'untested',
            last_tested TEXT, created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS fusion_rules (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, field TEXT NOT NULL,
            strategy TEXT NOT NULL, source_priority TEXT,
            condition_expr TEXT, active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS audit_log (
            id TEXT PRIMARY KEY, action TEXT, entity_type TEXT,
            entity_id TEXT, user_id TEXT, details TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS api_connectors (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            connector_type TEXT NOT NULL DEFAULT 'rest_api',
            base_url TEXT,
            auth_type TEXT DEFAULT 'none',
            auth_config TEXT DEFAULT '{}',
            headers TEXT DEFAULT '{}',
            data_path TEXT DEFAULT '',
            field_mapping TEXT DEFAULT '{}',
            target_type TEXT DEFAULT 'entity',
            sync_interval_minutes INTEGER DEFAULT 0,
            last_sync TEXT,
            last_sync_status TEXT DEFAULT 'never',
            last_sync_count INTEGER DEFAULT 0,
            enabled INTEGER DEFAULT 1,
            status TEXT DEFAULT 'untested',
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS writeback_configs (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            target_type TEXT NOT NULL DEFAULT 'db',
            connection_id TEXT,
            connector_id TEXT,
            target_table TEXT,
            api_endpoint TEXT,
            api_method TEXT DEFAULT 'POST',
            field_mapping TEXT DEFAULT '{}',
            mode TEXT DEFAULT 'upsert',
            match_key TEXT DEFAULT '',
            enabled INTEGER DEFAULT 1,
            last_run TEXT,
            last_run_status TEXT DEFAULT 'never',
            last_run_count INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS writeback_logs (
            id TEXT PRIMARY KEY,
            config_id TEXT,
            golden_record_id TEXT,
            status TEXT DEFAULT 'pending',
            records_sent INTEGER DEFAULT 0,
            records_success INTEGER DEFAULT 0,
            records_error INTEGER DEFAULT 0,
            error_details TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    cur = db.execute("SELECT id FROM users WHERE email='admin@osmdm.local'")
    if not cur.fetchone():
        db.execute("INSERT INTO users(id,email,password,name,role) VALUES(?,?,?,?,?)",
                   (str(uuid.uuid4()), 'admin@osmdm.local', hash_pw('admin123'), 'Administrateur', 'admin'))
    # Tables maritimes
    db.executescript("""
        CREATE TABLE IF NOT EXISTS maritime_vessels (
            id TEXT PRIMARY KEY, mdm_id TEXT UNIQUE, status TEXT DEFAULT 'active',
            source TEXT DEFAULT 'manual', imo_number TEXT UNIQUE, mmsi TEXT,
            vessel_name TEXT NOT NULL, vessel_name_normalized TEXT, vessel_type TEXT,
            flag_code TEXT, flag_name TEXT, gross_tonnage REAL, deadweight REAL,
            year_built INTEGER, owner_id TEXT, operator TEXT, class_society TEXT,
            data TEXT, validation_errors TEXT, confidence_score REAL DEFAULT 1.0,
            created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS maritime_owners (
            id TEXT PRIMARY KEY, mdm_id TEXT UNIQUE, status TEXT DEFAULT 'active',
            source TEXT DEFAULT 'manual', owner_name TEXT NOT NULL,
            owner_name_normalized TEXT, owner_type TEXT, country_code TEXT,
            country_name TEXT, city TEXT, address TEXT, contact_email TEXT,
            contact_phone TEXT, fleet_size INTEGER DEFAULT 0, data TEXT,
            created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS maritime_ports (
            id TEXT PRIMARY KEY, mdm_id TEXT UNIQUE, status TEXT DEFAULT 'active',
            source TEXT DEFAULT 'manual', port_name TEXT NOT NULL, un_locode TEXT UNIQUE,
            country_code TEXT, country_name TEXT, latitude REAL, longitude REAL,
            port_function TEXT, max_vessel_size TEXT, max_draft REAL,
            tide_dependent INTEGER DEFAULT 0, pilotage_required INTEGER DEFAULT 1, data TEXT,
            created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS maritime_port_calls (
            id TEXT PRIMARY KEY, mdm_id TEXT UNIQUE, status TEXT DEFAULT 'active',
            source TEXT DEFAULT 'manual', vessel_id TEXT, vessel_name TEXT,
            imo_number TEXT, port_id TEXT, port_name TEXT, un_locode TEXT,
            terminal TEXT, berth TEXT, eta TEXT, etd TEXT, ata TEXT, atd TEXT,
            call_status TEXT DEFAULT 'Planned', cargo_type TEXT, cargo_quantity REAL,
            cargo_unit TEXT, agent_name TEXT, voyage_number TEXT, data TEXT,
            created_at TEXT DEFAULT (datetime('now')), updated_at TEXT DEFAULT (datetime('now'))
        );
    """)
    # §3.2 — SQL INDEXES
    for idx in [
        "CREATE INDEX IF NOT EXISTS idx_entities_status ON entities(status)",
        "CREATE INDEX IF NOT EXISTS idx_entities_source ON entities(source)",
        "CREATE INDEX IF NOT EXISTS idx_entities_name ON entities(entity_name)",
        "CREATE INDEX IF NOT EXISTS idx_entities_vstatus ON entities(validation_status)",
        "CREATE INDEX IF NOT EXISTS idx_entities_etype ON entities(entity_type)",
        "CREATE INDEX IF NOT EXISTS idx_duplicates_status ON duplicates(status)",
        "CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_mv_imo ON maritime_vessels(imo_number)",
        "CREATE INDEX IF NOT EXISTS idx_mv_mmsi ON maritime_vessels(mmsi)",
        "CREATE INDEX IF NOT EXISTS idx_mv_name ON maritime_vessels(vessel_name_normalized)",
        "CREATE INDEX IF NOT EXISTS idx_mv_status ON maritime_vessels(status)",
        "CREATE INDEX IF NOT EXISTS idx_mv_confidence ON maritime_vessels(confidence_score)",
        "CREATE INDEX IF NOT EXISTS idx_mo_name ON maritime_owners(owner_name_normalized)",
        "CREATE INDEX IF NOT EXISTS idx_mp_locode ON maritime_ports(un_locode)",
        "CREATE INDEX IF NOT EXISTS idx_mpc_vessel ON maritime_port_calls(vessel_id)",
        "CREATE INDEX IF NOT EXISTS idx_mpc_port ON maritime_port_calls(port_id)",
        "CREATE INDEX IF NOT EXISTS idx_mpc_eta ON maritime_port_calls(eta)",
        "CREATE INDEX IF NOT EXISTS idx_mpc_status ON maritime_port_calls(call_status)",
    ]:
        try: db.execute(idx)
        except: pass
    # §4.1 — Entity comments table
    try:
        db.execute("""CREATE TABLE IF NOT EXISTS entity_comments (
            id TEXT PRIMARY KEY, entity_type TEXT, entity_id TEXT,
            user_id TEXT, user_name TEXT, comment TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )""")
    except: pass
    # §3.5 — FTS5 Full-text search tables
    for fts in [
        "CREATE VIRTUAL TABLE IF NOT EXISTS fts_vessels USING fts5(vessel_id, vessel_name, imo_number, mmsi, vessel_type, flag_name, content='')",
        "CREATE VIRTUAL TABLE IF NOT EXISTS fts_owners USING fts5(owner_id, owner_name, country_name, content='')",
        "CREATE VIRTUAL TABLE IF NOT EXISTS fts_ports USING fts5(port_id, port_name, un_locode, country_name, content='')",
    ]:
        try: db.execute(fts)
        except: pass
    # Règle de fusion par défaut (§4 — out-of-the-box)
    existing_rules = db.execute("SELECT COUNT(*) FROM fusion_rules").fetchone()[0]
    if existing_rules == 0:
        for rid, name, field, strategy in [
            (str(uuid.uuid4()), 'Valeur la plus complète', '*', 'most_complete'),
            (str(uuid.uuid4()), 'Email non vide', 'email', 'non_empty'),
            (str(uuid.uuid4()), 'Téléphone non vide', 'telephone', 'non_empty'),
        ]:
            try:
                db.execute("INSERT INTO fusion_rules(id,name,field,strategy,source_priority,active) VALUES(?,?,?,?,'[]',1)",
                           (rid, name, field, strategy))
            except: pass
    db.commit(); db.close()

def audit(action, entity_type, entity_id, details=None):
    try:
        db = get_db()
        uid = g.current_user.get('user_id','?') if hasattr(g,'current_user') else '?'
        db.execute("INSERT INTO audit_log(id,action,entity_type,entity_id,user_id,details) VALUES(?,?,?,?,?,?)",
                   (str(uuid.uuid4()), action, entity_type, entity_id, uid, json.dumps(details or {})))
        db.commit()
    except: pass

# ── AUTH ─────────────────────────────────────
def token_required(f):
    @wraps(f)
    def dec(*a, **kw):
        token = request.headers.get('Authorization','').replace('Bearer ','')
        if not token: return jsonify({'error':'Token manquant'}), 401
        try:
            g.current_user = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        except jwt.ExpiredSignatureError:
            return jsonify({'error':'Token expiré'}), 401
        except: return jsonify({'error':'Token invalide'}), 401
        return f(*a, **kw)
    return dec

@app.route('/api/auth/login', methods=['POST'])
def login():
    b = request.json or {}
    email = b.get('email','').strip().lower()
    pwd   = b.get('password','')
    db = get_db()
    u = db.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    if not u or not check_pw(pwd, u['password']):
        return jsonify({'error':'Identifiants invalides'}), 401
    token = jwt.encode({
        'user_id': u['id'], 'email': u['email'], 'role': u['role'],
        'exp': datetime.now(timezone.utc) + timedelta(hours=12)
    }, SECRET_KEY, algorithm='HS256')
    return jsonify({'token': token, 'user': {'id':u['id'],'email':u['email'],'name':u['name'],'role':u['role']}})

@app.route('/api/auth/me', methods=['GET'])
@token_required
def me():
    u = get_db().execute("SELECT id,email,name,role,created_at FROM users WHERE id=?",
                          (g.current_user['user_id'],)).fetchone()
    return jsonify(dict(u)) if u else (jsonify({'error':'Introuvable'}), 404)

# ── DASHBOARD ────────────────────────────────
@app.route('/api/dashboard/stats', methods=['GET'])
@token_required
def dashboard_stats():
    # §6.3 — Cache dashboard 15s
    c = cached('dashboard_stats', ttl=15)
    if c: return jsonify(c)
    db = get_db()
    total_e  = db.execute("SELECT COUNT(*) FROM entities WHERE status='active'").fetchone()[0]
    total_d  = db.execute("SELECT COUNT(*) FROM duplicates WHERE status='pending'").fetchone()[0]
    total_g  = db.execute("SELECT COUNT(*) FROM golden_records").fetchone()[0]
    total_i  = db.execute("SELECT COUNT(*) FROM import_logs").fetchone()[0]
    total_cn = db.execute("SELECT COUNT(*) FROM db_connections WHERE status='ok'").fetchone()[0]
    total_r  = db.execute("SELECT COUNT(*) FROM fusion_rules WHERE active=1").fetchone()[0]
    recent   = db.execute("SELECT id,source,created_at,data FROM entities WHERE status='active' ORDER BY created_at DESC LIMIT 5").fetchall()
    sources  = db.execute("SELECT source, COUNT(*) as cnt FROM entities WHERE status='active' GROUP BY source ORDER BY cnt DESC LIMIT 10").fetchall()
    trend    = db.execute("""SELECT DATE(created_at) as day, SUM(imported) as total FROM import_logs
                             WHERE created_at >= date('now','-7 days') GROUP BY day ORDER BY day""").fetchall()
    data = {
        'total_entities': total_e, 'total_duplicates': total_d,
        'total_golden': total_g, 'total_imports': total_i,
        'total_connections': total_cn, 'total_rules': total_r,
        'recent_entities': [dict(r) for r in recent],
        'sources_breakdown': [dict(s) for s in sources],
        'import_trend': [dict(t) for t in trend],
    }
    set_cache('dashboard_stats', data)
    return jsonify(data)

# ── IMPORT ───────────────────────────────────
# §6.4 — Extraire nom/type/pays du JSON pour les colonnes indexables
NAME_KEYS = ['nom', 'name', 'company', 'raison_sociale', 'nom_navire', 'vessel_name', 'owner_name', 'port_name', 'société', 'organisation']
TYPE_KEYS = ['type', 'entity_type', 'categorie', 'category']
COUNTRY_KEYS = ['pays', 'country', 'country_code', 'nation', 'nationalite']

def extract_meta(data_dict):
    """Extrait entity_name, entity_type, country depuis un dict de données"""
    if not isinstance(data_dict, dict): return None, None, None
    lower_map = {k.lower(): v for k, v in data_dict.items()}
    name = next((str(lower_map[k]) for k in NAME_KEYS if k in lower_map and lower_map[k]), None)
    if not name:  # Fallback: première valeur string non vide
        for v in data_dict.values():
            if isinstance(v, str) and v.strip():
                name = v.strip(); break
    etype = next((str(lower_map[k]) for k in TYPE_KEYS if k in lower_map and lower_map[k]), None)
    country = next((str(lower_map[k]) for k in COUNTRY_KEYS if k in lower_map and lower_map[k]), None)
    return name, etype, country

@app.route('/api/import/csv', methods=['POST'])
@token_required
def import_csv():
    if 'file' not in request.files:
        return jsonify({'error':'Aucun fichier'}), 400
    file = request.files['file']
    source_label = request.form.get('source_label', file.filename)
    ext = file.filename.rsplit('.',1)[-1].lower() if '.' in file.filename else ''
    log_id = str(uuid.uuid4())
    db = get_db()
    db.execute("INSERT INTO import_logs(id,filename,source_label,status) VALUES(?,?,?,'processing')",
               (log_id, file.filename, source_label))
    db.commit()
    try:
        if ext == 'csv':
            content = file.read().decode('utf-8-sig', errors='replace')
            df = pd.read_csv(StringIO(content))
        elif ext in ('xlsx','xls'):
            df = pd.read_excel(BytesIO(file.read()))
        else:
            return jsonify({'error':'Format non supporté'}), 400
        df = df.where(pd.notnull(df), None)
        total = len(df); imported = 0; errors = 0
        # §3.1 — BATCH INSERT (lots de 1000) + §6.4 colonnes extraites
        BATCH = 1000; batch = []
        for _, row in df.iterrows():
            try:
                eid = str(uuid.uuid4())
                row_dict = row.to_dict()
                data_json = json.dumps(row_dict, ensure_ascii=False, default=str)
                ename, etype, ecountry = extract_meta(row_dict)
                batch.append((eid, 'MDM-'+eid[:8].upper(), 'active', source_label, data_json,
                              ename, etype, ecountry, 'draft'))
                if len(batch) >= BATCH:
                    db.executemany("INSERT INTO entities(id,mdm_id,status,source,data,entity_name,entity_type,country,validation_status) VALUES(?,?,?,?,?,?,?,?,?)", batch)
                    imported += len(batch); batch = []
            except: errors += 1
        if batch:
            db.executemany("INSERT INTO entities(id,mdm_id,status,source,data,entity_name,entity_type,country,validation_status) VALUES(?,?,?,?,?,?,?,?,?)", batch)
            imported += len(batch)
        _cache.pop('dashboard_stats', None)
        db.execute("UPDATE import_logs SET total_rows=?,imported=?,errors=?,status='done' WHERE id=?",
                   (total, imported, errors, log_id))
        db.commit()
        audit('import', 'import_log', log_id, {'file': file.filename, 'imported': imported})
        return jsonify({'log_id':log_id,'total':total,'imported':imported,'errors':errors})
    except Exception as e:
        db.execute("UPDATE import_logs SET status='error' WHERE id=?", (log_id,))
        db.commit()
        return jsonify({'error': str(e)}), 500

@app.route('/api/import/logs', methods=['GET'])
@token_required
def import_logs():
    logs = get_db().execute("SELECT * FROM import_logs ORDER BY created_at DESC LIMIT 50").fetchall()
    return jsonify([dict(l) for l in logs])

# ── ENTITIES ─────────────────────────────────
@app.route('/api/entities/sources', methods=['GET'])
@token_required
def list_entity_sources():
    rows = get_db().execute("SELECT DISTINCT source FROM entities WHERE status='active' AND source IS NOT NULL ORDER BY source").fetchall()
    return jsonify([r['source'] for r in rows])

@app.route('/api/entities', methods=['GET'])
@token_required
def list_entities():
    db = get_db()
    page = int(request.args.get('page',1))
    per  = int(request.args.get('per_page',20))
    srch = request.args.get('search','').strip()
    src  = request.args.get('source','').strip()
    off  = (page-1)*per
    wh   = ["status='active'"]; params = []
    if srch: wh.append("data LIKE ?"); params.append(f'%{srch}%')
    if src:  wh.append("source LIKE ?"); params.append(f'%{src}%')
    w = ' AND '.join(wh)
    total = db.execute(f"SELECT COUNT(*) FROM entities WHERE {w}", params).fetchone()[0]
    rows  = db.execute(f"SELECT * FROM entities WHERE {w} ORDER BY created_at DESC LIMIT ? OFFSET ?",
                       params+[per,off]).fetchall()
    ents = []
    for r in rows:
        e = dict(r)
        try: e['data'] = json.loads(e['data'])
        except: pass
        ents.append(e)
    return jsonify({'total':total,'page':page,'per_page':per,'entities':ents})

@app.route('/api/entities/<eid>', methods=['GET'])
@token_required
def get_entity(eid):
    r = get_db().execute("SELECT * FROM entities WHERE id=?", (eid,)).fetchone()
    if not r: return jsonify({'error':'Introuvable'}), 404
    e = dict(r)
    try: e['data'] = json.loads(e['data'])
    except: pass
    return jsonify(e)

@app.route('/api/entities', methods=['POST'])
@token_required
def create_entity():
    b = request.json or {}
    eid = str(uuid.uuid4())
    mid = 'MDM-'+eid[:8].upper()
    db  = get_db()
    data_dict = b.get('data', {})
    data_json = json.dumps(data_dict, ensure_ascii=False)
    ename, etype, ecountry = extract_meta(data_dict)
    db.execute("INSERT INTO entities(id,mdm_id,status,source,data,entity_name,entity_type,country,validation_status) VALUES(?,?,'active','manual',?,?,?,?,'draft')",
               (eid, mid, data_json, ename, etype, ecountry))
    db.commit()
    audit('create','entity',eid)
    return jsonify({'id':eid,'mdm_id':mid}), 201

@app.route('/api/entities/<eid>', methods=['PUT'])
@token_required
def update_entity(eid):
    b = request.json or {}
    db = get_db()
    if not db.execute("SELECT id FROM entities WHERE id=?", (eid,)).fetchone():
        return jsonify({'error':'Introuvable'}), 404
    data_dict = b.get('data', {})
    data_json = json.dumps(data_dict, ensure_ascii=False)
    ename, etype, ecountry = extract_meta(data_dict)
    db.execute("UPDATE entities SET data=?,entity_name=?,entity_type=?,country=?,updated_at=datetime('now') WHERE id=?",
               (data_json, ename, etype, ecountry, eid))
    db.commit()
    audit('update','entity',eid)
    return jsonify({'message':'Mis à jour'})

@app.route('/api/entities/<eid>', methods=['DELETE'])
@token_required
def delete_entity(eid):
    db = get_db()
    db.execute("UPDATE entities SET status='deleted' WHERE id=?", (eid,))
    db.commit()
    audit('delete','entity',eid)
    return jsonify({'message':'Supprimé'})

# ── DB CONNECTIONS ────────────────────────────
def _make_conn(info):
    db_type = info['db_type'].lower()
    h = info.get('host','localhost')
    p = info.get('port')
    db_name = info.get('database_name','')
    user = info.get('username','')
    pwd  = info.get('password','')
    if db_type == 'postgresql':
        import psycopg2
        return psycopg2.connect(host=h, port=p or 5432, dbname=db_name, user=user, password=pwd, connect_timeout=10)
    elif db_type in ('mysql','mariadb'):
        import pymysql
        return pymysql.connect(host=h, port=int(p or 3306), database=db_name, user=user, password=pwd, connect_timeout=10)
    elif db_type == 'mssql':
        import pyodbc
        return pyodbc.connect(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={h},{p or 1433};DATABASE={db_name};UID={user};PWD={pwd};timeout=10")
    elif db_type == 'oracle':
        import cx_Oracle
        dsn = cx_Oracle.makedsn(h, p or 1521, service_name=db_name)
        return cx_Oracle.connect(user=user, password=pwd, dsn=dsn)
    else:
        import sqlite3 as _sq
        return _sq.connect(db_name)

@app.route('/api/connections', methods=['GET'])
@token_required
def list_connections():
    rows = get_db().execute("SELECT id,name,db_type,host,port,database_name,username,status,last_tested,created_at FROM db_connections ORDER BY created_at DESC").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/connections', methods=['POST'])
@token_required
def create_connection():
    b = request.json or {}
    cid = str(uuid.uuid4())
    get_db().execute("INSERT INTO db_connections(id,name,db_type,host,port,database_name,username,password,status) VALUES(?,?,?,?,?,?,?,?,'untested')",
               (cid, b.get('name'), b.get('db_type'), b.get('host'), b.get('port'),
                b.get('database_name'), b.get('username'), b.get('password','')))
    get_db().commit()
    return jsonify({'id': cid}), 201

@app.route('/api/connections/<cid>', methods=['PUT'])
@token_required
def update_connection(cid):
    b = request.json or {}
    db = get_db()
    db.execute("UPDATE db_connections SET name=?,db_type=?,host=?,port=?,database_name=?,username=?,password=? WHERE id=?",
               (b.get('name'), b.get('db_type'), b.get('host'), b.get('port'),
                b.get('database_name'), b.get('username'), b.get('password',''), cid))
    db.commit()
    return jsonify({'message':'Mis à jour'})

@app.route('/api/connections/<cid>', methods=['DELETE'])
@token_required
def delete_connection(cid):
    get_db().execute("DELETE FROM db_connections WHERE id=?", (cid,))
    get_db().commit()
    return jsonify({'message':'Supprimé'})

@app.route('/api/connections/<cid>/test', methods=['POST'])
@token_required
def test_connection(cid):
    db  = get_db()
    row = db.execute("SELECT * FROM db_connections WHERE id=?", (cid,)).fetchone()
    if not row: return jsonify({'error':'Introuvable'}), 404
    try:
        conn = _make_conn(dict(row))
        conn.cursor().execute("SELECT 1")
        conn.close()
        db.execute("UPDATE db_connections SET status='ok', last_tested=datetime('now') WHERE id=?", (cid,))
        db.commit()
        return jsonify({'status':'ok', 'message':'Connexion réussie ✅'})
    except Exception as e:
        db.execute("UPDATE db_connections SET status='error', last_tested=datetime('now') WHERE id=?", (cid,))
        db.commit()
        return jsonify({'status':'error', 'message': str(e)}), 400

@app.route('/api/connections/<cid>/tables', methods=['GET'])
@token_required
def list_tables(cid):
    row = get_db().execute("SELECT * FROM db_connections WHERE id=?", (cid,)).fetchone()
    if not row: return jsonify({'error':'Introuvable'}), 404
    try:
        conn = _make_conn(dict(row))
        cur  = conn.cursor()
        db_type = row['db_type'].lower()
        if db_type == 'postgresql':
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
        elif db_type in ('mysql','mariadb'):
            cur.execute("SHOW TABLES")
        elif db_type == 'mssql':
            cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
        elif db_type == 'oracle':
            cur.execute("SELECT table_name FROM user_tables ORDER BY table_name")
        else:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]
        conn.close()
        return jsonify({'tables': tables})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

def _select_with_limit(db_type: str, table: str, limit: int) -> str:
    db_type = (db_type or "").lower()
    if db_type == "mssql":
        return f"SELECT TOP ({limit}) * FROM [{table}]"
    elif db_type in ("mysql", "mariadb"):
        return f"SELECT * FROM `{table}` LIMIT {limit}"
    elif db_type == "postgresql":
        return f'SELECT * FROM "{table}" LIMIT {limit}'
    elif db_type == "oracle":
        return f'SELECT * FROM "{table}" FETCH FIRST {limit} ROWS ONLY'
    else:
        return f'SELECT * FROM "{table}" LIMIT {limit}'

@app.route('/api/connections/<cid>/preview', methods=['POST'])
@token_required
def preview_table(cid):
    row = get_db().execute("SELECT * FROM db_connections WHERE id=?", (cid,)).fetchone()
    if not row:
        return jsonify({'error': 'Introuvable'}), 404

    table = (request.json or {}).get('table', '')
    if not table:
        return jsonify({'error': 'Table requise'}), 400

    if not all(c.isalnum() or c in '_.' for c in table):
        return jsonify({'error': 'Nom de table invalide'}), 400

    try:
        conn = _make_conn(dict(row))

        db_type = (row["db_type"] or "").lower()
        if db_type == "mssql":
            sql = f"SELECT TOP (20) * FROM [{table}]"
        else:
            sql = f'SELECT * FROM "{table}" LIMIT 20'

        df = pd.read_sql(sql, conn)
        conn.close()

        return jsonify({
            'columns': list(df.columns),
            'rows': df.fillna('').to_dict(orient='records')
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/connections/<cid>/import', methods=['POST'])
@token_required
def import_from_db(cid):
    row = get_db().execute("SELECT * FROM db_connections WHERE id=?", (cid,)).fetchone()
    if not row: return jsonify({'error':'Introuvable'}), 404
    b     = request.json or {}
    table = b.get('table','')
    query = b.get('query','')
    limit = int(b.get('limit', 10000))
    if not table and not query: return jsonify({'error':'Table ou SQL requis'}), 400
    if not query:
        if not all(c.isalnum() or c in '_.' for c in table):
            return jsonify({'error':'Nom de table invalide'}), 400
        query = _select_with_limit(row["db_type"], table, limit)
    source_label = b.get('source_label') or f"{row['name']}.{table or 'query'}"
    log_id = str(uuid.uuid4())
    db = get_db()
    db.execute("INSERT INTO import_logs(id,filename,source_label,status) VALUES(?,?,?,'processing')",
               (log_id, f"[DB] {source_label}", source_label))
    db.commit()
    try:
        conn = _make_conn(dict(row))
        df   = pd.read_sql(query, conn)
        conn.close()
        df = df.where(pd.notnull(df), None)
        total = len(df); imported = 0; errors = 0
        # §3.1 — BATCH INSERT DB externe
        BATCH = 1000; batch = []
        for _, row_data in df.iterrows():
            try:
                eid = str(uuid.uuid4())
                batch.append((eid, 'MDM-'+eid[:8].upper(), 'active', source_label,
                              json.dumps(row_data.to_dict(), ensure_ascii=False, default=str)))
                if len(batch) >= BATCH:
                    db.executemany("INSERT INTO entities(id,mdm_id,status,source,data) VALUES(?,?,?,?,?)", batch)
                    imported += len(batch); batch = []
            except: errors += 1
        if batch:
            db.executemany("INSERT INTO entities(id,mdm_id,status,source,data) VALUES(?,?,?,?,?)", batch)
            imported += len(batch)
        _cache.pop('dashboard_stats', None)
        db.execute("UPDATE import_logs SET total_rows=?,imported=?,errors=?,status='done' WHERE id=?",
                   (total, imported, errors, log_id))
        db.commit()
        audit('db_import','import_log',log_id,{'source':source_label,'imported':imported})
        return jsonify({'log_id':log_id,'total':total,'imported':imported,'errors':errors})
    except Exception as e:
        db.execute("UPDATE import_logs SET status='error' WHERE id=?", (log_id,))
        db.commit()
        return jsonify({'error': str(e)}), 500

# ── API / ERP CONNECTORS ────────────────────────
import urllib.request, urllib.error

CONNECTOR_PRESETS = {
    'marinetraffic': {
        'name': 'MarineTraffic AIS',
        'base_url': 'https://services.marinetraffic.com/api/exportvessels/v:8',
        'auth_type': 'query_param',
        'auth_config': {'param_name': 'apikey', 'apikey': ''},
        'data_path': '',
        'icon': '🛰️',
        'description': 'Données AIS temps réel — positions, navires, escales',
        'target_type': 'vessel',
    },
    'vesselfinder': {
        'name': 'VesselFinder',
        'base_url': 'https://api.vesselfinder.com/vessels',
        'auth_type': 'query_param',
        'auth_config': {'param_name': 'userkey', 'apikey': ''},
        'data_path': '',
        'icon': '🚢',
        'description': 'Données navires et tracking AIS',
        'target_type': 'vessel',
    },
    'fleetmon': {
        'name': 'FleetMon',
        'base_url': 'https://apiv2.fleetmon.com/regional_tracking',
        'auth_type': 'basic',
        'auth_config': {'username': '', 'password': ''},
        'data_path': 'vessels',
        'icon': '📡',
        'description': 'Monitoring de flotte et tracking',
        'target_type': 'vessel',
    },
    'odoo': {
        'name': 'Odoo ERP',
        'base_url': 'https://your-instance.odoo.com/api/v1',
        'auth_type': 'bearer',
        'auth_config': {'token': ''},
        'data_path': 'records',
        'icon': '🏭',
        'description': 'ERP — contacts, produits, factures',
        'target_type': 'entity',
    },
    'salesforce': {
        'name': 'Salesforce CRM',
        'base_url': 'https://your-instance.salesforce.com/services/data/v58.0/query',
        'auth_type': 'bearer',
        'auth_config': {'token': ''},
        'data_path': 'records',
        'icon': '☁️',
        'description': 'CRM — comptes, contacts, opportunités',
        'target_type': 'entity',
    },
    'sap': {
        'name': 'SAP OData',
        'base_url': 'https://your-sap-host/sap/opu/odata/sap/',
        'auth_type': 'basic',
        'auth_config': {'username': '', 'password': ''},
        'data_path': 'd.results',
        'icon': '🔷',
        'description': 'ERP SAP — données maîtres via OData',
        'target_type': 'entity',
    },
    'custom_rest': {
        'name': 'API REST personnalisée',
        'base_url': '',
        'auth_type': 'none',
        'auth_config': {},
        'data_path': '',
        'icon': '🔗',
        'description': 'Connecteur API REST générique',
        'target_type': 'entity',
    },
}

@app.route('/api/connectors/presets', methods=['GET'])
@token_required
def list_connector_presets():
    return jsonify(CONNECTOR_PRESETS)

@app.route('/api/connectors', methods=['GET'])
@token_required
def list_connectors():
    rows = get_db().execute("SELECT * FROM api_connectors ORDER BY created_at DESC").fetchall()
    result = []
    for r in rows:
        d = dict(r)
        for k in ('auth_config', 'headers', 'field_mapping'):
            try: d[k] = json.loads(d[k])
            except: pass
        result.append(d)
    return jsonify(result)

@app.route('/api/connectors', methods=['POST'])
@token_required
def create_connector():
    b = request.json or {}
    cid = str(uuid.uuid4())
    db = get_db()
    db.execute("""INSERT INTO api_connectors(id,name,connector_type,base_url,auth_type,auth_config,
                  headers,data_path,field_mapping,target_type,sync_interval_minutes,enabled)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        (cid, b.get('name',''), b.get('connector_type','rest_api'),
         b.get('base_url',''), b.get('auth_type','none'),
         json.dumps(b.get('auth_config',{})), json.dumps(b.get('headers',{})),
         b.get('data_path',''), json.dumps(b.get('field_mapping',{})),
         b.get('target_type','entity'), int(b.get('sync_interval_minutes', 0)),
         1 if b.get('enabled', True) else 0))
    db.commit()
    audit('create', 'connector', cid, {'name': b.get('name')})
    return jsonify({'id': cid, 'message': 'Connecteur créé'}), 201

@app.route('/api/connectors/<cid>', methods=['PUT'])
@token_required
def update_connector(cid):
    b = request.json or {}
    db = get_db()
    db.execute("""UPDATE api_connectors SET name=?,connector_type=?,base_url=?,auth_type=?,
                  auth_config=?,headers=?,data_path=?,field_mapping=?,target_type=?,
                  sync_interval_minutes=?,enabled=?,updated_at=datetime('now') WHERE id=?""",
        (b.get('name',''), b.get('connector_type','rest_api'),
         b.get('base_url',''), b.get('auth_type','none'),
         json.dumps(b.get('auth_config',{})), json.dumps(b.get('headers',{})),
         b.get('data_path',''), json.dumps(b.get('field_mapping',{})),
         b.get('target_type','entity'), int(b.get('sync_interval_minutes', 0)),
         1 if b.get('enabled', True) else 0, cid))
    db.commit()
    return jsonify({'message': 'Mis à jour'})

@app.route('/api/connectors/<cid>', methods=['DELETE'])
@token_required
def delete_connector(cid):
    get_db().execute("DELETE FROM api_connectors WHERE id=?", (cid,)); get_db().commit()
    return jsonify({'message': 'Supprimé'})

def _call_api_connector(connector):
    """Appelle une API REST et retourne la liste de records JSON"""
    url = connector['base_url']
    auth_type = connector.get('auth_type', 'none')
    auth_cfg = connector.get('auth_config', {})
    if isinstance(auth_cfg, str):
        try: auth_cfg = json.loads(auth_cfg)
        except: auth_cfg = {}
    hdrs = connector.get('headers', {})
    if isinstance(hdrs, str):
        try: hdrs = json.loads(hdrs)
        except: hdrs = {}

    # Construire headers d'auth
    if auth_type == 'bearer':
        hdrs['Authorization'] = f"Bearer {auth_cfg.get('token','')}"
    elif auth_type == 'api_key':
        hdrs[auth_cfg.get('header_name','X-API-Key')] = auth_cfg.get('apikey','')
    elif auth_type == 'query_param':
        sep = '&' if '?' in url else '?'
        url = f"{url}{sep}{auth_cfg.get('param_name','apikey')}={auth_cfg.get('apikey','')}"

    hdrs.setdefault('Accept', 'application/json')
    hdrs.setdefault('User-Agent', 'OS-MDM-V2/1.0')

    req = urllib.request.Request(url, headers=hdrs)
    if auth_type == 'basic':
        import base64
        creds = base64.b64encode(f"{auth_cfg.get('username','')}:{auth_cfg.get('password','')}".encode()).decode()
        req.add_header('Authorization', f'Basic {creds}')

    resp = urllib.request.urlopen(req, timeout=30)
    body = json.loads(resp.read().decode())

    # Extraire les records via data_path (ex: "d.results", "records", "data.vessels")
    data_path = connector.get('data_path', '')
    if data_path:
        for part in data_path.split('.'):
            if isinstance(body, dict):
                body = body.get(part, body)
            elif isinstance(body, list) and part.isdigit():
                body = body[int(part)]

    if isinstance(body, dict):
        body = [body]
    if not isinstance(body, list):
        body = []
    return body

@app.route('/api/connectors/<cid>/test', methods=['POST'])
@token_required
def test_connector(cid):
    db = get_db()
    row = db.execute("SELECT * FROM api_connectors WHERE id=?", (cid,)).fetchone()
    if not row: return jsonify({'error': 'Introuvable'}), 404
    try:
        records = _call_api_connector(dict(row))
        db.execute("UPDATE api_connectors SET status='ok', updated_at=datetime('now') WHERE id=?", (cid,))
        db.commit()
        # Retourner un aperçu
        sample = records[:3] if records else []
        fields = list(set(k for r in sample for k in (r.keys() if isinstance(r, dict) else [])))
        return jsonify({
            'status': 'ok',
            'message': f'✅ Connexion réussie — {len(records)} enregistrement(s) reçu(s)',
            'total_records': len(records),
            'sample': sample,
            'available_fields': sorted(fields),
        })
    except Exception as e:
        db.execute("UPDATE api_connectors SET status='error', updated_at=datetime('now') WHERE id=?", (cid,))
        db.commit()
        return jsonify({'status': 'error', 'message': str(e)}), 400

@app.route('/api/connectors/<cid>/preview', methods=['POST'])
@token_required
def preview_connector(cid):
    row = get_db().execute("SELECT * FROM api_connectors WHERE id=?", (cid,)).fetchone()
    if not row: return jsonify({'error': 'Introuvable'}), 404
    try:
        records = _call_api_connector(dict(row))
        return jsonify({
            'total': len(records),
            'sample': records[:10],
            'fields': sorted(set(k for r in records[:50] for k in (r.keys() if isinstance(r, dict) else [])))
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/connectors/<cid>/sync', methods=['POST'])
@token_required
def sync_connector(cid):
    """Synchronisation manuelle — appelle l'API et importe les données"""
    db = get_db()
    row = db.execute("SELECT * FROM api_connectors WHERE id=?", (cid,)).fetchone()
    if not row: return jsonify({'error': 'Introuvable'}), 404
    connector = dict(row)
    field_mapping = connector.get('field_mapping', '{}')
    if isinstance(field_mapping, str):
        try: field_mapping = json.loads(field_mapping)
        except: field_mapping = {}
    target_type = connector.get('target_type', 'entity')

    try:
        records = _call_api_connector(connector)
        if not records:
            db.execute("UPDATE api_connectors SET last_sync=datetime('now'),last_sync_status='empty',last_sync_count=0 WHERE id=?", (cid,))
            db.commit()
            return jsonify({'imported': 0, 'total': 0, 'message': 'Aucun enregistrement reçu'})

        # Import log
        log_id = str(uuid.uuid4())
        source_label = f"[API] {connector['name']}"
        db.execute("INSERT INTO import_logs(id,filename,source_label,status) VALUES(?,?,?,'processing')",
                   (log_id, source_label, source_label))
        db.commit()

        imported = 0; errors = 0; total = len(records)
        BATCH = 1000; batch = []
        for rec in records:
            if not isinstance(rec, dict): errors += 1; continue
            # Appliquer le field_mapping
            mapped = {}
            if field_mapping:
                for target_field, source_field in field_mapping.items():
                    if source_field in rec:
                        mapped[target_field] = rec[source_field]
            else:
                mapped = rec  # Pas de mapping → tout prendre

            if target_type == 'vessel':
                # Import direct dans maritime_vessels
                try:
                    vid = str(uuid.uuid4())
                    mdm_id = f"MV-{uuid.uuid4().hex[:8].upper()}"
                    db.execute("""INSERT OR IGNORE INTO maritime_vessels(id,mdm_id,vessel_name,vessel_name_normalized,
                        imo_number,mmsi,vessel_type,flag_code,gross_tonnage,source,confidence_score)
                        VALUES(?,?,?,?,?,?,?,?,?,?,0.5)""",
                        (vid, mdm_id, mapped.get('vessel_name',''),
                         mapped.get('vessel_name','').upper().strip(),
                         mapped.get('imo_number',''), mapped.get('mmsi',''),
                         mapped.get('vessel_type',''), mapped.get('flag_code',''),
                         mapped.get('gross_tonnage'), source_label))
                    imported += 1
                except: errors += 1
            else:
                # Import dans entities (générique)
                batch.append((str(uuid.uuid4()), f"MDM-{uuid.uuid4().hex[:8].upper()}",
                              json.dumps(mapped, ensure_ascii=False), source_label))
                if len(batch) >= BATCH:
                    db.executemany("INSERT INTO entities(id,mdm_id,data,source) VALUES(?,?,?,?)", batch)
                    imported += len(batch); batch = []

        if batch:
            db.executemany("INSERT INTO entities(id,mdm_id,data,source) VALUES(?,?,?,?)", batch)
            imported += len(batch)

        db.execute("UPDATE import_logs SET total_rows=?,imported=?,errors=?,status='done' WHERE id=?",
                   (total, imported, errors, log_id))
        db.execute("""UPDATE api_connectors SET last_sync=datetime('now'),last_sync_status='ok',
                      last_sync_count=? WHERE id=?""", (imported, cid))
        db.commit()
        _cache.pop('dashboard_stats', None)
        audit('api_sync', 'connector', cid, {'imported': imported, 'errors': errors})
        return jsonify({'imported': imported, 'errors': errors, 'total': total, 'log_id': log_id})
    except Exception as e:
        db.execute("""UPDATE api_connectors SET last_sync=datetime('now'),
                      last_sync_status='error' WHERE id=?""", (cid,))
        db.commit()
        return jsonify({'error': str(e)}), 500

@app.route('/api/connectors/scheduled', methods=['GET'])
@token_required
def list_scheduled_connectors():
    """Liste les connecteurs avec synchro automatique activée"""
    rows = get_db().execute("""SELECT id,name,connector_type,sync_interval_minutes,last_sync,
                               last_sync_status,last_sync_count,enabled
                               FROM api_connectors WHERE sync_interval_minutes > 0 AND enabled=1
                               ORDER BY last_sync""").fetchall()
    return jsonify([dict(r) for r in rows])

# ── DUPLICATES ───────────────────────────────
def normalize(v): return str(v or '').strip().lower()

@app.route('/api/duplicates/detect', methods=['POST'])
@token_required
def detect_duplicates():
    b = request.json or {}
    fields = b.get('fields', [])
    method = b.get('method', 'exact')
    db = get_db()
    db.execute("DELETE FROM duplicates WHERE status='pending'"); db.commit()
    rows = db.execute("SELECT id,data FROM entities WHERE status='active'").fetchall()
    entities = []
    for r in rows:
        try: d = json.loads(r['data'])
        except: d = {}
        entities.append({'id': r['id'], 'data': d})
    pairs = []; found = 0
    if method == 'exact':
        from collections import defaultdict
        groups = defaultdict(list)
        for e in entities:
            parts = [normalize(e['data'].get(f,'')) for f in fields] if fields else \
                    [normalize(v) for v in e['data'].values() if isinstance(v,(str,int,float))]
            key = '|'.join(parts)
            if any(p.strip() for p in parts): groups[key].append(e['id'])
        for key, ids in groups.items():
            if len(ids) > 1:
                for i in range(len(ids)):
                    for j in range(i+1,len(ids)):
                        pairs.append((ids[i],ids[j],1.0,'exact'))
    elif method == 'fuzzy':
        try: from thefuzz import fuzz
        except: return jsonify({'error':'thefuzz non installé'}), 500
        threshold = int(b.get('threshold',80))
        # §2.2.1 — Blocking keys : pré-filtrer par premiers caractères pour réduire O(n²)
        from collections import defaultdict
        blocks = defaultdict(list)
        for e in entities:
            if fields:
                bk = normalize(e['data'].get(fields[0], ''))[:3]
            else:
                vals = [normalize(v) for v in e['data'].values() if isinstance(v, (str,)) and v]
                bk = vals[0][:3] if vals else ''
            if bk: blocks[bk].append(e)
        # Comparer uniquement au sein de chaque bloc
        for bk, group in blocks.items():
            for i in range(len(group)):
                for j in range(i+1, len(group)):
                    e1, e2 = group[i], group[j]
                    if fields:
                        scores = [fuzz.ratio(normalize(e1['data'].get(f,'')), normalize(e2['data'].get(f,'')))
                                  for f in fields if e1['data'].get(f) and e2['data'].get(f)]
                        score = sum(scores)/len(scores) if scores else 0
                    else:
                        s1=' '.join(normalize(v) for v in e1['data'].values() if isinstance(v,(str,int,float)))
                        s2=' '.join(normalize(v) for v in e2['data'].values() if isinstance(v,(str,int,float)))
                        score = fuzz.token_sort_ratio(s1,s2)
                    if score >= threshold: pairs.append((e1['id'],e2['id'],score/100.0,'fuzzy'))
    for e1id,e2id,score,meth in pairs:
        try:
            db.execute("INSERT INTO duplicates(id,entity1_id,entity2_id,score,status,method) VALUES(?,?,?,?,'pending',?)",
                       (str(uuid.uuid4()),e1id,e2id,score,meth)); found+=1
        except: pass
    db.commit()
    return jsonify({'found':found,'method':method})

@app.route('/api/duplicates', methods=['GET'])
@token_required
def list_duplicates():
    db = get_db()
    status = request.args.get('status','pending')
    # §3.3 — Single JOIN au lieu de N+1 requêtes
    rows = db.execute("""
        SELECT d.*,
               e1.id as e1_id, e1.mdm_id as e1_mdm_id, e1.data as e1_data, e1.source as e1_source,
               e2.id as e2_id, e2.mdm_id as e2_mdm_id, e2.data as e2_data, e2.source as e2_source
        FROM duplicates d
        LEFT JOIN entities e1 ON d.entity1_id = e1.id
        LEFT JOIN entities e2 ON d.entity2_id = e2.id
        WHERE d.status=?
        ORDER BY d.score DESC
    """, (status,)).fetchall()
    result = []
    for r in rows:
        d = {'id':r['id'],'entity1_id':r['entity1_id'],'entity2_id':r['entity2_id'],
             'score':r['score'],'status':r['status'],'method':r['method'],'created_at':r['created_at']}
        for prefix, alias in [('e1','entity1'),('e2','entity2')]:
            eid = r[f'{prefix}_id']
            if eid:
                try: data = json.loads(r[f'{prefix}_data'])
                except: data = {}
                d[alias] = {'id':eid,'mdm_id':r[f'{prefix}_mdm_id'],'data':data,'source':r[f'{prefix}_source']}
        result.append(d)
    return jsonify(result)

@app.route('/api/duplicates/ignore', methods=['POST'])
@token_required
def ignore_duplicate():
    dup_id=(request.json or {}).get('duplicate_id')
    if not dup_id: return jsonify({'error':'duplicate_id requis'}),400
    get_db().execute("UPDATE duplicates SET status='ignored' WHERE id=?",(dup_id,))
    get_db().commit()
    return jsonify({'message':'Ignoré'})

# ── FUSION RULES ─────────────────────────────
@app.route('/api/fusion-rules', methods=['GET'])
@token_required
def list_rules():
    rows=get_db().execute("SELECT * FROM fusion_rules ORDER BY created_at DESC").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/fusion-rules', methods=['POST'])
@token_required
def create_rule():
    b=request.json or {}
    rid=str(uuid.uuid4())
    get_db().execute("INSERT INTO fusion_rules(id,name,field,strategy,source_priority,condition_expr,active) VALUES(?,?,?,?,?,?,1)",
               (rid,b.get('name'),b.get('field'),b.get('strategy','most_complete'),
                json.dumps(b.get('source_priority',[])),b.get('condition_expr','')))
    get_db().commit()
    return jsonify({'id':rid}),201

@app.route('/api/fusion-rules/<rid>', methods=['PUT'])
@token_required
def update_rule(rid):
    b=request.json or {}
    get_db().execute("UPDATE fusion_rules SET name=?,field=?,strategy=?,source_priority=?,condition_expr=?,active=? WHERE id=?",
               (b.get('name'),b.get('field'),b.get('strategy'),
                json.dumps(b.get('source_priority',[])),b.get('condition_expr',''),int(b.get('active',1)),rid))
    get_db().commit()
    return jsonify({'message':'Mis à jour'})

@app.route('/api/fusion-rules/<rid>', methods=['DELETE'])
@token_required
def delete_rule(rid):
    get_db().execute("DELETE FROM fusion_rules WHERE id=?",(rid,))
    get_db().commit()
    return jsonify({'message':'Supprimé'})

@app.route('/api/fusion-rules/preview', methods=['POST'])
@token_required
def preview_rules():
    b=request.json or {}
    e1=b.get('entity1',{})
    e2=b.get('entity2',{})
    db=get_db()
    rules=db.execute("SELECT * FROM fusion_rules WHERE active=1 ORDER BY created_at").fetchall()
    all_keys=list(set(list(e1.keys())+list(e2.keys())))
    result={}; applied=[]
    for key in all_keys:
        v1=e1.get(key,''); v2=e2.get(key,'')
        chosen=v1; rule_used=None
        for rule in rules:
            r=dict(rule)
            if r['field']!=key and r['field']!='*': continue
            strat=r['strategy']
            if strat=='most_complete':
                chosen=v1 if len(str(v1 or ''))>=len(str(v2 or '')) else v2
            elif strat=='source_priority':
                try:
                    prio=json.loads(r['source_priority'] or '[]')
                    src1=e1.get('_source',''); src2=e2.get('_source','')
                    i1=prio.index(src1) if src1 in prio else 99
                    i2=prio.index(src2) if src2 in prio else 99
                    chosen=v1 if i1<=i2 else v2
                except: chosen=v1
            elif strat=='always_entity1': chosen=v1
            elif strat=='always_entity2': chosen=v2
            elif strat=='non_empty': chosen=v1 if v1 and str(v1).strip() else v2
            elif strat=='longest': chosen=v1 if len(str(v1 or ''))>=len(str(v2 or '')) else v2
            rule_used=r['name']; break
        result[key]=chosen
        applied.append({'field':key,'v1':v1,'v2':v2,'chosen':chosen,'rule':rule_used or 'défaut (entité 1)'})
    return jsonify({'merged':result,'applied':applied})

# ── GOLDEN RECORDS ────────────────────────────
@app.route('/api/golden-records/merge', methods=['POST'])
@token_required
def merge_entities():
    b=request.json or {}
    entity_ids=b.get('entity_ids',[])
    merged_data=b.get('merged_data',{})
    dup_id=b.get('duplicate_id')
    rules_used=b.get('rules_applied',[])
    if len(entity_ids)<2: return jsonify({'error':'Au moins 2 entités requises'}),400
    db=get_db()
    gid=str(uuid.uuid4()); mid='GR-'+gid[:8].upper()
    db.execute("INSERT INTO golden_records(id,mdm_id,data,source_ids,rules_applied) VALUES(?,?,?,?,?)",
               (gid,mid,json.dumps(merged_data,ensure_ascii=False),
                json.dumps(entity_ids),json.dumps(rules_used)))
    for eid in entity_ids:
        db.execute("UPDATE entities SET status='merged' WHERE id=?",(eid,))
    if dup_id:
        db.execute("UPDATE duplicates SET status='resolved' WHERE id=?",(dup_id,))
    db.commit()
    _cache.pop('dashboard_stats', None)
    _cache.pop('reporting_overview', None)
    audit('merge','golden_record',gid,{'sources':entity_ids})
    return jsonify({'golden_record_id':gid,'mdm_id':mid}),201

@app.route('/api/golden-records', methods=['GET'])
@token_required
def list_golden_records():
    rows=get_db().execute("SELECT * FROM golden_records ORDER BY created_at DESC").fetchall()
    result=[]
    for r in rows:
        gr=dict(r)
        for k in ('data','source_ids','rules_applied'):
            try: gr[k]=json.loads(gr[k])
            except: pass
        result.append(gr)
    return jsonify(result)

# ── REPORTING ────────────────────────────────
@app.route('/api/reporting/overview', methods=['GET'])
@token_required
def reporting_overview():
    # §6.3 — Cache reporting 30s
    c = cached('reporting_overview', ttl=30)
    if c: return jsonify(c)
    db=get_db()
    total_e   =db.execute("SELECT COUNT(*) FROM entities WHERE status='active'").fetchone()[0]
    total_mer =db.execute("SELECT COUNT(*) FROM entities WHERE status='merged'").fetchone()[0]
    total_del =db.execute("SELECT COUNT(*) FROM entities WHERE status='deleted'").fetchone()[0]
    total_dup =db.execute("SELECT COUNT(*) FROM duplicates WHERE status='pending'").fetchone()[0]
    total_res =db.execute("SELECT COUNT(*) FROM duplicates WHERE status='resolved'").fetchone()[0]
    total_ign =db.execute("SELECT COUNT(*) FROM duplicates WHERE status='ignored'").fetchone()[0]
    total_gr  =db.execute("SELECT COUNT(*) FROM golden_records").fetchone()[0]
    total_i   =db.execute("SELECT COUNT(*) FROM import_logs WHERE status='done'").fetchone()[0]
    total_rows=db.execute("SELECT COALESCE(SUM(imported),0) FROM import_logs WHERE status='done'").fetchone()[0]
    dup_rate  =round(total_dup/max(total_e,1)*100,1)
    coverage  =round(total_gr/max(total_e+total_mer,1)*100,1)
    by_source =db.execute("SELECT source,COUNT(*) as count FROM entities WHERE status IN ('active','merged') GROUP BY source ORDER BY count DESC").fetchall()
    import_trend=db.execute("SELECT DATE(created_at) as day,COUNT(*) as imports,COALESCE(SUM(imported),0) as rows FROM import_logs WHERE status='done' AND created_at>=date('now','-30 days') GROUP BY day ORDER BY day").fetchall()
    dup_trend   =db.execute("SELECT DATE(created_at) as day,COUNT(*) as found FROM duplicates WHERE created_at>=date('now','-30 days') GROUP BY day ORDER BY day").fetchall()
    gr_trend    =db.execute("SELECT DATE(created_at) as day,COUNT(*) as created FROM golden_records WHERE created_at>=date('now','-30 days') GROUP BY day ORDER BY day").fetchall()
    top_sources =db.execute("SELECT source_label,SUM(imported) as total_rows FROM import_logs WHERE status='done' GROUP BY source_label ORDER BY total_rows DESC LIMIT 10").fetchall()
    status_dist =db.execute("SELECT status,COUNT(*) as count FROM entities GROUP BY status").fetchall()
    data = {
        'kpis':{'total_entities':total_e,'total_merged':total_mer,'total_deleted':total_del,
                'total_duplicates_pending':total_dup,'total_duplicates_resolved':total_res,
                'total_duplicates_ignored':total_ign,'total_golden_records':total_gr,
                'total_imports':total_i,'total_rows_imported':total_rows,
                'duplicate_rate':dup_rate,'golden_coverage':coverage},
        'by_source':[dict(r) for r in by_source],
        'import_trend':[dict(r) for r in import_trend],
        'dup_trend':[dict(r) for r in dup_trend],
        'gr_trend':[dict(r) for r in gr_trend],
        'top_sources':[dict(r) for r in top_sources],
        'status_dist':[dict(r) for r in status_dist],
    }
    set_cache('reporting_overview', data)
    return jsonify(data)

@app.route('/api/reporting/pivot', methods=['POST'])
@token_required
def pivot_table():
    b=request.json or {}
    row_f=b.get('row_field'); col_f=b.get('col_field')
    agg=b.get('aggregation','count'); val_f=b.get('value_field')
    src=b.get('source','')
    if not row_f: return jsonify({'error':'row_field requis'}),400
    db=get_db()
    wh="status IN ('active','merged')"; params=[]
    if src: wh+=" AND source LIKE ?"; params.append(f'%{src}%')
    rows=db.execute(f"SELECT data,source FROM entities WHERE {wh}",params).fetchall()
    data=[]
    for r in rows:
        try: d=json.loads(r['data']); d['_source']=r['source']
        except: d={}
        data.append(d)
    if not data: return jsonify({'pivot':[],'columns':[],'rows':[]})
    df=pd.DataFrame(data)
    if row_f not in df.columns: return jsonify({'error':f"Champ '{row_f}' introuvable"}),400
    try:
        if col_f and col_f in df.columns:
            if agg=='count':
                piv=pd.crosstab(df[row_f],df[col_f])
            else:
                if not val_f or val_f not in df.columns: return jsonify({'error':f"value_field requis"}),400
                df[val_f]=pd.to_numeric(df[val_f],errors='coerce')
                piv=df.pivot_table(index=row_f,columns=col_f,values=val_f,aggfunc=agg,fill_value=0)
            piv=piv.reset_index()
            return jsonify({'pivot':piv.fillna(0).to_dict(orient='records'),'columns':list(piv.columns),'row_field':row_f,'col_field':col_f})
        else:
            if agg=='count':
                series=df[row_f].value_counts().reset_index(); series.columns=[row_f,'count']
            else:
                if not val_f or val_f not in df.columns: return jsonify({'error':'value_field requis'}),400
                df[val_f]=pd.to_numeric(df[val_f],errors='coerce')
                series=df.groupby(row_f)[val_f].agg(agg).reset_index(); series.columns=[row_f,agg]
            return jsonify({'pivot':series.fillna(0).to_dict(orient='records'),'columns':list(series.columns),'row_field':row_f})
    except Exception as e: return jsonify({'error':str(e)}),500

@app.route('/api/reporting/fields', methods=['GET'])
@token_required
def get_all_fields():
    rows=get_db().execute("SELECT data FROM entities WHERE status IN ('active','merged') LIMIT 300").fetchall()
    fields=set()
    for r in rows:
        try: fields.update(json.loads(r['data']).keys())
        except: pass
    return jsonify({'fields':sorted(fields)})

# ── EXPORT ───────────────────────────────────
@app.route('/api/export/csv', methods=['GET'])
@token_required
def export_csv():
    db=get_db(); params=[]
    include_merged=request.args.get('include_merged','false')=='true'
    source_filter=request.args.get('source','')
    q="SELECT * FROM entities WHERE status='active'"
    if include_merged: q="SELECT * FROM entities WHERE status IN ('active','merged')"
    if source_filter: q+=" AND source LIKE ?"; params.append(f'%{source_filter}%')
    rows=db.execute(q+' ORDER BY created_at DESC',params).fetchall()
    if not rows: return jsonify({'error':'Aucune entité'}),404
    all_data=[]; all_keys=set()
    for r in rows:
        e=dict(r)
        try: data=json.loads(e['data'])
        except: data={}
        flat={'_id':e['id'],'_mdm_id':e['mdm_id'],'_status':e['status'],'_source':e['source'],'_created_at':e['created_at']}
        flat.update(data); all_keys.update(data.keys()); all_data.append(flat)
    output=StringIO()
    fn=['_id','_mdm_id','_status','_source','_created_at']+sorted(all_keys)
    w=csv.DictWriter(output,fieldnames=fn,extrasaction='ignore')
    w.writeheader(); w.writerows(all_data)
    return Response(output.getvalue(),mimetype='text/csv',
                    headers={'Content-Disposition':f'attachment;filename=osmdm_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'})

@app.route('/api/export/golden-records/csv', methods=['GET'])
@token_required
def export_golden_csv():
    rows=get_db().execute("SELECT * FROM golden_records ORDER BY created_at DESC").fetchall()
    all_data=[]; all_keys=set()
    for r in rows:
        gr=dict(r)
        try: data=json.loads(gr['data'])
        except: data={}
        flat={'_id':gr['id'],'_mdm_id':gr['mdm_id'],'_created_at':gr['created_at']}
        flat.update(data); all_keys.update(data.keys()); all_data.append(flat)
    output=StringIO()
    fn=['_id','_mdm_id','_created_at']+sorted(all_keys)
    w=csv.DictWriter(output,fieldnames=fn,extrasaction='ignore')
    w.writeheader(); w.writerows(all_data)
    return Response(output.getvalue(),mimetype='text/csv',
                    headers={'Content-Disposition':f'attachment;filename=golden_records_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'})

# ── AUDIT ────────────────────────────────────
@app.route('/api/audit', methods=['GET'])
@token_required
def get_audit():
    logs=get_db().execute("SELECT * FROM audit_log ORDER BY created_at DESC LIMIT 100").fetchall()
    return jsonify([dict(l) for l in logs])

# ── USERS ────────────────────────────────────

# ── ADMIN USERS ──────────────────────────────────────────────────────────
def admin_required(f):
    @wraps(f)
    def dec(*a, **kw):
        token = request.headers.get('Authorization','').replace('Bearer ','')
        if not token: return jsonify({'error':'Token manquant'}), 401
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
            if payload.get('role') != 'admin':
                return jsonify({'error':'Accès réservé aux admins'}), 403
            g.current_user = payload
        except: return jsonify({'error':'Token invalide'}), 401
        return f(*a, **kw)
    return dec

@app.route('/api/users', methods=['GET'])
@token_required
def list_users():
    rows = get_db().execute("SELECT id,email,name,role,avatar,provider,active,created_at,last_login FROM users ORDER BY created_at DESC").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/users', methods=['POST'])
@admin_required
def create_user():
    b = request.json or {}
    if not b.get('email'): return jsonify({'error':'Email requis'}), 400
    uid = str(uuid.uuid4())
    pwd = hash_pw(b.get('password','changeme123')) if b.get('password') else hash_pw('changeme123')
    try:
        get_db().execute(
            "INSERT INTO users(id,email,password,name,role,provider,active) VALUES(?,?,?,?,?,'local',1)",
            (uid, b['email'].strip().lower(), pwd, b.get('name',''), b.get('role','viewer'))
        )
        get_db().commit()
        return jsonify({'id': uid, 'message': 'Utilisateur créé'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/users/<uid>', methods=['PUT'])
@admin_required
def update_user(uid):
    b = request.json or {}
    db = get_db()
    updates = []
    params = []
    if 'name'   in b: updates.append('name=?');   params.append(b['name'])
    if 'role'   in b: updates.append('role=?');   params.append(b['role'])
    if 'active' in b: updates.append('active=?'); params.append(1 if b['active'] else 0)
    if 'password' in b and b['password']:
        updates.append('password=?'); params.append(hash_pw(b['password']))
    if not updates: return jsonify({'error':'Rien à modifier'}), 400
    params.append(uid)
    db.execute(f"UPDATE users SET {','.join(updates)} WHERE id=?", params)
    db.commit()
    return jsonify({'message': 'Mis à jour'})

@app.route('/api/users/<uid>', methods=['DELETE'])
@admin_required
def delete_user(uid):
    if g.current_user.get('user_id') == uid:
        return jsonify({'error':'Impossible de supprimer son propre compte'}), 400
    get_db().execute("DELETE FROM users WHERE id=?", (uid,))
    get_db().commit()
    return jsonify({'message': 'Supprimé'})

@app.route('/api/auth/change-password', methods=['POST'])
@token_required
def change_password():
    b = request.json or {}
    old_pwd = b.get('old_password','')
    new_pwd = b.get('new_password','')
    if not new_pwd or len(new_pwd) < 8:
        return jsonify({'error':'Nouveau mot de passe trop court (8 min)'}), 400
    db = get_db()
    u = db.execute("SELECT * FROM users WHERE id=?", (g.current_user['user_id'],)).fetchone()
    if not u: return jsonify({'error':'Utilisateur introuvable'}), 404
    if u['provider'] != 'local':
        return jsonify({'error':'Compte SSO — mot de passe géré par Google/Microsoft'}), 400
    if not check_pw(old_pwd, u['password']):
        return jsonify({'error':'Ancien mot de passe incorrect'}), 401
    db.execute("UPDATE users SET password=? WHERE id=?", (hash_pw(new_pwd), u['id']))
    db.commit()
    return jsonify({'message': 'Mot de passe mis à jour'})

@app.route('/api/auth/profile', methods=['PUT'])
@token_required
def update_profile():
    b = request.json or {}
    db = get_db()
    if 'name' in b:
        db.execute("UPDATE users SET name=? WHERE id=?", (b['name'], g.current_user['user_id']))
        db.commit()
    u = db.execute("SELECT id,email,name,role,avatar,provider FROM users WHERE id=?", (g.current_user['user_id'],)).fetchone()
    return jsonify(dict(u))

# ── SSO GOOGLE ───────────────────────────────────────────────────────────
@app.route('/api/auth/google/callback', methods=['POST'])
def google_callback():
    import urllib.request as ur, json as _j
    token = (request.json or {}).get('credential','')
    if not token: return jsonify({'error':'Token Google manquant'}), 400
    try:
        # Vérifier le token Google
        req = ur.Request(f'https://oauth2.googleapis.com/tokeninfo?id_token={token}')
        with ur.urlopen(req, timeout=10) as r:
            info = _j.loads(r.read())
        email = info.get('email','').lower()
        name  = info.get('name','')
        avatar= info.get('picture','')
        gid   = info.get('sub','')
        if not email: return jsonify({'error':'Email Google invalide'}), 400
        db = get_db()
        u = db.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
        if not u:
            # Créer l'utilisateur SSO
            uid = str(uuid.uuid4())
            db.execute(
                "INSERT INTO users(id,email,password,name,role,avatar,provider,provider_id,active) VALUES(?,?,'',?,?,?,'google',?,1)",
                (uid, email, name, 'viewer', avatar, gid)
            )
            db.commit()
            u = db.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
        elif not u['active']:
            return jsonify({'error':'Compte désactivé'}), 403
        # Mettre à jour avatar et last_login
        db.execute("UPDATE users SET avatar=?,last_login=datetime('now'),provider_id=? WHERE id=?", (avatar,gid,u['id']))
        db.commit()
        mdm_token = jwt.encode({
            'user_id':u['id'],'email':u['email'],'role':u['role'],
            'exp': datetime.now(timezone.utc)+timedelta(hours=12)
        }, SECRET_KEY, algorithm='HS256')
        return jsonify({'token':mdm_token,'user':{'id':u['id'],'email':u['email'],'name':u['name'],'role':u['role'],'avatar':avatar}})
    except Exception as e:
        return jsonify({'error':f'Erreur Google SSO: {str(e)}'}), 500

# ── SSO MICROSOFT ────────────────────────────────────────────────────────
@app.route('/api/auth/microsoft/callback', methods=['POST'])
def microsoft_callback():
    import urllib.request as ur, json as _j
    b = request.json or {}
    access_token = b.get('access_token','')
    if not access_token: return jsonify({'error':'Token Microsoft manquant'}), 400
    try:
        req = ur.Request('https://graph.microsoft.com/v1.0/me',
            headers={'Authorization': f'Bearer {access_token}','Content-Type':'application/json'})
        with ur.urlopen(req, timeout=10) as r:
            info = _j.loads(r.read())
        email  = (info.get('mail') or info.get('userPrincipalName','')).lower()
        name   = info.get('displayName','')
        mid    = info.get('id','')
        if not email: return jsonify({'error':'Email Microsoft invalide'}), 400
        db = get_db()
        u = db.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
        if not u:
            uid = str(uuid.uuid4())
            db.execute(
                "INSERT INTO users(id,email,password,name,role,provider,provider_id,active) VALUES(?,?,'',?,?,'microsoft',?,1)",
                (uid, email, name, 'viewer', mid)
            )
            db.commit()
            u = db.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
        elif not u['active']:
            return jsonify({'error':'Compte désactivé'}), 403
        db.execute("UPDATE users SET last_login=datetime('now'),provider_id=? WHERE id=?", (mid, u['id']))
        db.commit()
        mdm_token = jwt.encode({
            'user_id':u['id'],'email':u['email'],'role':u['role'],
            'exp': datetime.now(timezone.utc)+timedelta(hours=12)
        }, SECRET_KEY, algorithm='HS256')
        return jsonify({'token':mdm_token,'user':{'id':u['id'],'email':u['email'],'name':u['name'],'role':u['role']}})
    except Exception as e:
        return jsonify({'error':f'Erreur Microsoft SSO: {str(e)}'}), 500



# ── WRITE-BACK — Repousser les GR vers les sources ─────────
@app.route('/api/writeback/configs', methods=['GET'])
@token_required
def list_writeback_configs():
    db = get_db()
    rows = db.execute("""SELECT w.*, 
        CASE WHEN w.target_type='db' THEN dc.name ELSE ac.name END as target_name
        FROM writeback_configs w
        LEFT JOIN db_connections dc ON w.connection_id = dc.id
        LEFT JOIN api_connectors ac ON w.connector_id = ac.id
        ORDER BY w.created_at DESC""").fetchall()
    result = []
    for r in rows:
        d = dict(r)
        try: d['field_mapping'] = json.loads(d['field_mapping'])
        except: pass
        result.append(d)
    return jsonify(result)

@app.route('/api/writeback/configs', methods=['POST'])
@token_required
def create_writeback_config():
    b = request.json or {}
    wid = str(uuid.uuid4())
    db = get_db()
    db.execute("""INSERT INTO writeback_configs(id,name,target_type,connection_id,connector_id,
                  target_table,api_endpoint,api_method,field_mapping,mode,match_key,enabled)
                  VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        (wid, b.get('name',''), b.get('target_type','db'),
         b.get('connection_id'), b.get('connector_id'),
         b.get('target_table',''), b.get('api_endpoint',''),
         b.get('api_method','POST'), json.dumps(b.get('field_mapping',{})),
         b.get('mode','upsert'), b.get('match_key',''), 1))
    db.commit()
    audit('create', 'writeback_config', wid, {'name': b.get('name')})
    return jsonify({'id': wid, 'message': 'Configuration write-back créée'}), 201

@app.route('/api/writeback/configs/<wid>', methods=['PUT'])
@token_required
def update_writeback_config(wid):
    b = request.json or {}
    db = get_db()
    db.execute("""UPDATE writeback_configs SET name=?,target_type=?,connection_id=?,connector_id=?,
                  target_table=?,api_endpoint=?,api_method=?,field_mapping=?,mode=?,match_key=?,enabled=? WHERE id=?""",
        (b.get('name',''), b.get('target_type','db'),
         b.get('connection_id'), b.get('connector_id'),
         b.get('target_table',''), b.get('api_endpoint',''),
         b.get('api_method','POST'), json.dumps(b.get('field_mapping',{})),
         b.get('mode','upsert'), b.get('match_key',''), 1 if b.get('enabled',True) else 0, wid))
    db.commit()
    return jsonify({'message': 'Mis à jour'})

@app.route('/api/writeback/configs/<wid>', methods=['DELETE'])
@token_required
def delete_writeback_config(wid):
    get_db().execute("DELETE FROM writeback_configs WHERE id=?", (wid,))
    get_db().commit()
    return jsonify({'message': 'Supprimé'})

def _writeback_to_db(config, records, dry_run=False):
    """Pousser des records vers une connexion DB (SQL Server, PostgreSQL, MySQL…)"""
    db = get_db()
    conn_row = db.execute("SELECT * FROM db_connections WHERE id=?", (config['connection_id'],)).fetchone()
    if not conn_row:
        return {'error': 'Connexion DB introuvable'}
    conn = _make_conn(dict(conn_row))
    cur = conn.cursor()
    table = config.get('target_table', '')
    if not table:
        conn.close()
        return {'error': 'Table cible non définie'}
    mapping = config.get('field_mapping', {})
    if isinstance(mapping, str):
        try: mapping = json.loads(mapping)
        except: mapping = {}
    mode = config.get('mode', 'insert')
    match_key = config.get('match_key', '')
    db_type = conn_row['db_type'].lower()
    success = 0; errors = 0; error_msgs = []
    # Dry-run : on trace les actions sans les exécuter
    preview = []  # Liste des actions simulées
    for rec in records:
        mapped = {}
        if mapping:
            for db_col, mdm_field in mapping.items():
                mapped[db_col] = rec.get(mdm_field, '')
        else:
            mapped = {k: v for k, v in rec.items() if not k.startswith('_gr_')}
        cols = list(mapped.keys())
        vals = list(mapped.values())
        try:
            if mode == 'upsert' and match_key and match_key in mapped:
                check_sql = f'SELECT COUNT(*) FROM "{table}" WHERE "{match_key}" = ?'
                if db_type in ('mysql','mariadb'):
                    check_sql = f"SELECT COUNT(*) FROM `{table}` WHERE `{match_key}` = %s"
                elif db_type == 'mssql':
                    check_sql = f"SELECT COUNT(*) FROM [{table}] WHERE [{match_key}] = ?"
                cur.execute(check_sql, (mapped[match_key],))
                exists = cur.fetchone()[0] > 0
                action = 'UPDATE' if exists else 'INSERT'
                if dry_run:
                    preview.append({'action': action, 'match_value': mapped[match_key], 'data': mapped})
                    success += 1
                else:
                    if exists:
                        set_clause = ', '.join(
                            f'`{c}` = %s' if db_type in ('mysql','mariadb')
                            else f'[{c}] = ?' if db_type == 'mssql'
                            else f'"{c}" = ?' for c in cols if c != match_key)
                        where_clause = (f'`{match_key}` = %s' if db_type in ('mysql','mariadb')
                                        else f'[{match_key}] = ?' if db_type == 'mssql'
                                        else f'"{match_key}" = ?')
                        update_vals = [v for c, v in zip(cols, vals) if c != match_key] + [mapped[match_key]]
                        tbl = f'`{table}`' if db_type in ('mysql','mariadb') else f'[{table}]' if db_type == 'mssql' else f'"{table}"'
                        cur.execute(f"UPDATE {tbl} SET {set_clause} WHERE {where_clause}", update_vals)
                    else:
                        placeholders = ', '.join(['%s'] * len(cols)) if db_type in ('mysql','mariadb') else ', '.join(['?'] * len(cols))
                        col_list = ', '.join(f'`{c}`' if db_type in ('mysql','mariadb') else f'[{c}]' if db_type == 'mssql' else f'"{c}"' for c in cols)
                        tbl = f'`{table}`' if db_type in ('mysql','mariadb') else f'[{table}]' if db_type == 'mssql' else f'"{table}"'
                        cur.execute(f"INSERT INTO {tbl} ({col_list}) VALUES ({placeholders})", vals)
                    success += 1
            elif mode == 'update' and match_key and match_key in mapped:
                check_sql = f'SELECT COUNT(*) FROM "{table}" WHERE "{match_key}" = ?'
                if db_type in ('mysql','mariadb'):
                    check_sql = f"SELECT COUNT(*) FROM `{table}` WHERE `{match_key}` = %s"
                elif db_type == 'mssql':
                    check_sql = f"SELECT COUNT(*) FROM [{table}] WHERE [{match_key}] = ?"
                cur.execute(check_sql, (mapped[match_key],))
                exists = cur.fetchone()[0] > 0
                if not exists:
                    if dry_run:
                        preview.append({'action': 'SKIP', 'match_value': mapped[match_key], 'reason': 'non trouvé en base'})
                    continue
                if dry_run:
                    preview.append({'action': 'UPDATE', 'match_value': mapped[match_key], 'data': mapped})
                    success += 1
                else:
                    set_clause = ', '.join(
                        f'`{c}` = %s' if db_type in ('mysql','mariadb')
                        else f'[{c}] = ?' if db_type == 'mssql'
                        else f'"{c}" = ?' for c in cols if c != match_key)
                    where_clause = (f'`{match_key}` = %s' if db_type in ('mysql','mariadb')
                                    else f'[{match_key}] = ?' if db_type == 'mssql'
                                    else f'"{match_key}" = ?')
                    update_vals = [v for c, v in zip(cols, vals) if c != match_key] + [mapped[match_key]]
                    tbl = f'`{table}`' if db_type in ('mysql','mariadb') else f'[{table}]' if db_type == 'mssql' else f'"{table}"'
                    cur.execute(f"UPDATE {tbl} SET {set_clause} WHERE {where_clause}", update_vals)
                    success += 1
            else:
                if dry_run:
                    preview.append({'action': 'INSERT', 'data': mapped})
                    success += 1
                else:
                    placeholders = ', '.join(['%s'] * len(cols)) if db_type in ('mysql','mariadb') else ', '.join(['?'] * len(cols))
                    col_list = ', '.join(f'`{c}`' if db_type in ('mysql','mariadb') else f'[{c}]' if db_type == 'mssql' else f'"{c}"' for c in cols)
                    tbl = f'`{table}`' if db_type in ('mysql','mariadb') else f'[{table}]' if db_type == 'mssql' else f'"{table}"'
                    cur.execute(f"INSERT INTO {tbl} ({col_list}) VALUES ({placeholders})", vals)
                    success += 1
        except Exception as e:
            errors += 1
            error_msgs.append(str(e)[:200])
            if dry_run:
                preview.append({'action': 'ERROR', 'error': str(e)[:200], 'data': mapped})
    if not dry_run:
        conn.commit()
    conn.close()
    result = {'success': success, 'errors': errors, 'error_details': error_msgs[:5]}
    if dry_run:
        result['preview'] = preview
    return result

def _writeback_to_api(config, records, dry_run=False):
    """Pousser des records vers un connecteur API (POST/PUT)"""
    db = get_db()
    conn_row = db.execute("SELECT * FROM api_connectors WHERE id=?", (config.get('connector_id'),)).fetchone()
    if not conn_row:
        return {'error': 'Connecteur API introuvable'}
    connector = dict(conn_row)
    auth_type = connector.get('auth_type', 'none')
    auth_cfg = connector.get('auth_config', '{}')
    if isinstance(auth_cfg, str):
        try: auth_cfg = json.loads(auth_cfg)
        except: auth_cfg = {}
    hdrs = connector.get('headers', '{}')
    if isinstance(hdrs, str):
        try: hdrs = json.loads(hdrs)
        except: hdrs = {}
    if auth_type == 'bearer':
        hdrs['Authorization'] = f"Bearer {auth_cfg.get('token','')}"
    elif auth_type == 'api_key':
        hdrs[auth_cfg.get('header_name','X-API-Key')] = auth_cfg.get('apikey','')
    elif auth_type == 'basic':
        import base64
        creds = base64.b64encode(f"{auth_cfg.get('username','')}:{auth_cfg.get('password','')}".encode()).decode()
        hdrs['Authorization'] = f'Basic {creds}'
    hdrs['Content-Type'] = 'application/json'
    hdrs['User-Agent'] = 'OS-MDM-V2/1.0'
    endpoint = config.get('api_endpoint') or connector.get('base_url', '')
    method = config.get('api_method', 'POST').upper()
    mapping = config.get('field_mapping', {})
    if isinstance(mapping, str):
        try: mapping = json.loads(mapping)
        except: mapping = {}
    success = 0; errors = 0; error_msgs = []; preview = []
    for rec in records:
        mapped = {}
        if mapping:
            for api_field, mdm_field in mapping.items():
                mapped[api_field] = rec.get(mdm_field, '')
        else:
            mapped = {k: v for k, v in rec.items() if not k.startswith('_gr_')}
        if dry_run:
            preview.append({'action': method, 'endpoint': endpoint, 'data': mapped})
            success += 1
            continue
        try:
            url = endpoint
            if auth_type == 'query_param':
                sep = '&' if '?' in url else '?'
                url = f"{url}{sep}{auth_cfg.get('param_name','apikey')}={auth_cfg.get('apikey','')}"
            payload = json.dumps(mapped, ensure_ascii=False).encode('utf-8')
            req = urllib.request.Request(url, data=payload, headers=hdrs, method=method)
            resp = urllib.request.urlopen(req, timeout=30)
            if resp.status < 400: success += 1
            else: errors += 1; error_msgs.append(f"HTTP {resp.status}")
        except Exception as e:
            errors += 1; error_msgs.append(str(e)[:200])
    result = {'success': success, 'errors': errors, 'error_details': error_msgs[:5]}
    if dry_run: result['preview'] = preview
    return result

@app.route('/api/writeback/push', methods=['POST'])
@token_required
def writeback_push():
    """Pousser un ou plusieurs Golden Records vers une destination configurée"""
    b = request.json or {}
    config_id = b.get('config_id')
    gr_ids = b.get('golden_record_ids', [])
    push_all = b.get('push_all', False)
    dry_run = b.get('dry_run', False)
    db = get_db()
    cfg = db.execute("SELECT * FROM writeback_configs WHERE id=?", (config_id,)).fetchone()
    if not cfg: return jsonify({'error': 'Configuration write-back introuvable'}), 404
    config = dict(cfg)
    try: config['field_mapping'] = json.loads(config['field_mapping'])
    except: pass
    if push_all:
        grs = db.execute("SELECT * FROM golden_records").fetchall()
    elif gr_ids:
        placeholders = ','.join('?' * len(gr_ids))
        grs = db.execute(f"SELECT * FROM golden_records WHERE id IN ({placeholders})", gr_ids).fetchall()
    else:
        return jsonify({'error': 'Fournir golden_record_ids ou push_all=true'}), 400
    records = []
    for gr in grs:
        data = gr['data']
        try: data = json.loads(data) if isinstance(data, str) else data
        except: data = {}
        if isinstance(data, dict):
            data['_gr_id'] = gr['id']
            data['_gr_mdm_id'] = gr['mdm_id']
            records.append(data)
    if not records:
        return jsonify({'error': 'Aucun Golden Record à pousser'}), 400
    # Dry-run : simuler sans écrire
    if dry_run:
        try:
            if config['target_type'] == 'db':
                result = _writeback_to_db(config, records, dry_run=True)
            elif config['target_type'] == 'api':
                result = _writeback_to_api(config, records, dry_run=True)
            else:
                return jsonify({'error': f"Type cible inconnu: {config['target_type']}"}), 400
            if 'error' in result:
                return jsonify({'error': result['error']}), 400
            # Résumé dry-run
            preview = result.get('preview', [])
            summary = {}
            for p in preview:
                a = p.get('action', '?')
                summary[a] = summary.get(a, 0) + 1
            return jsonify({
                'dry_run': True,
                'message': f"🔍 Simulation : {len(records)} GR analysé(s) — aucune modification effectuée",
                'total': len(records),
                'summary': summary,
                'preview': preview[:20],  # Max 20 pour pas surcharger
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    # Exécution réelle
    log_id = str(uuid.uuid4())
    db.execute("INSERT INTO writeback_logs(id,config_id,golden_record_id,status,records_sent) VALUES(?,?,?,?,?)",
               (log_id, config_id, json.dumps(gr_ids or ['all']), 'processing', len(records)))
    db.commit()
    try:
        if config['target_type'] == 'db':
            result = _writeback_to_db(config, records)
        elif config['target_type'] == 'api':
            result = _writeback_to_api(config, records)
        else:
            return jsonify({'error': f"Type cible inconnu: {config['target_type']}"}), 400
        if 'error' in result:
            db.execute("UPDATE writeback_logs SET status='error',error_details=? WHERE id=?",
                       (result['error'], log_id))
            db.execute("UPDATE writeback_configs SET last_run=datetime('now'),last_run_status='error' WHERE id=?", (config_id,))
            db.commit()
            return jsonify({'error': result['error']}), 500
        db.execute("UPDATE writeback_logs SET status='done',records_success=?,records_error=?,error_details=? WHERE id=?",
                   (result['success'], result['errors'], json.dumps(result.get('error_details',[])), log_id))
        db.execute("UPDATE writeback_configs SET last_run=datetime('now'),last_run_status='ok',last_run_count=? WHERE id=?",
                   (result['success'], config_id))
        db.commit()
        audit('writeback', 'golden_records', config_id, {'sent': len(records), 'success': result['success'], 'errors': result['errors']})
        return jsonify({
            'message': f"✅ {result['success']} enregistrement(s) poussé(s), {result['errors']} erreur(s)",
            'success': result['success'],
            'errors': result['errors'],
            'error_details': result.get('error_details', []),
            'log_id': log_id,
        })
    except Exception as e:
        db.execute("UPDATE writeback_logs SET status='error',error_details=? WHERE id=?", (str(e), log_id))
        db.execute("UPDATE writeback_configs SET last_run=datetime('now'),last_run_status='error' WHERE id=?", (config_id,))
        db.commit()
        return jsonify({'error': str(e)}), 500

@app.route('/api/writeback/logs', methods=['GET'])
@token_required
def list_writeback_logs():
    rows = get_db().execute("""SELECT wl.*, wc.name as config_name 
        FROM writeback_logs wl
        LEFT JOIN writeback_configs wc ON wl.config_id = wc.id
        ORDER BY wl.created_at DESC LIMIT 50""").fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/writeback/targets', methods=['GET'])
@token_required
def list_writeback_targets():
    """Liste les cibles possibles (connexions DB + connecteurs API) pour le write-back"""
    db = get_db()
    dbs = db.execute("SELECT id,name,db_type,host,database_name,status FROM db_connections WHERE status='ok' ORDER BY name").fetchall()
    apis = db.execute("SELECT id,name,connector_type,base_url,status FROM api_connectors WHERE status='ok' AND enabled=1 ORDER BY name").fetchall()
    return jsonify({
        'databases': [dict(r) for r in dbs],
        'api_connectors': [dict(r) for r in apis],
    })


if __name__=='__main__':
    init_db()
    print("\n✅  O.S MDM V2 — Backend démarré !")
    print("📋  Admin : admin@osmdm.local / admin123")
    print("🌐  API   : http://localhost:5001/api\n")
    app.run(host='127.0.0.1', debug=True, port=5001, use_reloader=False)

# ── §4.5 VUE 360° ENTITÉ GÉNÉRIQUE ─────────────────────
@app.route('/api/entities/<eid>/360', methods=['GET'])
@token_required
def entity_360(eid):
    """Fiche complète 360° d'une entité générique"""
    db = get_db()
    r = db.execute("SELECT * FROM entities WHERE id=?", (eid,)).fetchone()
    if not r: return jsonify({'error': 'Introuvable'}), 404
    entity = dict(r)
    try: entity['data'] = json.loads(entity['data'])
    except: pass
    # Doublons liés
    dups = db.execute("""
        SELECT d.*, e.mdm_id as other_mdm_id, e.data as other_data, e.source as other_source
        FROM duplicates d
        LEFT JOIN entities e ON (CASE WHEN d.entity1_id=? THEN d.entity2_id ELSE d.entity1_id END) = e.id
        WHERE (d.entity1_id=? OR d.entity2_id=?) AND d.status IN ('pending','resolved','different')
        ORDER BY d.score DESC
    """, (eid, eid, eid)).fetchall()
    entity['duplicates'] = []
    for d in dups:
        dd = dict(d)
        try: dd['other_data'] = json.loads(dd['other_data'])
        except: pass
        entity['duplicates'].append(dd)
    # Golden Records contenant cette entité
    grs = db.execute("SELECT * FROM golden_records WHERE source_ids LIKE ?", (f'%{eid}%',)).fetchall()
    entity['golden_records'] = []
    for gr in grs:
        grd = dict(gr)
        for k in ('data', 'source_ids', 'rules_applied'):
            try: grd[k] = json.loads(grd[k])
            except: pass
        entity['golden_records'].append(grd)
    # Audit trail
    logs = db.execute("SELECT * FROM audit_log WHERE entity_id=? ORDER BY created_at DESC LIMIT 20", (eid,)).fetchall()
    entity['audit_trail'] = [dict(l) for l in logs]
    # Commentaires
    comments = db.execute("SELECT * FROM entity_comments WHERE entity_id=? ORDER BY created_at DESC", (eid,)).fetchall()
    entity['comments'] = [dict(c) for c in comments]
    return jsonify(entity)

# ── §6.2 ASYNC IMPORT (thread pool) ─────────────────────
import threading
_jobs = {}

@app.route('/api/import/async', methods=['POST'])
@token_required
def import_async():
    """Lance un import CSV en tâche de fond, retourne un job_id pour polling"""
    if 'file' not in request.files:
        return jsonify({'error': 'Aucun fichier'}), 400
    file = request.files['file']
    source_label = request.form.get('source_label', file.filename)
    ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else ''
    if ext not in ('csv', 'xlsx', 'xls'):
        return jsonify({'error': 'Format non supporté'}), 400
    # Lire le fichier en mémoire
    content = file.read()
    filename = file.filename
    job_id = str(uuid.uuid4())
    _jobs[job_id] = {'status': 'processing', 'progress': 0, 'total': 0, 'imported': 0, 'errors': 0}

    def run_import():
        try:
            if ext == 'csv':
                df = pd.read_csv(StringIO(content.decode('utf-8-sig', errors='replace')))
            else:
                df = pd.read_excel(BytesIO(content))
            df = df.where(pd.notnull(df), None)
            total = len(df)
            _jobs[job_id]['total'] = total
            imported = 0; errors = 0
            BATCH = 1000; batch = []
            db = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES)
            db.execute("PRAGMA journal_mode=WAL")
            log_id = str(uuid.uuid4())
            db.execute("INSERT INTO import_logs(id,filename,source_label,status) VALUES(?,?,?,'processing')",
                       (log_id, filename, source_label))
            db.commit()
            for idx, (_, row) in enumerate(df.iterrows()):
                try:
                    eid = str(uuid.uuid4())
                    batch.append((eid, 'MDM-'+eid[:8].upper(), 'active', source_label,
                                  json.dumps(row.to_dict(), ensure_ascii=False, default=str)))
                    if len(batch) >= BATCH:
                        db.executemany("INSERT INTO entities(id,mdm_id,status,source,data) VALUES(?,?,?,?,?)", batch)
                        imported += len(batch); batch = []
                except: errors += 1
                if idx % 500 == 0:
                    _jobs[job_id]['progress'] = round(idx / max(total, 1) * 100)
                    _jobs[job_id]['imported'] = imported
            if batch:
                db.executemany("INSERT INTO entities(id,mdm_id,status,source,data) VALUES(?,?,?,?,?)", batch)
                imported += len(batch)
            db.execute("UPDATE import_logs SET total_rows=?,imported=?,errors=?,status='done' WHERE id=?",
                       (total, imported, errors, log_id))
            db.commit(); db.close()
            _jobs[job_id].update({'status': 'done', 'progress': 100, 'imported': imported, 'errors': errors, 'log_id': log_id})
            _cache.pop('dashboard_stats', None)
        except Exception as e:
            _jobs[job_id].update({'status': 'error', 'error': str(e)})

    t = threading.Thread(target=run_import, daemon=True)
    t.start()
    return jsonify({'job_id': job_id, 'status': 'processing'}), 202

@app.route('/api/import/status/<job_id>', methods=['GET'])
@token_required
def import_status(job_id):
    """Polling du statut d'un import async"""
    job = _jobs.get(job_id)
    if not job: return jsonify({'error': 'Job introuvable'}), 404
    return jsonify(job)

# ── §4.1 WORKFLOW VALIDATION ─────────────────────
VALID_STATUSES = ['draft', 'review', 'validated', 'published']

@app.route('/api/entities/<eid>/validation', methods=['PUT'])
@token_required
def update_validation_status(eid):
    """Change le statut de validation : draft → review → validated → published"""
    b = request.json or {}
    new_status = b.get('validation_status', '')
    if new_status not in VALID_STATUSES:
        return jsonify({'error': f"Statut invalide. Valeurs : {', '.join(VALID_STATUSES)}"}), 400
    db = get_db()
    row = db.execute("SELECT id, status FROM entities WHERE id=?", (eid,)).fetchone()
    if not row: return jsonify({'error': 'Introuvable'}), 404
    # Vérification des rôles pour les transitions
    role = g.current_user.get('role', 'viewer')
    if new_status in ('validated', 'published') and role not in ('admin', 'manager'):
        return jsonify({'error': 'Seuls les managers et admins peuvent valider/publier'}), 403
    db.execute("UPDATE entities SET status=?, updated_at=datetime('now') WHERE id=?", (new_status, eid))
    db.commit()
    audit('validation', 'entity', eid, {'new_status': new_status, 'by': role})
    _cache.pop('dashboard_stats', None)
    return jsonify({'message': f'Statut mis à jour → {new_status}'})

# ── §4.1 COMMENTAIRES ENTITÉS ─────────────────────
@app.route('/api/comments', methods=['POST'])
@token_required
def add_comment():
    b = request.json or {}
    entity_type = b.get('entity_type', 'entity')
    entity_id = b.get('entity_id')
    comment = b.get('comment', '').strip()
    if not entity_id or not comment:
        return jsonify({'error': 'entity_id et comment requis'}), 400
    cid = str(uuid.uuid4())
    user_name = g.current_user.get('email', '?')
    # Récupérer le nom de l'utilisateur
    u = get_db().execute("SELECT name FROM users WHERE id=?", (g.current_user.get('user_id'),)).fetchone()
    if u: user_name = u['name'] or user_name
    get_db().execute(
        "INSERT INTO entity_comments(id,entity_type,entity_id,user_id,user_name,comment) VALUES(?,?,?,?,?,?)",
        (cid, entity_type, entity_id, g.current_user.get('user_id', '?'), user_name, comment))
    get_db().commit()
    return jsonify({'id': cid, 'message': 'Commentaire ajouté'}), 201

@app.route('/api/comments/<entity_type>/<entity_id>', methods=['GET'])
@token_required
def get_comments(entity_type, entity_id):
    rows = get_db().execute(
        "SELECT * FROM entity_comments WHERE entity_type=? AND entity_id=? ORDER BY created_at DESC",
        (entity_type, entity_id)).fetchall()
    return jsonify([dict(r) for r in rows])

# ── §4.2 DOUBLONS — Marquer comme différents ─────────
@app.route('/api/duplicates/mark-different', methods=['POST'])
@token_required
def mark_different():
    b = request.json or {}
    dup_id = b.get('duplicate_id')
    reason = b.get('reason', '')
    if not dup_id: return jsonify({'error': 'duplicate_id requis'}), 400
    db = get_db()
    db.execute("UPDATE duplicates SET status='different' WHERE id=?", (dup_id,))
    db.commit()
    audit('mark_different', 'duplicate', dup_id, {'reason': reason})
    return jsonify({'message': 'Marqué comme différent', 'reason': reason})

# ── §4.5 GOLDEN RECORD DÉTAILS ─────────────────────
@app.route('/api/golden-records/<gid>', methods=['GET'])
@token_required
def get_golden_record(gid):
    db = get_db()
    gr = db.execute("SELECT * FROM golden_records WHERE id=?", (gid,)).fetchone()
    if not gr: return jsonify({'error': 'Introuvable'}), 404
    result = dict(gr)
    for k in ('data', 'source_ids', 'rules_applied'):
        try: result[k] = json.loads(result[k])
        except: pass
    # Charger les entités sources
    source_entities = []
    for sid in (result.get('source_ids') or []):
        e = db.execute("SELECT id,mdm_id,data,source,status FROM entities WHERE id=?", (sid,)).fetchone()
        if e:
            ed = dict(e)
            try: ed['data'] = json.loads(ed['data'])
            except: pass
            source_entities.append(ed)
    result['source_entities'] = source_entities
    # Audit trail
    logs = db.execute("SELECT * FROM audit_log WHERE entity_id=? ORDER BY created_at DESC", (gid,)).fetchall()
    result['audit_trail'] = [dict(l) for l in logs]
    # Commentaires
    comments = db.execute("SELECT * FROM entity_comments WHERE entity_id=? ORDER BY created_at DESC", (gid,)).fetchall()
    result['comments'] = [dict(c) for c in comments]
    return jsonify(result)

# ── REPORTING PDF DATA ────────────────────────────
@app.route('/api/reporting/export-pdf-data', methods=['GET'])
@token_required
def export_pdf_data():
    db = get_db()
    return jsonify({
        'generated_at': datetime.now().strftime('%d/%m/%Y %H:%M'),
        'kpis': {
            'total_entities': db.execute("SELECT COUNT(*) FROM entities WHERE status='active'").fetchone()[0],
            'total_golden': db.execute("SELECT COUNT(*) FROM golden_records").fetchone()[0],
            'total_duplicates': db.execute("SELECT COUNT(*) FROM duplicates WHERE status='pending'").fetchone()[0],
            'total_imports': db.execute("SELECT COUNT(*) FROM import_logs").fetchone()[0],
        },
        'by_source': [dict(r) for r in db.execute("SELECT source,COUNT(*) as count FROM entities WHERE status IN ('active','merged') GROUP BY source ORDER BY count DESC").fetchall()],
        'import_trend': [dict(r) for r in db.execute("SELECT DATE(created_at) as day, COALESCE(SUM(imported),0) as rows FROM import_logs WHERE status='done' AND created_at>=date('now','-30 days') GROUP BY day ORDER BY day").fetchall()],
    })
