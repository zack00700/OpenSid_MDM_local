"""
O.S MDM V2 — Module Maritime
Gestion des entités : Navires, Escales, Armateurs, Ports & Terminaux
Standards : IMO, MMSI, UN/LOCODE, BIMCO
"""

import json, uuid, re
from datetime import datetime
from flask import Blueprint, request, jsonify, g
from functools import wraps

maritime_bp = Blueprint('maritime', __name__)

# ── STANDARDS & RÉFÉRENTIELS ───────────────────────────────────────────────

VESSEL_TYPES = [
    'Bulk Carrier', 'Container Ship', 'Tanker', 'General Cargo',
    'Ro-Ro', 'Passenger', 'Ferry', 'Tug', 'Offshore Supply',
    'Chemical Tanker', 'LNG Carrier', 'LPG Carrier', 'Dredger',
    'Fishing Vessel', 'Research Vessel', 'Yacht', 'Barge', 'Other'
]

FLAGS = {
    'PA': 'Panama', 'LR': 'Liberia', 'MH': 'Marshall Islands', 'HK': 'Hong Kong',
    'SG': 'Singapore', 'BS': 'Bahamas', 'MT': 'Malta', 'CY': 'Cyprus',
    'CN': 'China', 'GB': 'United Kingdom', 'NO': 'Norway', 'GR': 'Greece',
    'JP': 'Japan', 'KR': 'South Korea', 'IT': 'Italy', 'DE': 'Germany',
    'FR': 'France', 'US': 'United States', 'NL': 'Netherlands', 'DK': 'Denmark',
    'MA': 'Morocco', 'DZ': 'Algeria', 'TN': 'Tunisia', 'EG': 'Egypt',
    'NG': 'Nigeria', 'ZA': 'South Africa', 'AE': 'UAE', 'IN': 'India',
    'BR': 'Brazil', 'ES': 'Spain', 'PT': 'Portugal', 'TR': 'Turkey',
    'RU': 'Russia', 'BE': 'Belgium', 'SE': 'Sweden', 'FI': 'Finland',
}

PORT_FUNCTIONS = [
    'Port of Loading', 'Port of Discharge', 'Transhipment Hub',
    'Bunkering Port', 'Port of Refuge', 'Anchorage', 'Dry Dock',
    'River Port', 'Lake Port', 'Canal Port'
]

CALL_STATUSES = ['Planned', 'In Transit', 'At Anchor', 'Berthed', 'Departed', 'Cancelled']
CARGO_TYPES = ['Dry Bulk', 'Liquid Bulk', 'Container', 'Ro-Ro', 'General Cargo',
               'Project Cargo', 'Refrigerated', 'Hazardous', 'Passengers', 'Ballast', 'Other']

# ── VALIDATION RULES (règles MDM maritimes) ───────────────────────────────

def validate_imo(imo_raw):
    """
    Validation IMO number — standard international (IMO Res. A.600(15))
    Format : IMO + 7 chiffres, dernier = checkdigit
    Ex: IMO9321483
    """
    if not imo_raw:
        return None, 'IMO manquant'
    imo = str(imo_raw).strip().upper().replace(' ', '')
    if not imo.startswith('IMO'):
        imo = 'IMO' + imo
    digits = imo[3:]
    if not digits.isdigit() or len(digits) != 7:
        return None, f"IMO invalide '{imo_raw}' — doit être 7 chiffres après IMO"
    # Checksum IMO
    total = sum(int(d) * (7 - i) for i, d in enumerate(digits[:6]))
    if total % 10 != int(digits[6]):
        return None, f"Checksum IMO invalide pour '{imo}'"
    return imo, None

def validate_mmsi(mmsi_raw):
    """
    MMSI — Maritime Mobile Service Identity
    Format : 9 chiffres, commence par 2-7
    """
    if not mmsi_raw:
        return None, None  # optionnel
    mmsi = str(mmsi_raw).strip().replace(' ', '')
    if not mmsi.isdigit() or len(mmsi) != 9:
        return None, f"MMSI invalide '{mmsi_raw}' — doit être exactement 9 chiffres"
    if mmsi[0] not in '234567':
        return None, f"MMSI invalide — premier chiffre doit être entre 2 et 7"
    return mmsi, None

def validate_locode(locode_raw):
    """
    UN/LOCODE — identifiant port international
    Format : 2 lettres pays + espace + 3 lettres/chiffres
    Ex: MACAO, FRPAR, NLRTM (Rotterdam)
    """
    if not locode_raw:
        return None, 'UN/LOCODE manquant'
    raw = str(locode_raw).strip().upper().replace(' ', '')
    if len(raw) == 5:
        locode = raw[:2] + ' ' + raw[2:]
    else:
        locode = str(locode_raw).strip().upper()
    parts = locode.split()
    if len(parts) != 2 or len(parts[0]) != 2 or len(parts[1]) != 3:
        return None, f"UN/LOCODE invalide '{locode_raw}' — format attendu: XX YYY (ex: MA AGD)"
    if not parts[0].isalpha():
        return None, f"Code pays invalide dans UN/LOCODE '{locode_raw}'"
    return locode, None

def normalize_vessel_name(name):
    """
    Normalisation nom navire pour MDM matching
    Supprime les préfixes standard (MV, MS, SS, MT, etc.)
    """
    if not name:
        return ''
    prefixes = ['MV ', 'MS ', 'SS ', 'MT ', 'M/V ', 'M/T ', 'M.V. ', 'M.T. ',
                'SV ', 'RV ', 'FSO ', 'FPSO ', 'FPU ']
    normalized = str(name).strip().upper()
    for p in prefixes:
        if normalized.startswith(p):
            normalized = normalized[len(p):]
            break
    return normalized.strip()

# ── §2.2.2 NORMALISATION ARMATEURS (pipeline 5 étapes) ────────
LEGAL_SUFFIXES = re.compile(
    r'\b(S\.?A\.?|Ltd\.?|GmbH|Co\.?|Inc\.?|B\.?V\.?|AS|Pte|N\.?V\.?|LLC|LLP|Corp\.?|Pty|PLC|SE|KG|AG|SRL|SARL|SAS)\b\.?',
    re.IGNORECASE)

KNOWN_ACRONYMS = {
    'MSC': 'MEDITERRANEAN SHIPPING COMPANY',
    'CMA CGM': 'CMA CGM',
    'CMA-CGM': 'CMA CGM',
    'COSCO': 'COSCO SHIPPING',
    'MAERSK': 'MAERSK',
    'HAPAG': 'HAPAG LLOYD',
    'HAPAG-LLOYD': 'HAPAG LLOYD',
    'EVERGREEN': 'EVERGREEN',
    'PIL': 'PACIFIC INTERNATIONAL LINES',
    'ZIM': 'ZIM',
    'HMM': 'HYUNDAI MERCHANT MARINE',
    'YANG MING': 'YANG MING',
    'ONE': 'OCEAN NETWORK EXPRESS',
    'OOCL': 'ORIENT OVERSEAS CONTAINER LINE',
    'WAN HAI': 'WAN HAI LINES',
}

def normalize_owner_name(name):
    """§2.2.2 — Pipeline normalisation armateurs en 5 étapes"""
    if not name:
        return ''
    n = str(name).strip().upper()
    # Étape 1 : Suppression suffixes juridiques
    n = LEGAL_SUFFIXES.sub('', n).strip()
    # Étape 2 : Normalisation acronymes connus
    for acronym, canonical in KNOWN_ACRONYMS.items():
        if acronym in n or n.startswith(acronym.split()[0]):
            n = canonical
            break
    # Étape 3 : Translittération basique (caractères spéciaux → ASCII)
    import unicodedata
    n = unicodedata.normalize('NFKD', n).encode('ascii', 'ignore').decode('ascii')
    # Étape 4 : Token sort (gérer inversions)
    tokens = sorted(n.split())
    n = ' '.join(tokens)
    # Étape 5 : Nettoyage final
    n = re.sub(r'\s+', ' ', n).strip()
    n = re.sub(r'[^\w\s]', '', n).strip()
    return n

# ── §2.2.4 SCORE DE CONFIANCE PONDÉRÉ ────────
def compute_confidence_score(vessel_data, errors):
    """§2.2.4 — Score multi-facteurs au lieu du calcul linéaire"""
    score = 0.0
    # IMO valide : 35%
    if vessel_data.get('imo_number') and not any('IMO' in str(e) for e in errors):
        score += 0.35
    # MMSI valide : 15%
    if vessel_data.get('mmsi') and not any('MMSI' in str(e) for e in errors):
        score += 0.15
    # Nom présent : 15%
    if vessel_data.get('vessel_name', '').strip():
        score += 0.15
    # Type renseigné : 10%
    if vessel_data.get('vessel_type'):
        score += 0.10
    # Pavillon renseigné : 10%
    if vessel_data.get('flag_code'):
        score += 0.10
    # Tonnage renseigné : 5%
    if vessel_data.get('gross_tonnage'):
        score += 0.05
    # Année construction : 5%
    if vessel_data.get('year_built'):
        score += 0.05
    # Aucune erreur : bonus 5%
    if not errors:
        score += 0.05
    return round(min(1.0, max(0.1, score)), 2)

def detect_vessel_duplicates(db, vessel_data, exclude_id=None):
    """
    Règles de déduplication navire :
    1. IMO identique → doublon certain (score 1.0)
    2. MMSI identique → doublon probable (score 0.95)
    3. Nom normalisé + pavillon identiques → doublon possible (score 0.8)
    """
    duplicates = []
    vessels = db.execute(
        "SELECT * FROM maritime_vessels WHERE status='active'" +
        (f" AND id != '{exclude_id}'" if exclude_id else "")
    ).fetchall()

    new_imo  = vessel_data.get('imo_number', '')
    new_mmsi = vessel_data.get('mmsi', '')
    new_name = normalize_vessel_name(vessel_data.get('vessel_name', ''))
    new_flag = vessel_data.get('flag_code', '')

    for v in vessels:
        try: vd = json.loads(v['data'])
        except: continue

        # Règle 1 : IMO identique
        if new_imo and new_imo == vd.get('imo_number'):
            duplicates.append({'vessel_id': v['id'], 'score': 1.0, 'reason': 'IMO identique'})
            continue

        # Règle 2 : MMSI identique
        if new_mmsi and new_mmsi == vd.get('mmsi'):
            duplicates.append({'vessel_id': v['id'], 'score': 0.95, 'reason': 'MMSI identique'})
            continue

        # Règle 3 : Nom normalisé + pavillon
        existing_name = normalize_vessel_name(vd.get('vessel_name', ''))
        if new_name and existing_name and new_name == existing_name and new_flag == vd.get('flag_code'):
            duplicates.append({'vessel_id': v['id'], 'score': 0.8, 'reason': 'Nom + pavillon identiques'})

    return duplicates

def get_db_from_app():
    """Récupère la connexion DB partagée depuis le contexte Flask (§3.4 Audit)"""
    import sqlite3
    from flask import current_app
    # Réutiliser g.db si déjà ouvert par app.py, sinon en créer un
    if 'db' not in g:
        db_path = current_app.config.get('DB_PATH')
        g.db = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA foreign_keys=ON")
    return g.db

# ── INIT TABLES ──────────────────────────────────────────────────────────

MARITIME_SCHEMA = """
CREATE TABLE IF NOT EXISTS maritime_vessels (
    id TEXT PRIMARY KEY,
    mdm_id TEXT UNIQUE,
    status TEXT DEFAULT 'active',
    source TEXT DEFAULT 'manual',
    imo_number TEXT UNIQUE,
    mmsi TEXT,
    vessel_name TEXT NOT NULL,
    vessel_name_normalized TEXT,
    vessel_type TEXT,
    flag_code TEXT,
    flag_name TEXT,
    gross_tonnage REAL,
    deadweight REAL,
    year_built INTEGER,
    owner_id TEXT,
    operator TEXT,
    class_society TEXT,
    data TEXT,
    validation_errors TEXT,
    confidence_score REAL DEFAULT 1.0,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS maritime_owners (
    id TEXT PRIMARY KEY,
    mdm_id TEXT UNIQUE,
    status TEXT DEFAULT 'active',
    source TEXT DEFAULT 'manual',
    owner_name TEXT NOT NULL,
    owner_name_normalized TEXT,
    owner_type TEXT,
    country_code TEXT,
    country_name TEXT,
    city TEXT,
    address TEXT,
    contact_email TEXT,
    contact_phone TEXT,
    fleet_size INTEGER DEFAULT 0,
    data TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS maritime_ports (
    id TEXT PRIMARY KEY,
    mdm_id TEXT UNIQUE,
    status TEXT DEFAULT 'active',
    source TEXT DEFAULT 'manual',
    port_name TEXT NOT NULL,
    un_locode TEXT UNIQUE,
    country_code TEXT,
    country_name TEXT,
    latitude REAL,
    longitude REAL,
    port_function TEXT,
    max_vessel_size TEXT,
    max_draft REAL,
    tide_dependent INTEGER DEFAULT 0,
    pilotage_required INTEGER DEFAULT 1,
    data TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS maritime_port_calls (
    id TEXT PRIMARY KEY,
    mdm_id TEXT UNIQUE,
    status TEXT DEFAULT 'active',
    source TEXT DEFAULT 'manual',
    vessel_id TEXT,
    vessel_name TEXT,
    imo_number TEXT,
    port_id TEXT,
    port_name TEXT,
    un_locode TEXT,
    terminal TEXT,
    berth TEXT,
    eta TEXT,
    etd TEXT,
    ata TEXT,
    atd TEXT,
    call_status TEXT DEFAULT 'Planned',
    cargo_type TEXT,
    cargo_quantity REAL,
    cargo_unit TEXT,
    agent_name TEXT,
    voyage_number TEXT,
    data TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);
"""

# ── HELPERS ──────────────────────────────────────────────────────────────

def maritime_response(rows):
    result = []
    for r in rows:
        item = dict(r)
        for k in ('data', 'validation_errors'):
            if k in item:
                try: item[k] = json.loads(item[k]) if item[k] else {}
                except: pass
        result.append(item)
    return result

def new_maritime_id(prefix):
    uid = str(uuid.uuid4())
    return uid, f"{prefix}-{uid[:8].upper()}"

# ── VESSELS ──────────────────────────────────────────────────────────────

@maritime_bp.route('/vessels', methods=['GET'])
def list_vessels():
    db   = get_db_from_app()
    page = int(request.args.get('page', 1))
    per  = int(request.args.get('per_page', 20))
    srch = request.args.get('search', '').strip()
    flag = request.args.get('flag', '').strip()
    vtype= request.args.get('vessel_type', '').strip()
    wh   = ["status='active'"]; params = []
    if srch:  wh.append("(vessel_name LIKE ? OR imo_number LIKE ? OR mmsi LIKE ?)"); params += [f'%{srch}%']*3
    if flag:  wh.append("flag_code=?"); params.append(flag)
    if vtype: wh.append("vessel_type=?"); params.append(vtype)
    w = ' AND '.join(wh)
    total = db.execute(f"SELECT COUNT(*) FROM maritime_vessels WHERE {w}", params).fetchone()[0]
    rows  = db.execute(f"SELECT * FROM maritime_vessels WHERE {w} ORDER BY created_at DESC LIMIT ? OFFSET ?",
                       params+[per, (page-1)*per]).fetchall()
    return jsonify({'total': total, 'page': page, 'per_page': per,
                    'vessels': maritime_response(rows)})

@maritime_bp.route('/vessels/<vid>', methods=['GET'])
def get_vessel(vid):
    r = get_db_from_app().execute("SELECT * FROM maritime_vessels WHERE id=?", (vid,)).fetchone()
    if not r: return jsonify({'error': 'Navire introuvable'}), 404
    return jsonify(maritime_response([r])[0])

@maritime_bp.route('/vessels', methods=['POST'])
def create_vessel():
    b  = request.json or {}
    db = get_db_from_app()
    errors = []

    # Validation IMO
    imo, err = validate_imo(b.get('imo_number'))
    if err: errors.append(err)

    # Validation MMSI
    mmsi, err = validate_mmsi(b.get('mmsi'))
    if err: errors.append(err)

    # Nom obligatoire
    vessel_name = str(b.get('vessel_name', '')).strip()
    if not vessel_name: errors.append('Nom du navire obligatoire')

    # Vérifier unicité IMO
    if imo:
        existing = db.execute("SELECT id FROM maritime_vessels WHERE imo_number=?", (imo,)).fetchone()
        if existing:
            return jsonify({'error': f"Un navire avec l'IMO {imo} existe déjà", 'existing_id': existing['id']}), 409

    vid, mdm_id = new_maritime_id('VES')
    name_norm   = normalize_vessel_name(vessel_name)
    flag_code   = str(b.get('flag_code', '')).strip().upper()
    flag_name   = FLAGS.get(flag_code, flag_code)
    owner_id    = b.get('owner_id')

    # Détection doublons préventive
    dups = detect_vessel_duplicates(db, {
        'imo_number': imo, 'mmsi': mmsi,
        'vessel_name': vessel_name, 'flag_code': flag_code
    })

    # §2.2.4 — Score de confiance pondéré multi-facteurs
    confidence = compute_confidence_score({
        'imo_number': imo, 'mmsi': mmsi, 'vessel_name': vessel_name,
        'vessel_type': b.get('vessel_type'), 'flag_code': flag_code,
        'gross_tonnage': b.get('gross_tonnage'), 'year_built': b.get('year_built'),
    }, errors)

    extra_data = {k: v for k, v in b.items() if k not in
                  ['imo_number','mmsi','vessel_name','vessel_type','flag_code','flag_name',
                   'gross_tonnage','deadweight','year_built','owner_id','operator','class_society']}

    db.execute("""INSERT INTO maritime_vessels
        (id,mdm_id,status,source,imo_number,mmsi,vessel_name,vessel_name_normalized,
         vessel_type,flag_code,flag_name,gross_tonnage,deadweight,year_built,
         owner_id,operator,class_society,data,validation_errors,confidence_score)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (vid, mdm_id, 'active', b.get('source','manual'),
         imo, mmsi, vessel_name, name_norm,
         b.get('vessel_type'), flag_code, flag_name,
         b.get('gross_tonnage'), b.get('deadweight'), b.get('year_built'),
         owner_id, b.get('operator'), b.get('class_society'),
         json.dumps(extra_data, ensure_ascii=False),
         json.dumps(errors) if errors else None,
         confidence))
    db.commit()

    return jsonify({
        'id': vid, 'mdm_id': mdm_id,
        'validation_errors': errors,
        'potential_duplicates': dups,
        'confidence_score': confidence,
        'warning': f"{len(dups)} doublon(s) potentiel(s) détecté(s)" if dups else None
    }), 201

@maritime_bp.route('/vessels/<vid>', methods=['PUT'])
def update_vessel(vid):
    b  = request.json or {}
    db = get_db_from_app()
    if not db.execute("SELECT id FROM maritime_vessels WHERE id=?", (vid,)).fetchone():
        return jsonify({'error': 'Introuvable'}), 404
    errors = []
    imo,  err = validate_imo(b.get('imo_number')); errors += [err] if err else []
    mmsi, err = validate_mmsi(b.get('mmsi'));      errors += [err] if err else []
    vessel_name = str(b.get('vessel_name', '')).strip()
    flag_code   = str(b.get('flag_code', '')).strip().upper()
    db.execute("""UPDATE maritime_vessels SET
        imo_number=?,mmsi=?,vessel_name=?,vessel_name_normalized=?,vessel_type=?,
        flag_code=?,flag_name=?,gross_tonnage=?,deadweight=?,year_built=?,
        owner_id=?,operator=?,class_society=?,validation_errors=?,updated_at=datetime('now')
        WHERE id=?""",
        (imo, mmsi, vessel_name, normalize_vessel_name(vessel_name),
         b.get('vessel_type'), flag_code, FLAGS.get(flag_code, flag_code),
         b.get('gross_tonnage'), b.get('deadweight'), b.get('year_built'),
         b.get('owner_id'), b.get('operator'), b.get('class_society'),
         json.dumps(errors) if errors else None, vid))
    db.commit()
    return jsonify({'message': 'Navire mis à jour', 'validation_errors': errors})

@maritime_bp.route('/vessels/<vid>', methods=['DELETE'])
def delete_vessel(vid):
    db = get_db_from_app()
    db.execute("UPDATE maritime_vessels SET status='deleted' WHERE id=?", (vid,))
    db.commit()
    return jsonify({'message': 'Navire supprimé'})

@maritime_bp.route('/vessels/<vid>/port-calls', methods=['GET'])
def vessel_port_calls(vid):
    r   = get_db_from_app().execute("SELECT * FROM maritime_vessels WHERE id=?", (vid,)).fetchone()
    if not r: return jsonify({'error': 'Navire introuvable'}), 404
    calls = get_db_from_app().execute(
        "SELECT * FROM maritime_port_calls WHERE vessel_id=? ORDER BY eta DESC", (vid,)).fetchall()
    return jsonify({'vessel': maritime_response([r])[0], 'port_calls': maritime_response(calls)})

@maritime_bp.route('/vessels/validate-imo', methods=['POST'])
def validate_imo_route():
    imo_raw = (request.json or {}).get('imo')
    imo, err = validate_imo(imo_raw)
    return jsonify({'valid': err is None, 'imo': imo, 'error': err})

@maritime_bp.route('/vessels/validate-mmsi', methods=['POST'])
def validate_mmsi_route():
    mmsi_raw = (request.json or {}).get('mmsi')
    mmsi, err = validate_mmsi(mmsi_raw)
    return jsonify({'valid': err is None, 'mmsi': mmsi, 'error': err})

# ── OWNERS ───────────────────────────────────────────────────────────────

@maritime_bp.route('/owners', methods=['GET'])
def list_owners():
    db   = get_db_from_app()
    page = int(request.args.get('page', 1))
    per  = int(request.args.get('per_page', 20))
    srch = request.args.get('search', '').strip()
    country = request.args.get('country', '').strip()
    wh = ["status='active'"]; params = []
    if srch:    wh.append("owner_name LIKE ?"); params.append(f'%{srch}%')
    if country: wh.append("country_code=?");   params.append(country)
    w = ' AND '.join(wh)
    total = db.execute(f"SELECT COUNT(*) FROM maritime_owners WHERE {w}", params).fetchone()[0]
    rows  = db.execute(f"SELECT * FROM maritime_owners WHERE {w} ORDER BY owner_name LIMIT ? OFFSET ?",
                       params+[per, (page-1)*per]).fetchall()
    return jsonify({'total': total, 'page': page, 'per_page': per,
                    'owners': maritime_response(rows)})

@maritime_bp.route('/owners/<oid>', methods=['GET'])
def get_owner(oid):
    r = get_db_from_app().execute("SELECT * FROM maritime_owners WHERE id=?", (oid,)).fetchone()
    if not r: return jsonify({'error': 'Armateur introuvable'}), 404
    # Enrichir avec la flotte
    fleet = get_db_from_app().execute(
        "SELECT id,mdm_id,vessel_name,imo_number,vessel_type,flag_code FROM maritime_vessels WHERE owner_id=? AND status='active'", (oid,)).fetchall()
    result = maritime_response([r])[0]
    result['fleet'] = [dict(v) for v in fleet]
    result['fleet_size'] = len(fleet)
    return jsonify(result)

@maritime_bp.route('/owners', methods=['POST'])
def create_owner():
    b  = request.json or {}
    db = get_db_from_app()
    oid, mdm_id = new_maritime_id('OWN')
    owner_name  = str(b.get('owner_name', '')).strip()
    if not owner_name: return jsonify({'error': 'Nom armateur obligatoire'}), 400
    country_code = str(b.get('country_code', '')).strip().upper()
    extra = {k: v for k, v in b.items() if k not in
             ['owner_name','owner_type','country_code','country_name','city','address','contact_email','contact_phone']}
    db.execute("""INSERT INTO maritime_owners
        (id,mdm_id,status,source,owner_name,owner_name_normalized,owner_type,
         country_code,country_name,city,address,contact_email,contact_phone,data)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (oid, mdm_id, 'active', b.get('source','manual'),
         owner_name, normalize_owner_name(owner_name),
         b.get('owner_type'), country_code, FLAGS.get(country_code, b.get('country_name','')),
         b.get('city'), b.get('address'), b.get('contact_email'), b.get('contact_phone'),
         json.dumps(extra, ensure_ascii=False)))
    db.commit()
    return jsonify({'id': oid, 'mdm_id': mdm_id}), 201

@maritime_bp.route('/owners/<oid>', methods=['PUT'])
def update_owner(oid):
    b  = request.json or {}
    db = get_db_from_app()
    if not db.execute("SELECT id FROM maritime_owners WHERE id=?", (oid,)).fetchone():
        return jsonify({'error': 'Introuvable'}), 404
    country_code = str(b.get('country_code', '')).strip().upper()
    db.execute("""UPDATE maritime_owners SET owner_name=?,owner_name_normalized=?,owner_type=?,
        country_code=?,country_name=?,city=?,address=?,contact_email=?,contact_phone=?,
        updated_at=datetime('now') WHERE id=?""",
        (b.get('owner_name'), normalize_owner_name(b.get('owner_name','')),
         b.get('owner_type'), country_code, FLAGS.get(country_code,''),
         b.get('city'), b.get('address'), b.get('contact_email'), b.get('contact_phone'), oid))
    db.commit()
    return jsonify({'message': 'Armateur mis à jour'})

@maritime_bp.route('/owners/<oid>', methods=['DELETE'])
def delete_owner(oid):
    db = get_db_from_app()
    db.execute("UPDATE maritime_owners SET status='deleted' WHERE id=?", (oid,))
    db.commit()
    return jsonify({'message': 'Armateur supprimé'})

# ── PORTS ────────────────────────────────────────────────────────────────

@maritime_bp.route('/ports', methods=['GET'])
def list_ports():
    db   = get_db_from_app()
    page = int(request.args.get('page', 1))
    per  = int(request.args.get('per_page', 20))
    srch = request.args.get('search', '').strip()
    country = request.args.get('country', '').strip()
    wh = ["status='active'"]; params = []
    if srch:    wh.append("(port_name LIKE ? OR un_locode LIKE ?)"); params += [f'%{srch}%']*2
    if country: wh.append("country_code=?"); params.append(country)
    w = ' AND '.join(wh)
    total = db.execute(f"SELECT COUNT(*) FROM maritime_ports WHERE {w}", params).fetchone()[0]
    rows  = db.execute(f"SELECT * FROM maritime_ports WHERE {w} ORDER BY port_name LIMIT ? OFFSET ?",
                       params+[per, (page-1)*per]).fetchall()
    return jsonify({'total': total, 'page': page, 'per_page': per,
                    'ports': maritime_response(rows)})

@maritime_bp.route('/ports/<pid>', methods=['GET'])
def get_port(pid):
    r = get_db_from_app().execute("SELECT * FROM maritime_ports WHERE id=?", (pid,)).fetchone()
    if not r: return jsonify({'error': 'Port introuvable'}), 404
    # Dernières escales dans ce port
    recent_calls = get_db_from_app().execute(
        "SELECT * FROM maritime_port_calls WHERE port_id=? ORDER BY eta DESC LIMIT 10", (pid,)).fetchall()
    result = maritime_response([r])[0]
    result['recent_calls'] = maritime_response(recent_calls)
    return jsonify(result)

@maritime_bp.route('/ports', methods=['POST'])
def create_port():
    b  = request.json or {}
    db = get_db_from_app()
    pid, mdm_id = new_maritime_id('PRT')
    port_name   = str(b.get('port_name', '')).strip()
    if not port_name: return jsonify({'error': 'Nom du port obligatoire'}), 400

    # Validation UN/LOCODE
    locode, err = validate_locode(b.get('un_locode'))
    if err: return jsonify({'error': err, 'field': 'un_locode'}), 400

    # Vérifier unicité LOCODE
    if locode:
        existing = db.execute("SELECT id FROM maritime_ports WHERE un_locode=?", (locode,)).fetchone()
        if existing:
            return jsonify({'error': f"Port avec LOCODE {locode} existe déjà", 'existing_id': existing['id']}), 409

    country_code = str(b.get('country_code', locode[:2] if locode else '')).strip().upper()
    extra = {k: v for k, v in b.items() if k not in
             ['port_name','un_locode','country_code','country_name','latitude','longitude',
              'port_function','max_vessel_size','max_draft','tide_dependent','pilotage_required']}

    db.execute("""INSERT INTO maritime_ports
        (id,mdm_id,status,source,port_name,un_locode,country_code,country_name,
         latitude,longitude,port_function,max_vessel_size,max_draft,tide_dependent,pilotage_required,data)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (pid, mdm_id, 'active', b.get('source','manual'),
         port_name, locode, country_code, FLAGS.get(country_code, b.get('country_name','')),
         b.get('latitude'), b.get('longitude'), b.get('port_function'),
         b.get('max_vessel_size'), b.get('max_draft'),
         1 if b.get('tide_dependent') else 0,
         1 if b.get('pilotage_required', True) else 0,
         json.dumps(extra, ensure_ascii=False)))
    db.commit()
    return jsonify({'id': pid, 'mdm_id': mdm_id, 'un_locode': locode}), 201

@maritime_bp.route('/ports/<pid>', methods=['PUT'])
def update_port(pid):
    b  = request.json or {}
    db = get_db_from_app()
    if not db.execute("SELECT id FROM maritime_ports WHERE id=?", (pid,)).fetchone():
        return jsonify({'error': 'Introuvable'}), 404
    locode, err = validate_locode(b.get('un_locode'))
    if err: return jsonify({'error': err}), 400
    country_code = str(b.get('country_code', '')).strip().upper()
    db.execute("""UPDATE maritime_ports SET port_name=?,un_locode=?,country_code=?,country_name=?,
        latitude=?,longitude=?,port_function=?,max_vessel_size=?,max_draft=?,
        tide_dependent=?,pilotage_required=?,updated_at=datetime('now') WHERE id=?""",
        (b.get('port_name'), locode, country_code, FLAGS.get(country_code,''),
         b.get('latitude'), b.get('longitude'), b.get('port_function'),
         b.get('max_vessel_size'), b.get('max_draft'),
         1 if b.get('tide_dependent') else 0,
         1 if b.get('pilotage_required', True) else 0, pid))
    db.commit()
    return jsonify({'message': 'Port mis à jour'})

@maritime_bp.route('/ports/<pid>', methods=['DELETE'])
def delete_port(pid):
    get_db_from_app().execute("UPDATE maritime_ports SET status='deleted' WHERE id=?", (pid,))
    get_db_from_app().commit()
    return jsonify({'message': 'Port supprimé'})

# ── PORT CALLS ────────────────────────────────────────────────────────────

@maritime_bp.route('/port-calls', methods=['GET'])
def list_port_calls():
    db   = get_db_from_app()
    page = int(request.args.get('page', 1))
    per  = int(request.args.get('per_page', 20))
    srch = request.args.get('search', '').strip()
    status_f   = request.args.get('status', '').strip()
    vessel_id  = request.args.get('vessel_id', '').strip()
    port_id    = request.args.get('port_id', '').strip()
    locode_f   = request.args.get('locode', '').strip()
    wh = ["pc.status != 'deleted'"]; params = []
    if srch:      wh.append("(pc.vessel_name LIKE ? OR pc.imo_number LIKE ? OR pc.port_name LIKE ?)"); params += [f'%{srch}%']*3
    if status_f:  wh.append("pc.call_status=?");   params.append(status_f)
    if vessel_id: wh.append("pc.vessel_id=?");     params.append(vessel_id)
    if port_id:   wh.append("pc.port_id=?");       params.append(port_id)
    if locode_f:  wh.append("pc.un_locode LIKE ?");params.append(f'%{locode_f}%')
    w = ' AND '.join(wh)
    total = db.execute(f"SELECT COUNT(*) FROM maritime_port_calls pc WHERE {w}", params).fetchone()[0]
    rows  = db.execute(f"SELECT pc.* FROM maritime_port_calls pc WHERE {w} ORDER BY pc.eta DESC LIMIT ? OFFSET ?",
                       params+[per, (page-1)*per]).fetchall()
    return jsonify({'total': total, 'page': page, 'per_page': per,
                    'port_calls': maritime_response(rows)})

@maritime_bp.route('/port-calls/<cid>', methods=['GET'])
def get_port_call(cid):
    r = get_db_from_app().execute("SELECT * FROM maritime_port_calls WHERE id=?", (cid,)).fetchone()
    if not r: return jsonify({'error': 'Escale introuvable'}), 404
    return jsonify(maritime_response([r])[0])

@maritime_bp.route('/port-calls', methods=['POST'])
def create_port_call():
    b  = request.json or {}
    db = get_db_from_app()
    cid, mdm_id = new_maritime_id('ETA')
    errors = []

    # Résolution navire
    vessel_id = b.get('vessel_id')
    vessel_name = b.get('vessel_name', '')
    imo_number  = b.get('imo_number', '')
    if not vessel_id and imo_number:
        v = db.execute("SELECT id,vessel_name FROM maritime_vessels WHERE imo_number=?", (imo_number,)).fetchone()
        if v: vessel_id = v['id']; vessel_name = v['vessel_name']
    if not vessel_name and vessel_id:
        v = db.execute("SELECT vessel_name,imo_number FROM maritime_vessels WHERE id=?", (vessel_id,)).fetchone()
        if v: vessel_name=v['vessel_name']; imo_number=v['imo_number']

    # Résolution port
    port_id   = b.get('port_id')
    port_name = b.get('port_name', '')
    un_locode = b.get('un_locode', '')
    if not port_id and un_locode:
        p = db.execute("SELECT id,port_name FROM maritime_ports WHERE un_locode=?", (un_locode,)).fetchone()
        if p: port_id=p['id']; port_name=p['port_name']
    if not port_name and port_id:
        p = db.execute("SELECT port_name,un_locode FROM maritime_ports WHERE id=?", (port_id,)).fetchone()
        if p: port_name=p['port_name']; un_locode=p['un_locode']

    # Validation dates ETA/ETD
    eta = b.get('eta'); etd = b.get('etd')
    if eta and etd:
        try:
            eta_dt = datetime.fromisoformat(eta.replace('Z',''))
            etd_dt = datetime.fromisoformat(etd.replace('Z',''))
            if etd_dt < eta_dt:
                errors.append('ETD ne peut pas être avant ETA')
        except: errors.append('Format de date invalide (ISO 8601 attendu)')

    # Validation statut
    call_status = b.get('call_status', 'Planned')
    if call_status not in CALL_STATUSES:
        errors.append(f"Statut invalide '{call_status}'. Valeurs: {', '.join(CALL_STATUSES)}")
        call_status = 'Planned'

    if not vessel_name: errors.append('Navire requis (vessel_id ou imo_number ou vessel_name)')
    if not port_name:   errors.append('Port requis (port_id ou un_locode ou port_name)')

    extra = {k: v for k, v in b.items() if k not in
             ['vessel_id','vessel_name','imo_number','port_id','port_name','un_locode',
              'terminal','berth','eta','etd','ata','atd','call_status',
              'cargo_type','cargo_quantity','cargo_unit','agent_name','voyage_number']}

    db.execute("""INSERT INTO maritime_port_calls
        (id,mdm_id,status,source,vessel_id,vessel_name,imo_number,port_id,port_name,un_locode,
         terminal,berth,eta,etd,ata,atd,call_status,cargo_type,cargo_quantity,cargo_unit,
         agent_name,voyage_number,data)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (cid, mdm_id, 'active', b.get('source','manual'),
         vessel_id, vessel_name, imo_number, port_id, port_name, un_locode,
         b.get('terminal'), b.get('berth'), eta, etd, b.get('ata'), b.get('atd'),
         call_status, b.get('cargo_type'), b.get('cargo_quantity'),
         b.get('cargo_unit','MT'), b.get('agent_name'), b.get('voyage_number'),
         json.dumps(extra, ensure_ascii=False)))
    db.commit()
    return jsonify({'id': cid, 'mdm_id': mdm_id, 'validation_errors': errors,
                    'warning': '; '.join(errors) if errors else None}), 201

@maritime_bp.route('/port-calls/<cid>', methods=['PUT'])
def update_port_call(cid):
    b  = request.json or {}
    db = get_db_from_app()
    if not db.execute("SELECT id FROM maritime_port_calls WHERE id=?", (cid,)).fetchone():
        return jsonify({'error': 'Introuvable'}), 404
    call_status = b.get('call_status', 'Planned')
    if call_status not in CALL_STATUSES: call_status = 'Planned'
    db.execute("""UPDATE maritime_port_calls SET
        vessel_name=?,imo_number=?,port_name=?,un_locode=?,terminal=?,berth=?,
        eta=?,etd=?,ata=?,atd=?,call_status=?,cargo_type=?,cargo_quantity=?,
        cargo_unit=?,agent_name=?,voyage_number=?,updated_at=datetime('now') WHERE id=?""",
        (b.get('vessel_name'), b.get('imo_number'), b.get('port_name'), b.get('un_locode'),
         b.get('terminal'), b.get('berth'), b.get('eta'), b.get('etd'),
         b.get('ata'), b.get('atd'), call_status, b.get('cargo_type'),
         b.get('cargo_quantity'), b.get('cargo_unit','MT'),
         b.get('agent_name'), b.get('voyage_number'), cid))
    db.commit()
    return jsonify({'message': 'Escale mise à jour'})

@maritime_bp.route('/port-calls/<cid>', methods=['DELETE'])
def delete_port_call(cid):
    get_db_from_app().execute("UPDATE maritime_port_calls SET status='deleted' WHERE id=?", (cid,))
    get_db_from_app().commit()
    return jsonify({'message': 'Escale supprimée'})

# ── MARITIME STATS ────────────────────────────────────────────────────────

@maritime_bp.route('/stats', methods=['GET'])
def maritime_stats():
    db = get_db_from_app()
    return jsonify({
        'total_vessels':    db.execute("SELECT COUNT(*) FROM maritime_vessels WHERE status='active'").fetchone()[0],
        'total_owners':     db.execute("SELECT COUNT(*) FROM maritime_owners WHERE status='active'").fetchone()[0],
        'total_ports':      db.execute("SELECT COUNT(*) FROM maritime_ports WHERE status='active'").fetchone()[0],
        'total_port_calls': db.execute("SELECT COUNT(*) FROM maritime_port_calls WHERE status='active'").fetchone()[0],
        'active_calls':     db.execute("SELECT COUNT(*) FROM maritime_port_calls WHERE call_status IN ('Berthed','At Anchor','In Transit')").fetchone()[0],
        'vessels_by_flag':  [dict(r) for r in db.execute("SELECT flag_code,flag_name,COUNT(*) as count FROM maritime_vessels WHERE status='active' GROUP BY flag_code ORDER BY count DESC LIMIT 10").fetchall()],
        'vessels_by_type':  [dict(r) for r in db.execute("SELECT vessel_type,COUNT(*) as count FROM maritime_vessels WHERE status='active' GROUP BY vessel_type ORDER BY count DESC").fetchall()],
        'calls_by_status':  [dict(r) for r in db.execute("SELECT call_status,COUNT(*) as count FROM maritime_port_calls GROUP BY call_status ORDER BY count DESC").fetchall()],
        'top_ports':        [dict(r) for r in db.execute("SELECT port_name,un_locode,COUNT(*) as call_count FROM maritime_port_calls GROUP BY port_id ORDER BY call_count DESC LIMIT 10").fetchall()],
        'calls_timeline':   [dict(r) for r in db.execute("SELECT DATE(eta) as day,COUNT(*) as count FROM maritime_port_calls WHERE eta>=date('now','-30 days') GROUP BY day ORDER BY day").fetchall()],
        'low_confidence':   db.execute("SELECT COUNT(*) FROM maritime_vessels WHERE confidence_score < 0.8").fetchone()[0],
        'validation_errors':db.execute("SELECT COUNT(*) FROM maritime_vessels WHERE validation_errors IS NOT NULL AND validation_errors != 'null'").fetchone()[0],
    })

@maritime_bp.route('/duplicates/detect', methods=['POST'])
def detect_maritime_duplicates():
    """Détection doublons navires basée sur règles IMO/MMSI/Nom"""
    db      = get_db_from_app()
    vessels = db.execute("SELECT * FROM maritime_vessels WHERE status='active'").fetchall()
    found   = 0
    groups  = {'by_imo':{}, 'by_mmsi':{}, 'by_name_flag':{}}
    for v in vessels:
        if v['imo_number']:  groups['by_imo'].setdefault(v['imo_number'], []).append(v['id'])
        if v['mmsi']:        groups['by_mmsi'].setdefault(v['mmsi'], []).append(v['id'])
        key = f"{v['vessel_name_normalized']}|{v['flag_code']}"
        if v['vessel_name_normalized']: groups['by_name_flag'].setdefault(key, []).append(v['id'])

    results = []
    for reason, group, score in [
        ('IMO identique', groups['by_imo'], 1.0),
        ('MMSI identique', groups['by_mmsi'], 0.95),
        ('Nom+Pavillon identiques', groups['by_name_flag'], 0.8)
    ]:
        for key, ids in group.items():
            if len(ids) > 1:
                for i in range(len(ids)):
                    for j in range(i+1, len(ids)):
                        results.append({'vessel1_id':ids[i],'vessel2_id':ids[j],'score':score,'reason':reason})
                        found += 1
    return jsonify({'found': found, 'duplicates': results})


# ── MIGRATION ENTITÉS → MARITIME ─────────────────────────────────────────
@maritime_bp.route('/migrate-from-entities', methods=['POST'])
def migrate_from_entities():
    """Migre les entités MDM vers les tables maritimes avec mapping utilisateur"""
    db = get_db_from_app()
    
    b = request.json or {}
    source_filter = b.get('source_filter', '')
    entity_type   = b.get('entity_type', 'vessel')
    mapping       = b.get('mapping', {})

    # §3.4 — Lire les entités via la connexion partagée (plus de 3ème connexion)
    q = "SELECT id, data, source FROM entities WHERE status='active'"
    params = []
    if source_filter:
        q += " AND source LIKE ?"
        params.append(f"%{source_filter}%")
    raw_entities = db.execute(q, params).fetchall()
    entities = [{'id': r['id'], 'data': r['data'], 'source': r['source']} for r in raw_entities]

    results = {'vessels':0, 'owners':0, 'ports':0, 'port_calls':0, 'skipped':0, 'errors':[]}

    def detect_type(data):
        if not isinstance(data, dict): return None
        keys = set(k.lower() for k in data.keys())
        if any(k in keys for k in ['imo','mmsi','nom_navire','vessel_name','navire','nom_navire']): return 'vessel'
        if any(k in keys for k in ['armateur','owner_name','company_name']): return 'owner'
        if any(k in keys for k in ['locode','un_locode','port_name','nom_port']): return 'port'
        if any(k in keys for k in ['eta','etd','escale','port_call','cargaison']): return 'port_call'
        return None

    def get_val(data, mdm_field, default=''):
        """Récupère la valeur via le mapping utilisateur ou fallback sur le nom du champ directement"""
        if not isinstance(data, dict): return default
        # 1. Via mapping utilisateur
        col = mapping.get(mdm_field)
        if col and col in data:
            v = data[col]
            return str(v).strip() if v is not None else default
        # 2. Fallback : chercher le champ directement (case-insensitive)
        for dk in data.keys():
            if dk.lower() == mdm_field.lower():
                v = data[dk]
                return str(v).strip() if v is not None else default
        return default

    for ent in entities:
        try:
            raw = ent['data']
            if isinstance(raw, str):
                try: data = json.loads(raw)
                except: results['skipped'] += 1; continue
            elif isinstance(raw, dict):
                data = raw
            else:
                results['skipped'] += 1; continue
            if not isinstance(data, dict):
                results['skipped'] += 1; continue
            etype = entity_type if entity_type != 'auto' else detect_type(data)
            # Si le type détecté ne correspond pas au type demandé → ignorer
            if etype != entity_type and entity_type != 'auto':
                results['skipped'] += 1; continue
            if etype is None:
                results['skipped'] += 1; continue

            if etype == 'vessel':
                imo_raw = get_val(data, 'imo_number')
                name    = get_val(data, 'vessel_name')
                if not name: results['skipped'] += 1; continue
                imo_clean, imo_err = validate_imo(imo_raw)
                flag_code = get_val(data, 'flag_code')
                flag_name = FLAGS.get(flag_code, flag_code)
                vtype     = get_val(data, 'vessel_type')
                gt        = get_val(data, 'gross_tonnage')
                armateur  = get_val(data, 'owner_name')
                name_norm = normalize_vessel_name(name)

                # §2.2.5 — Merge intelligent au lieu de INSERT OR IGNORE
                existing = None
                if imo_clean:
                    existing = db.execute("SELECT * FROM maritime_vessels WHERE imo_number=?", (imo_clean,)).fetchone()
                if not existing and name:
                    existing = db.execute("SELECT * FROM maritime_vessels WHERE vessel_name_normalized=? AND status='active'", (name_norm,)).fetchone()

                if existing:
                    # Enrichir le record existant avec les champs manquants
                    updates = []; params = []
                    if not existing['mmsi'] and get_val(data, 'mmsi'):
                        updates.append('mmsi=?'); params.append(get_val(data, 'mmsi'))
                    if not existing['vessel_type'] and vtype:
                        updates.append('vessel_type=?'); params.append(vtype)
                    if not existing['flag_code'] and flag_code:
                        updates.append('flag_code=?'); params.append(flag_code)
                        updates.append('flag_name=?'); params.append(flag_name)
                    if not existing['gross_tonnage'] and gt and gt.replace('.','').isdigit():
                        updates.append('gross_tonnage=?'); params.append(float(gt))
                    if not existing['year_built'] and get_val(data, 'year_built').isdigit():
                        updates.append('year_built=?'); params.append(int(get_val(data, 'year_built')))
                    if not existing['owner_id'] and armateur:
                        ow = db.execute("SELECT id FROM maritime_owners WHERE owner_name=?", (armateur,)).fetchone()
                        if ow: updates.append('owner_id=?'); params.append(ow['id'])
                    if updates:
                        # Tracer la source d'enrichissement
                        old_source = existing['source'] or ''
                        new_source = old_source + (', ' if old_source else '') + (ent['source'] or 'migration')
                        updates.append('source=?'); params.append(new_source)
                        updates.append("updated_at=datetime('now')")
                        params.append(existing['id'])
                        db.execute(f"UPDATE maritime_vessels SET {','.join(updates)} WHERE id=?", params)
                        results['vessels'] += 1
                    else:
                        results['skipped'] += 1
                    continue

                # Nouveau navire
                vid = str(uuid.uuid4())
                confidence = compute_confidence_score({
                    'imo_number': imo_clean, 'mmsi': get_val(data, 'mmsi'),
                    'vessel_name': name, 'vessel_type': vtype, 'flag_code': flag_code,
                    'gross_tonnage': gt if gt and gt.replace('.','').isdigit() else None,
                    'year_built': get_val(data, 'year_built') if get_val(data, 'year_built').isdigit() else None,
                }, [imo_err] if imo_err else [])
                db.execute("""INSERT INTO maritime_vessels
                    (id,vessel_name,vessel_name_normalized,imo_number,flag_code,flag_name,vessel_type,
                     gross_tonnage,status,source,confidence_score,validation_errors,created_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
                    (vid, name, name_norm, imo_clean if imo_clean else None,
                     flag_code, flag_name, vtype,
                     float(gt) if gt and gt.replace('.','').isdigit() else None,
                     'active', ent['source'], confidence,
                     None if not imo_err else json.dumps([imo_err])))
                if armateur:
                    ow = db.execute("SELECT id FROM maritime_owners WHERE owner_name=?", (armateur,)).fetchone()
                    if ow:
                        db.execute("UPDATE maritime_vessels SET owner_id=? WHERE id=?", (ow['id'], vid))
                results['vessels'] += 1

            elif etype == 'port_call':
                vessel_name = get_val(data, 'vessel_name')
                imo         = get_val(data, 'imo_number')
                port_name   = get_val(data, 'port_name')
                locode      = get_val(data, 'un_locode')
                eta         = get_val(data, 'eta')
                etd         = get_val(data, 'etd')
                status_call = get_val(data, 'call_status') or 'Planned'
                cargo       = get_val(data, 'cargo_type')
                qty         = get_val(data, 'cargo_qty')
                agent       = get_val(data, 'agent')
                if not vessel_name and not imo: results['skipped'] += 1; continue
                # Trouver le navire
                vessel = None
                if imo: vessel = db.execute("SELECT id FROM maritime_vessels WHERE imo_number=?", (imo,)).fetchone()
                if not vessel and vessel_name:
                    vessel = db.execute("SELECT id FROM maritime_vessels WHERE vessel_name=?", (vessel_name,)).fetchone()
                # Trouver le port
                port = None
                if locode: port = db.execute("SELECT id FROM maritime_ports WHERE un_locode=?", (locode,)).fetchone()
                if not port and port_name:
                    port = db.execute("SELECT id FROM maritime_ports WHERE port_name=?", (port_name,)).fetchone()
                # Vérifier doublons
                if vessel:
                    existing = db.execute("SELECT id FROM maritime_port_calls WHERE vessel_id=? AND eta=?", (vessel['id'], eta)).fetchone()
                    if existing: results['skipped'] += 1; continue
                cid = str(uuid.uuid4())
                db.execute("""INSERT INTO maritime_port_calls
                    (id,vessel_id,port_id,vessel_name_snapshot,port_name_snapshot,
                     un_locode_snapshot,eta,etd,call_status,cargo_type,
                     cargo_quantity_mt,shipping_agent,source,status,created_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
                    (cid,
                     vessel['id'] if vessel else None,
                     port['id'] if port else None,
                     vessel_name, port_name, locode,
                     eta or None, etd or None,
                     status_call, cargo,
                     float(qty) if qty and qty.replace('.','').isdigit() else None,
                     agent, ent['source'], 'active'))
                results['port_calls'] += 1

            elif etype == 'owner':
                name = get_val(data, 'owner_name')
                if not name: results['skipped'] += 1; continue
                existing = db.execute("SELECT id FROM maritime_owners WHERE owner_name=?", (name,)).fetchone()
                if existing: results['skipped'] += 1; continue
                oid = str(uuid.uuid4())
                db.execute("""INSERT INTO maritime_owners(id,owner_name,country,status,source,created_at)
                    VALUES(?,?,?,?,?,datetime('now'))""",
                    (oid, name, get_val(data,'country'), 'active', ent['source']))
                results['owners'] += 1

            elif etype == 'port':
                name = get_val(data, 'port_name')
                locode = get_val(data, 'un_locode')
                if not name: results['skipped'] += 1; continue
                existing = db.execute("SELECT id FROM maritime_ports WHERE port_name=? OR un_locode=?", (name, locode)).fetchone()
                if existing: results['skipped'] += 1; continue
                pid = str(uuid.uuid4())
                db.execute("""INSERT INTO maritime_ports(id,port_name,un_locode,country_code,status,source,created_at)
                    VALUES(?,?,?,?,?,?,datetime('now'))""",
                    (pid, name, locode, get_val(data,'country_code') or (locode[:2] if locode and len(locode)>=2 else ''), 'active', ent['source']))
                results['ports'] += 1

            else:
                results['skipped'] += 1

        except Exception as e:
            import traceback
            results['errors'].append({'entity_id': ent['id'], 'error': str(e), 'trace': traceback.format_exc().split('\n')[-3]})

    db.commit()
    total = results['vessels'] + results['owners'] + results['ports'] + results['port_calls']
    return jsonify({'success': True, 'total_migrated': total, 'details': results})

# ── GOLDEN RECORD MARITIME ────────────────────────────────────────────────
@maritime_bp.route('/golden-records/vessels', methods=['POST'])
def create_vessel_golden_record():
    """Fusionne plusieurs navires doublons en un Golden Record"""
    db = get_db_from_app()
    b = request.json or {}
    vessel_ids = b.get('vessel_ids', [])
    if len(vessel_ids) < 2:
        return jsonify({'error':'Au moins 2 navires requis'}), 400

    vessels = [db.execute("SELECT * FROM maritime_vessels WHERE id=?", (vid,)).fetchone() for vid in vessel_ids]
    vessels = [v for v in vessels if v]

    # Règle MDM maritime : priorité IMO → source la plus complète → données les plus récentes
    primary = None
    for v in vessels:
        if v['imo_number'] and v['confidence_score'] and (primary is None or v['confidence_score'] > primary['confidence_score']):
            primary = v
    if not primary: primary = vessels[0]

    # Fusionner les données
    merged = {
        'vessel_name':     primary['vessel_name'],
        'imo_number':      primary['imo_number'] or next((v['imo_number'] for v in vessels if v['imo_number']), None),
        'mmsi':            primary['mmsi'] or next((v['mmsi'] for v in vessels if v['mmsi']), None),
        'flag_code':       primary['flag_code'],
        'flag_name':       primary['flag_name'],
        'vessel_type':     primary['vessel_type'],
        'gross_tonnage':   primary['gross_tonnage'] or next((v['gross_tonnage'] for v in vessels if v['gross_tonnage']), None),
        'owner_id':        primary['owner_id'],
        'confidence_score': 1.0,
        'golden_record_id': 'GR-MAR-' + str(uuid.uuid4())[:8].upper(),
        'merged_from':      json.dumps(vessel_ids),
        'source':           'Golden Record — ' + ', '.join(set(v['source'] or '' for v in vessels)),
    }

    # Mettre à jour le navire principal
    db.execute("""UPDATE maritime_vessels SET 
        vessel_name=?, imo_number=?, mmsi=?, flag_code=?, flag_name=?,
        vessel_type=?, gross_tonnage=?, confidence_score=1.0,
        validation_errors=NULL, source=?
        WHERE id=?""",
        (merged['vessel_name'], merged['imo_number'], merged['mmsi'],
         merged['flag_code'], merged['flag_name'], merged['vessel_type'],
         merged['gross_tonnage'], merged['source'], primary['id']))

    # Rediriger les escales des doublons vers le navire principal
    for v in vessels:
        if v['id'] != primary['id']:
            db.execute("UPDATE maritime_port_calls SET vessel_id=? WHERE vessel_id=?", (primary['id'], v['id']))
            db.execute("UPDATE maritime_vessels SET status='merged' WHERE id=?", (v['id'],))

    db.commit()
    return jsonify({'success': True, 'golden_vessel_id': primary['id'], 'golden_record_id': merged['golden_record_id'], 'merged_count': len(vessels)})

# ── DASHBOARD MARITIME ENRICHI ────────────────────────────────────────────
@maritime_bp.route('/dashboard', methods=['GET'])
def maritime_dashboard():
    db = get_db_from_app()
    return jsonify({
        'kpis': {
            'total_vessels':    db.execute("SELECT COUNT(*) FROM maritime_vessels WHERE status='active'").fetchone()[0],
            'total_owners':     db.execute("SELECT COUNT(*) FROM maritime_owners WHERE status='active'").fetchone()[0],
            'total_ports':      db.execute("SELECT COUNT(*) FROM maritime_ports WHERE status='active'").fetchone()[0],
            'total_calls':      db.execute("SELECT COUNT(*) FROM maritime_port_calls WHERE status='active'").fetchone()[0],
            'active_calls':     db.execute("SELECT COUNT(*) FROM maritime_port_calls WHERE call_status IN ('Berthed','At Anchor','In Transit')").fetchone()[0],
            'planned_calls':    db.execute("SELECT COUNT(*) FROM maritime_port_calls WHERE call_status='Planned'").fetchone()[0],
            'low_confidence':   db.execute("SELECT COUNT(*) FROM maritime_vessels WHERE confidence_score < 0.8 AND status='active'").fetchone()[0],
            'duplicates':       db.execute("SELECT COUNT(*) FROM maritime_vessels WHERE status='active' AND imo_number IN (SELECT imo_number FROM maritime_vessels WHERE status='active' GROUP BY imo_number HAVING COUNT(*)>1)").fetchone()[0],
        },
        'vessels_by_type':   [dict(r) for r in db.execute("SELECT vessel_type, COUNT(*) as count FROM maritime_vessels WHERE status='active' GROUP BY vessel_type ORDER BY count DESC").fetchall()],
        'vessels_by_flag':   [dict(r) for r in db.execute("SELECT flag_name, COUNT(*) as count FROM maritime_vessels WHERE status='active' GROUP BY flag_code ORDER BY count DESC LIMIT 8").fetchall()],
        'calls_by_status':   [dict(r) for r in db.execute("SELECT call_status, COUNT(*) as count FROM maritime_port_calls GROUP BY call_status ORDER BY count DESC").fetchall()],
        'top_ports':         [dict(r) for r in db.execute("SELECT port_name_snapshot as port_name, un_locode_snapshot as locode, COUNT(*) as call_count FROM maritime_port_calls GROUP BY port_name_snapshot ORDER BY call_count DESC LIMIT 8").fetchall()],
        'calls_timeline':    [dict(r) for r in db.execute("SELECT DATE(eta) as day, COUNT(*) as count FROM maritime_port_calls WHERE eta >= date('now','-60 days') GROUP BY day ORDER BY day").fetchall()],
        'top_owners':        [dict(r) for r in db.execute("SELECT o.owner_name, COUNT(v.id) as vessel_count FROM maritime_owners o LEFT JOIN maritime_vessels v ON v.owner_id=o.id WHERE o.status='active' GROUP BY o.id ORDER BY vessel_count DESC LIMIT 8").fetchall()],
        'cargo_distribution':[dict(r) for r in db.execute("SELECT cargo_type, COUNT(*) as count FROM maritime_port_calls WHERE cargo_type IS NOT NULL GROUP BY cargo_type ORDER BY count DESC").fetchall()],
        'recent_calls':      [dict(r) for r in db.execute("SELECT vessel_name_snapshot, port_name_snapshot, eta, etd, call_status, cargo_type FROM maritime_port_calls ORDER BY created_at DESC LIMIT 10").fetchall()],
    })


# ── RÉFÉRENTIELS ─────────────────────────────────────────────────────────

@maritime_bp.route('/referentials/flags', methods=['GET'])
def get_flags():
    return jsonify([{'code': k, 'name': v} for k, v in sorted(FLAGS.items(), key=lambda x: x[1])])

@maritime_bp.route('/referentials/vessel-types', methods=['GET'])
def get_vessel_types():
    return jsonify(VESSEL_TYPES)

@maritime_bp.route('/referentials/call-statuses', methods=['GET'])
def get_call_statuses():
    return jsonify(CALL_STATUSES)

@maritime_bp.route('/referentials/cargo-types', methods=['GET'])
def get_cargo_types():
    return jsonify(CARGO_TYPES)

# ── §2.2.3 VALIDATION CROISÉE ESCALES ─────────────────────
@maritime_bp.route('/port-calls/validate', methods=['POST'])
def validate_port_call():
    """Vérifie la cohérence d'une escale avant création"""
    b = request.json or {}
    db = get_db_from_app()
    warnings = []
    vessel_id = b.get('vessel_id')
    eta = b.get('eta')
    etd = b.get('etd')
    port_id = b.get('port_id')

    # Règle 1 : pas d'escale active simultanée
    if vessel_id and eta:
        concurrent = db.execute("""
            SELECT port_name, eta, etd FROM maritime_port_calls
            WHERE vessel_id=? AND call_status IN ('Planned','In Transit','Berthed','At Anchor')
            AND ((eta <= ? AND (etd >= ? OR etd IS NULL))
                 OR (eta >= ? AND eta <= ?))
        """, (vessel_id, eta, eta, eta, etd or '9999-12-31')).fetchall()
        for c in concurrent:
            warnings.append(f"⚠️ Escale simultanée détectée au port {c['port_name']} (ETA: {c['eta']})")

    # Règle 2 : cohérence tirant d'eau vs capacité port
    if vessel_id and port_id:
        vessel = db.execute("SELECT vessel_name, deadweight FROM maritime_vessels WHERE id=?", (vessel_id,)).fetchone()
        port = db.execute("SELECT port_name, max_draft FROM maritime_ports WHERE id=?", (port_id,)).fetchone()
        if vessel and port and vessel['deadweight'] and port['max_draft']:
            # Estimation tirant d'eau ≈ DWT / 10000 (simplifié)
            est_draft = (vessel['deadweight'] or 0) / 10000
            if est_draft > (port['max_draft'] or 99):
                warnings.append(f"⚠️ Tirant d'eau estimé ({est_draft:.1f}m) dépasse capacité port {port['port_name']} ({port['max_draft']}m)")

    return jsonify({'valid': len(warnings) == 0, 'warnings': warnings})

# ── §4.4 AUTOCOMPLETE ─────────────────────────────
@maritime_bp.route('/autocomplete/vessels', methods=['GET'])
def autocomplete_vessels():
    q = request.args.get('q', '').strip()
    if len(q) < 2: return jsonify([])
    db = get_db_from_app()
    rows = db.execute("""
        SELECT id, vessel_name, imo_number, vessel_type, flag_code, gross_tonnage, owner_id
        FROM maritime_vessels WHERE status='active'
        AND (vessel_name LIKE ? OR imo_number LIKE ? OR mmsi LIKE ?)
        LIMIT 10
    """, (f'%{q}%', f'%{q}%', f'%{q}%')).fetchall()
    return jsonify([dict(r) for r in rows])

@maritime_bp.route('/autocomplete/ports', methods=['GET'])
def autocomplete_ports():
    q = request.args.get('q', '').strip()
    if len(q) < 2: return jsonify([])
    db = get_db_from_app()
    rows = db.execute("""
        SELECT id, port_name, un_locode, country_code, max_draft
        FROM maritime_ports WHERE status='active'
        AND (port_name LIKE ? OR un_locode LIKE ?)
        LIMIT 10
    """, (f'%{q}%', f'%{q}%')).fetchall()
    return jsonify([dict(r) for r in rows])

@maritime_bp.route('/autocomplete/owners', methods=['GET'])
def autocomplete_owners():
    q = request.args.get('q', '').strip()
    if len(q) < 2: return jsonify([])
    db = get_db_from_app()
    rows = db.execute("""
        SELECT id, owner_name, country_code, fleet_size
        FROM maritime_owners WHERE status='active'
        AND owner_name LIKE ? LIMIT 10
    """, (f'%{q}%',)).fetchall()
    return jsonify([dict(r) for r in rows])

# ── §4.5 VUE 360° NAVIRE ─────────────────────────────
@maritime_bp.route('/vessels/<vid>/360', methods=['GET'])
def vessel_360(vid):
    """Fiche complète 360° d'un navire"""
    db = get_db_from_app()
    v = db.execute("SELECT * FROM maritime_vessels WHERE id=?", (vid,)).fetchone()
    if not v: return jsonify({'error': 'Navire introuvable'}), 404
    vessel = dict(v)
    for k in ('data', 'validation_errors'):
        if k in vessel:
            try: vessel[k] = json.loads(vessel[k]) if vessel[k] else {}
            except: pass

    # Historique escales
    calls = db.execute(
        "SELECT * FROM maritime_port_calls WHERE vessel_id=? ORDER BY eta DESC LIMIT 50", (vid,)).fetchall()
    vessel['port_calls'] = [dict(c) for c in calls]

    # Armateur
    if vessel.get('owner_id'):
        owner = db.execute("SELECT id,owner_name,country_code FROM maritime_owners WHERE id=?",
                          (vessel['owner_id'],)).fetchone()
        vessel['owner'] = dict(owner) if owner else None

    # Doublons potentiels
    dups = detect_vessel_duplicates(db, {
        'imo_number': vessel.get('imo_number'), 'mmsi': vessel.get('mmsi'),
        'vessel_name': vessel.get('vessel_name'), 'flag_code': vessel.get('flag_code')
    }, exclude_id=vid)
    vessel['potential_duplicates'] = dups

    # Détail score confiance
    errors = vessel.get('validation_errors', [])
    if isinstance(errors, str):
        try: errors = json.loads(errors)
        except: errors = []
    vessel['confidence_detail'] = {
        'imo_valid': bool(vessel.get('imo_number')) and not any('IMO' in str(e) for e in errors),
        'mmsi_valid': bool(vessel.get('mmsi')) and not any('MMSI' in str(e) for e in errors),
        'name_present': bool(vessel.get('vessel_name', '').strip()),
        'type_present': bool(vessel.get('vessel_type')),
        'flag_present': bool(vessel.get('flag_code')),
        'tonnage_present': bool(vessel.get('gross_tonnage')),
        'year_present': bool(vessel.get('year_built')),
        'no_errors': len(errors) == 0,
    }

    # Audit trail
    logs = db.execute(
        "SELECT * FROM audit_log WHERE entity_id=? ORDER BY created_at DESC LIMIT 20", (vid,)).fetchall()
    vessel['audit_trail'] = [dict(l) for l in logs]

    # Commentaires
    comments = db.execute(
        "SELECT * FROM entity_comments WHERE entity_id=? ORDER BY created_at DESC", (vid,)).fetchall()
    vessel['comments'] = [dict(c) for c in comments]

    return jsonify(vessel)

# ── §3.5 FTS5 FULL-TEXT SEARCH ─────────────────────────────
@maritime_bp.route('/search', methods=['GET'])
def fts_search():
    """Recherche full-text sur navires, armateurs, ports"""
    q = request.args.get('q', '').strip()
    if len(q) < 2: return jsonify({'vessels': [], 'owners': [], 'ports': []})
    db = get_db_from_app()
    like = f'%{q}%'
    vessels = db.execute("""
        SELECT id, vessel_name, imo_number, mmsi, vessel_type, flag_code, flag_name, confidence_score
        FROM maritime_vessels WHERE status='active'
        AND (vessel_name LIKE ? OR vessel_name_normalized LIKE ? OR imo_number LIKE ? OR mmsi LIKE ?)
        ORDER BY confidence_score DESC LIMIT 20
    """, (like, like, like, like)).fetchall()
    owners = db.execute("""
        SELECT id, owner_name, country_code, country_name, fleet_size
        FROM maritime_owners WHERE status='active'
        AND (owner_name LIKE ? OR owner_name_normalized LIKE ?)
        ORDER BY owner_name LIMIT 10
    """, (like, like)).fetchall()
    ports = db.execute("""
        SELECT id, port_name, un_locode, country_code, country_name, max_draft
        FROM maritime_ports WHERE status='active'
        AND (port_name LIKE ? OR un_locode LIKE ?)
        ORDER BY port_name LIMIT 10
    """, (like, like)).fetchall()
    return jsonify({
        'vessels': [dict(r) for r in vessels],
        'owners': [dict(r) for r in owners],
        'ports': [dict(r) for r in ports],
        'total': len(vessels) + len(owners) + len(ports),
    })

# ── §4.3 DASHBOARD QUALITÉ MDM ─────────────────────────────
@maritime_bp.route('/quality', methods=['GET'])
def data_quality():
    """Indicateurs de qualité des données maritimes"""
    db = get_db_from_app()
    total_v = db.execute("SELECT COUNT(*) FROM maritime_vessels WHERE status='active'").fetchone()[0] or 1
    # Complétude par champ
    completeness = {}
    for col in ['imo_number','mmsi','vessel_type','flag_code','gross_tonnage','year_built','owner_id']:
        filled = db.execute(f"SELECT COUNT(*) FROM maritime_vessels WHERE status='active' AND {col} IS NOT NULL AND {col} != ''").fetchone()[0]
        completeness[col] = round(filled / total_v * 100, 1)
    # Score moyen
    avg_score = db.execute("SELECT AVG(confidence_score) FROM maritime_vessels WHERE status='active'").fetchone()[0] or 0
    # Navires sans IMO
    no_imo = db.execute("SELECT COUNT(*) FROM maritime_vessels WHERE status='active' AND (imo_number IS NULL OR imo_number='')").fetchone()[0]
    # Navires sans armateur
    no_owner = db.execute("SELECT COUNT(*) FROM maritime_vessels WHERE status='active' AND owner_id IS NULL").fetchone()[0]
    # Ports sans coordonnées
    total_p = db.execute("SELECT COUNT(*) FROM maritime_ports WHERE status='active'").fetchone()[0] or 1
    no_coords = db.execute("SELECT COUNT(*) FROM maritime_ports WHERE status='active' AND (latitude IS NULL OR longitude IS NULL)").fetchone()[0]
    # Armateurs sans flotte
    no_fleet = db.execute("SELECT COUNT(*) FROM maritime_owners o WHERE o.status='active' AND NOT EXISTS (SELECT 1 FROM maritime_vessels v WHERE v.owner_id=o.id AND v.status='active')").fetchone()[0]
    # Escales sans navire lié
    orphan_calls = db.execute("SELECT COUNT(*) FROM maritime_port_calls WHERE status='active' AND vessel_id IS NULL").fetchone()[0]

    return jsonify({
        'completeness': completeness,
        'average_confidence': round(avg_score, 2),
        'alerts': {
            'vessels_no_imo': no_imo,
            'vessels_no_owner': no_owner,
            'ports_no_coordinates': no_coords,
            'owners_no_fleet': no_fleet,
            'orphan_port_calls': orphan_calls,
        },
        'overall_completeness': round(sum(completeness.values()) / len(completeness), 1),
    })
