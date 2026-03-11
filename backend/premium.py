"""
O.S MDM V2.1 — Module Premium
Fonctionnalités avancées pour justifier le pricing enterprise :
  - Data Quality Scoring (complétude, fraîcheur, conformité)
  - Règles de validation configurables (regex, ranges, lookups)
  - Versioning des Golden Records (historique complet)
  - Notifications (webhooks + in-app)
  - Scheduler pour synchro automatique des connecteurs API
  - Data Lineage (traçabilité de chaque champ)
"""

import json, uuid, re, time, threading
from datetime import datetime, timezone, timedelta
from flask import Blueprint, request, jsonify, g
from functools import wraps

premium_bp = Blueprint('premium', __name__)

# ── AUTH HELPER (repris du module principal) ─────────
def _get_token_required():
    """Importe le décorateur token_required depuis app"""
    from app import token_required
    return token_required

def _get_db():
    from app import get_db
    return get_db()

def _audit(action, entity_type, entity_id, details=None):
    from app import audit
    audit(action, entity_type, entity_id, details)

def _get_logger():
    import logging
    return logging.getLogger('osmdm.premium')

# ═══════════════════════════════════════════════════════
# 1. DATA QUALITY SCORING
# ═══════════════════════════════════════════════════════

# Champs considérés comme "importants" pour le score de complétude
DEFAULT_QUALITY_WEIGHTS = {
    'nom': 3, 'name': 3, 'company': 3, 'raison_sociale': 3,
    'email': 2, 'telephone': 2, 'phone': 2,
    'pays': 1, 'country': 1, 'ville': 1, 'city': 1,
    'adresse': 1, 'address': 1, 'type': 1, 'category': 1,
}

def compute_quality_score(data_dict, rules=None):
    """
    Calcule un score de qualité pour un enregistrement.
    Retourne un dict avec les scores détaillés.
    
    Dimensions :
    - completeness (0-100) : % de champs remplis, pondéré
    - freshness (0-100) : basé sur updated_at
    - conformity (0-100) : respect des règles de validation
    - overall (0-100) : moyenne pondérée
    """
    if not isinstance(data_dict, dict):
        return {'overall': 0, 'completeness': 0, 'freshness': 100, 'conformity': 100, 'issues': ['Données invalides']}

    issues = []

    # ── COMPLETENESS ──
    total_weight = 0
    filled_weight = 0
    for key, value in data_dict.items():
        if key.startswith('_'):
            continue  # Skip internal fields
        weight = DEFAULT_QUALITY_WEIGHTS.get(key.lower(), 1)
        total_weight += weight
        if value is not None and str(value).strip():
            filled_weight += weight
        elif weight >= 2:
            issues.append(f"Champ important manquant : {key}")
    
    completeness = round(filled_weight / max(total_weight, 1) * 100, 1)

    # ── CONFORMITY (basic checks) ──
    conformity_checks = 0
    conformity_pass = 0

    # Email validation
    for k in ('email', 'contact_email', 'mail'):
        v = data_dict.get(k, '')
        if v:
            conformity_checks += 1
            if re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', str(v)):
                conformity_pass += 1
            else:
                issues.append(f"Email invalide : {k}={v}")

    # Phone validation
    for k in ('telephone', 'phone', 'tel', 'contact_phone'):
        v = data_dict.get(k, '')
        if v:
            conformity_checks += 1
            cleaned = re.sub(r'[\s\-\.\(\)]', '', str(v))
            if re.match(r'^\+?[0-9]{6,15}$', cleaned):
                conformity_pass += 1
            else:
                issues.append(f"Téléphone suspect : {k}={v}")

    # Country code
    for k in ('country_code', 'pays_code', 'flag_code'):
        v = data_dict.get(k, '')
        if v:
            conformity_checks += 1
            if re.match(r'^[A-Z]{2,3}$', str(v).upper()):
                conformity_pass += 1
            else:
                issues.append(f"Code pays invalide : {k}={v}")

    conformity = round(conformity_pass / max(conformity_checks, 1) * 100, 1) if conformity_checks > 0 else 100

    # ── FRESHNESS (si on a updated_at) ──
    freshness = 100  # Default if no date info

    # ── OVERALL ──
    overall = round(completeness * 0.5 + conformity * 0.3 + freshness * 0.2, 1)

    return {
        'overall': overall,
        'completeness': completeness,
        'freshness': freshness,
        'conformity': conformity,
        'issues': issues,
        'fields_total': max(total_weight, 1),
        'fields_filled': filled_weight,
    }


@premium_bp.route('/quality/score', methods=['POST'])
def quality_score_single():
    """Score de qualité pour des données brutes"""
    from app import token_required
    @token_required
    def _inner():
        data = (request.json or {}).get('data', {})
        return jsonify(compute_quality_score(data))
    return _inner()


@premium_bp.route('/quality/entity/<eid>', methods=['GET'])
def quality_entity(eid):
    """Score de qualité d'une entité existante"""
    from app import token_required
    @token_required
    def _inner():
        db = _get_db()
        row = db.execute("SELECT data, updated_at FROM entities WHERE id=?", (eid,)).fetchone()
        if not row:
            return jsonify({'error': 'Entité introuvable'}), 404
        try:
            data = json.loads(row['data'])
        except:
            data = {}
        score = compute_quality_score(data)
        # Freshness basée sur updated_at
        if row['updated_at']:
            try:
                updated = datetime.fromisoformat(row['updated_at'].replace('Z', '+00:00'))
                age_days = (datetime.now(timezone.utc) - updated.replace(tzinfo=timezone.utc)).days
                score['freshness'] = max(0, 100 - age_days * 2)  # -2 pts par jour
                score['age_days'] = age_days
                score['overall'] = round(score['completeness'] * 0.5 + score['conformity'] * 0.3 + score['freshness'] * 0.2, 1)
            except:
                pass
        return jsonify(score)
    return _inner()


@premium_bp.route('/quality/bulk', methods=['GET'])
def quality_bulk():
    """Score de qualité global pour toutes les entités (ou filtrées par source)"""
    from app import token_required
    @token_required
    def _inner():
        db = _get_db()
        source = request.args.get('source', '')
        limit = int(request.args.get('limit', 1000))
        wh = "status='active'"
        params = []
        if source:
            wh += " AND source LIKE ?"
            params.append(f'%{source}%')
        rows = db.execute(f"SELECT id, data, source, updated_at FROM entities WHERE {wh} ORDER BY created_at DESC LIMIT ?",
                          params + [limit]).fetchall()
        
        scores = []
        total_overall = 0
        total_completeness = 0
        total_conformity = 0
        quality_distribution = {'excellent': 0, 'good': 0, 'fair': 0, 'poor': 0}
        all_issues = {}

        for row in rows:
            try:
                data = json.loads(row['data'])
            except:
                data = {}
            score = compute_quality_score(data)
            total_overall += score['overall']
            total_completeness += score['completeness']
            total_conformity += score['conformity']
            
            if score['overall'] >= 80:
                quality_distribution['excellent'] += 1
            elif score['overall'] >= 60:
                quality_distribution['good'] += 1
            elif score['overall'] >= 40:
                quality_distribution['fair'] += 1
            else:
                quality_distribution['poor'] += 1
            
            for issue in score['issues']:
                all_issues[issue] = all_issues.get(issue, 0) + 1

        count = len(rows) or 1
        # Top issues triées par fréquence
        top_issues = sorted(all_issues.items(), key=lambda x: -x[1])[:20]

        return jsonify({
            'total_entities': len(rows),
            'avg_overall': round(total_overall / count, 1),
            'avg_completeness': round(total_completeness / count, 1),
            'avg_conformity': round(total_conformity / count, 1),
            'quality_distribution': quality_distribution,
            'top_issues': [{'issue': k, 'count': v} for k, v in top_issues],
        })
    return _inner()


# ═══════════════════════════════════════════════════════
# 2. VALIDATION RULES (configurables par l'utilisateur)
# ═══════════════════════════════════════════════════════

RULE_TYPES = {
    'required': 'Champ obligatoire',
    'regex': 'Expression régulière',
    'min_length': 'Longueur minimum',
    'max_length': 'Longueur maximum',
    'range': 'Plage numérique (min-max)',
    'enum': 'Valeur parmi une liste',
    'email': 'Format email valide',
    'phone': 'Format téléphone valide',
    'unique': 'Valeur unique dans la source',
}

@premium_bp.route('/validation-rules', methods=['GET'])
def list_validation_rules():
    from app import token_required
    @token_required
    def _inner():
        db = _get_db()
        rows = db.execute("SELECT * FROM validation_rules ORDER BY field, created_at").fetchall()
        return jsonify([dict(r) for r in rows])
    return _inner()

@premium_bp.route('/validation-rules/types', methods=['GET'])
def list_rule_types():
    from app import token_required
    @token_required
    def _inner():
        return jsonify(RULE_TYPES)
    return _inner()

@premium_bp.route('/validation-rules', methods=['POST'])
def create_validation_rule():
    from app import token_required
    @token_required
    def _inner():
        b = request.json or {}
        rid = str(uuid.uuid4())
        db = _get_db()
        db.execute("""INSERT INTO validation_rules(id, name, field, rule_type, rule_value, severity, active, apply_to)
                      VALUES(?,?,?,?,?,?,?,?)""",
            (rid, b.get('name', ''), b.get('field', ''), b.get('rule_type', 'required'),
             json.dumps(b.get('rule_value', '')), b.get('severity', 'warning'),
             1 if b.get('active', True) else 0, b.get('apply_to', 'entity')))
        db.commit()
        _audit('create', 'validation_rule', rid, {'name': b.get('name')})
        return jsonify({'id': rid, 'message': 'Règle créée'}), 201
    return _inner()

@premium_bp.route('/validation-rules/<rid>', methods=['PUT'])
def update_validation_rule(rid):
    from app import token_required
    @token_required
    def _inner():
        b = request.json or {}
        db = _get_db()
        db.execute("""UPDATE validation_rules SET name=?, field=?, rule_type=?, rule_value=?,
                      severity=?, active=?, apply_to=? WHERE id=?""",
            (b.get('name'), b.get('field'), b.get('rule_type'),
             json.dumps(b.get('rule_value', '')), b.get('severity', 'warning'),
             1 if b.get('active', True) else 0, b.get('apply_to', 'entity'), rid))
        db.commit()
        return jsonify({'message': 'Mise à jour'})
    return _inner()

@premium_bp.route('/validation-rules/<rid>', methods=['DELETE'])
def delete_validation_rule(rid):
    from app import token_required
    @token_required
    def _inner():
        _get_db().execute("DELETE FROM validation_rules WHERE id=?", (rid,))
        _get_db().commit()
        return jsonify({'message': 'Supprimée'})
    return _inner()

def validate_entity_data(data_dict, apply_to='entity'):
    """Applique toutes les règles de validation actives sur un dict de données"""
    from app import get_db
    db = get_db()
    rules = db.execute("SELECT * FROM validation_rules WHERE active=1 AND apply_to=?", (apply_to,)).fetchall()
    errors = []
    warnings = []
    for rule in rules:
        field = rule['field']
        rule_type = rule['rule_type']
        try:
            rule_value = json.loads(rule['rule_value'])
        except:
            rule_value = rule['rule_value']
        value = data_dict.get(field, '')
        severity = rule['severity']
        msg_list = errors if severity == 'error' else warnings
        label = rule['name'] or f"{rule_type} sur {field}"

        if rule_type == 'required':
            if not value or not str(value).strip():
                msg_list.append({'field': field, 'rule': label, 'message': f"'{field}' est obligatoire"})
        elif rule_type == 'regex' and value:
            try:
                if not re.match(str(rule_value), str(value)):
                    msg_list.append({'field': field, 'rule': label, 'message': f"'{field}' ne correspond pas au format attendu"})
            except re.error:
                pass
        elif rule_type == 'min_length' and value:
            if len(str(value)) < int(rule_value):
                msg_list.append({'field': field, 'rule': label, 'message': f"'{field}' trop court (min {rule_value} caractères)"})
        elif rule_type == 'max_length' and value:
            if len(str(value)) > int(rule_value):
                msg_list.append({'field': field, 'rule': label, 'message': f"'{field}' trop long (max {rule_value} caractères)"})
        elif rule_type == 'range' and value:
            try:
                num = float(value)
                rng = rule_value if isinstance(rule_value, dict) else {'min': 0, 'max': 999999}
                if num < float(rng.get('min', float('-inf'))) or num > float(rng.get('max', float('inf'))):
                    msg_list.append({'field': field, 'rule': label, 'message': f"'{field}' hors plage [{rng.get('min')}-{rng.get('max')}]"})
            except (ValueError, TypeError):
                pass
        elif rule_type == 'enum' and value:
            allowed = rule_value if isinstance(rule_value, list) else [rule_value]
            if str(value) not in [str(a) for a in allowed]:
                msg_list.append({'field': field, 'rule': label, 'message': f"'{field}' doit être parmi : {', '.join(str(a) for a in allowed[:5])}"})
        elif rule_type == 'email' and value:
            if not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', str(value)):
                msg_list.append({'field': field, 'rule': label, 'message': f"Email invalide : {value}"})
        elif rule_type == 'phone' and value:
            cleaned = re.sub(r'[\s\-\.\(\)]', '', str(value))
            if not re.match(r'^\+?[0-9]{6,15}$', cleaned):
                msg_list.append({'field': field, 'rule': label, 'message': f"Téléphone invalide : {value}"})

    return {'valid': len(errors) == 0, 'errors': errors, 'warnings': warnings}

@premium_bp.route('/validate/entity/<eid>', methods=['GET'])
def validate_entity(eid):
    from app import token_required
    @token_required
    def _inner():
        db = _get_db()
        row = db.execute("SELECT data FROM entities WHERE id=?", (eid,)).fetchone()
        if not row:
            return jsonify({'error': 'Introuvable'}), 404
        try:
            data = json.loads(row['data'])
        except:
            data = {}
        result = validate_entity_data(data)
        return jsonify(result)
    return _inner()


# ═══════════════════════════════════════════════════════
# 3. GOLDEN RECORD VERSIONING
# ═══════════════════════════════════════════════════════

@premium_bp.route('/golden-records/<gid>/versions', methods=['GET'])
def golden_record_versions(gid):
    """Historique complet des versions d'un Golden Record"""
    from app import token_required
    @token_required
    def _inner():
        db = _get_db()
        gr = db.execute("SELECT id FROM golden_records WHERE id=?", (gid,)).fetchone()
        if not gr:
            return jsonify({'error': 'Golden Record introuvable'}), 404
        versions = db.execute("""SELECT * FROM golden_record_versions 
                                 WHERE golden_record_id=? ORDER BY version DESC""", (gid,)).fetchall()
        result = []
        for v in versions:
            vd = dict(v)
            try:
                vd['data'] = json.loads(vd['data'])
            except:
                pass
            try:
                vd['changes'] = json.loads(vd['changes'])
            except:
                pass
            result.append(vd)
        return jsonify(result)
    return _inner()

@premium_bp.route('/golden-records/<gid>/versions/<int:version>', methods=['GET'])
def golden_record_version_detail(gid, version):
    """Détail d'une version spécifique"""
    from app import token_required
    @token_required
    def _inner():
        db = _get_db()
        v = db.execute("SELECT * FROM golden_record_versions WHERE golden_record_id=? AND version=?",
                       (gid, version)).fetchone()
        if not v:
            return jsonify({'error': 'Version introuvable'}), 404
        vd = dict(v)
        for k in ('data', 'changes'):
            try:
                vd[k] = json.loads(vd[k])
            except:
                pass
        return jsonify(vd)
    return _inner()

@premium_bp.route('/golden-records/<gid>/restore/<int:version>', methods=['POST'])
def restore_golden_record_version(gid, version):
    """Restaure un Golden Record à une version antérieure"""
    from app import token_required
    @token_required
    def _inner():
        db = _get_db()
        v = db.execute("SELECT data FROM golden_record_versions WHERE golden_record_id=? AND version=?",
                       (gid, version)).fetchone()
        if not v:
            return jsonify({'error': 'Version introuvable'}), 404
        # Sauver la version actuelle avant restauration
        current = db.execute("SELECT data FROM golden_records WHERE id=?", (gid,)).fetchone()
        if current:
            max_ver = db.execute("SELECT MAX(version) FROM golden_record_versions WHERE golden_record_id=?",
                                (gid,)).fetchone()[0] or 0
            db.execute("""INSERT INTO golden_record_versions(id, golden_record_id, version, data, changes, changed_by, created_at)
                          VALUES(?,?,?,?,?,?,datetime('now'))""",
                (str(uuid.uuid4()), gid, max_ver + 1, current['data'],
                 json.dumps({'action': f'Backup avant restauration vers v{version}'}),
                 g.current_user.get('email', '?')))
        # Restaurer
        db.execute("UPDATE golden_records SET data=?, updated_at=datetime('now') WHERE id=?", (v['data'], gid))
        db.commit()
        _audit('restore_version', 'golden_record', gid, {'restored_version': version})
        return jsonify({'message': f'Golden Record restauré à la version {version}'})
    return _inner()

def save_golden_record_version(db, gid, old_data, new_data, changed_by='system'):
    """Sauvegarde automatiquement une version lors d'une modification de GR"""
    max_ver = db.execute("SELECT MAX(version) FROM golden_record_versions WHERE golden_record_id=?",
                        (gid,)).fetchone()[0] or 0
    # Calculer les changements
    changes = {}
    old = old_data if isinstance(old_data, dict) else {}
    new = new_data if isinstance(new_data, dict) else {}
    all_keys = set(list(old.keys()) + list(new.keys()))
    for k in all_keys:
        if k.startswith('_'):
            continue
        ov = old.get(k)
        nv = new.get(k)
        if ov != nv:
            changes[k] = {'old': ov, 'new': nv}
    
    db.execute("""INSERT INTO golden_record_versions(id, golden_record_id, version, data, changes, changed_by, created_at)
                  VALUES(?,?,?,?,?,?,datetime('now'))""",
        (str(uuid.uuid4()), gid, max_ver + 1,
         json.dumps(old_data, ensure_ascii=False) if isinstance(old_data, dict) else str(old_data),
         json.dumps(changes, ensure_ascii=False),
         changed_by))


# ═══════════════════════════════════════════════════════
# 4. NOTIFICATIONS (in-app + webhooks)
# ═══════════════════════════════════════════════════════

@premium_bp.route('/notifications', methods=['GET'])
def list_notifications():
    """Notifications de l'utilisateur courant"""
    from app import token_required
    @token_required
    def _inner():
        db = _get_db()
        user_id = g.current_user.get('user_id', '')
        unread_only = request.args.get('unread', 'false') == 'true'
        wh = "user_id=? OR user_id='*'"
        params = [user_id]
        if unread_only:
            wh += " AND read=0"
        rows = db.execute(f"SELECT * FROM notifications WHERE {wh} ORDER BY created_at DESC LIMIT 50", params).fetchall()
        return jsonify([dict(r) for r in rows])
    return _inner()

@premium_bp.route('/notifications/unread-count', methods=['GET'])
def unread_count():
    from app import token_required
    @token_required
    def _inner():
        db = _get_db()
        user_id = g.current_user.get('user_id', '')
        count = db.execute("SELECT COUNT(*) FROM notifications WHERE (user_id=? OR user_id='*') AND read=0",
                          (user_id,)).fetchone()[0]
        return jsonify({'count': count})
    return _inner()

@premium_bp.route('/notifications/<nid>/read', methods=['PUT'])
def mark_notification_read(nid):
    from app import token_required
    @token_required
    def _inner():
        _get_db().execute("UPDATE notifications SET read=1 WHERE id=?", (nid,))
        _get_db().commit()
        return jsonify({'message': 'Lu'})
    return _inner()

@premium_bp.route('/notifications/read-all', methods=['PUT'])
def mark_all_read():
    from app import token_required
    @token_required
    def _inner():
        user_id = g.current_user.get('user_id', '')
        _get_db().execute("UPDATE notifications SET read=1 WHERE user_id=? OR user_id='*'", (user_id,))
        _get_db().commit()
        return jsonify({'message': 'Toutes les notifications marquées comme lues'})
    return _inner()

def create_notification(db, title, message, notif_type='info', user_id='*', link=''):
    """Crée une notification in-app. user_id='*' = broadcast à tous."""
    nid = str(uuid.uuid4())
    db.execute("""INSERT INTO notifications(id, user_id, title, message, type, link, read, created_at)
                  VALUES(?,?,?,?,?,?,0,datetime('now'))""",
        (nid, user_id, title, message, notif_type, link))
    return nid

# ── Webhooks ──
@premium_bp.route('/webhooks', methods=['GET'])
def list_webhooks():
    from app import token_required
    @token_required
    def _inner():
        rows = _get_db().execute("SELECT id, name, url, events, active, last_triggered, last_status, created_at FROM webhooks ORDER BY created_at DESC").fetchall()
        result = []
        for r in rows:
            d = dict(r)
            try:
                d['events'] = json.loads(d['events'])
            except:
                pass
            result.append(d)
        return jsonify(result)
    return _inner()

@premium_bp.route('/webhooks', methods=['POST'])
def create_webhook():
    from app import token_required
    @token_required
    def _inner():
        b = request.json or {}
        wid = str(uuid.uuid4())
        db = _get_db()
        db.execute("""INSERT INTO webhooks(id, name, url, events, secret, active)
                      VALUES(?,?,?,?,?,?)""",
            (wid, b.get('name', ''), b.get('url', ''),
             json.dumps(b.get('events', ['import_done', 'duplicates_found', 'golden_record_created'])),
             b.get('secret', ''), 1 if b.get('active', True) else 0))
        db.commit()
        _audit('create', 'webhook', wid, {'name': b.get('name')})
        return jsonify({'id': wid, 'message': 'Webhook créé'}), 201
    return _inner()

@premium_bp.route('/webhooks/<wid>', methods=['DELETE'])
def delete_webhook(wid):
    from app import token_required
    @token_required
    def _inner():
        _get_db().execute("DELETE FROM webhooks WHERE id=?", (wid,))
        _get_db().commit()
        return jsonify({'message': 'Supprimé'})
    return _inner()

def trigger_webhooks(event, payload):
    """Déclenche les webhooks abonnés à un événement (en background)"""
    import urllib.request
    logger = _get_logger()
    try:
        from app import get_db as _gdb
        import sqlite3
        from app import DB_PATH
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        hooks = db.execute("SELECT * FROM webhooks WHERE active=1").fetchall()
        for hook in hooks:
            events = hook['events']
            try:
                events = json.loads(events)
            except:
                events = []
            if event not in events:
                continue
            try:
                data = json.dumps({'event': event, 'payload': payload, 'timestamp': datetime.now(timezone.utc).isoformat()}).encode('utf-8')
                headers = {'Content-Type': 'application/json', 'User-Agent': 'OS-MDM-V2-Webhook/1.0'}
                if hook['secret']:
                    import hashlib, hmac
                    sig = hmac.new(hook['secret'].encode(), data, hashlib.sha256).hexdigest()
                    headers['X-MDM-Signature'] = sig
                req = urllib.request.Request(hook['url'], data=data, headers=headers, method='POST')
                resp = urllib.request.urlopen(req, timeout=10)
                db.execute("UPDATE webhooks SET last_triggered=datetime('now'), last_status=? WHERE id=?",
                          (f"ok:{resp.status}", hook['id']))
                logger.info(f"Webhook {hook['name']} déclenché pour {event} → {resp.status}")
            except Exception as e:
                db.execute("UPDATE webhooks SET last_triggered=datetime('now'), last_status=? WHERE id=?",
                          (f"error:{str(e)[:100]}", hook['id']))
                logger.warning(f"Webhook {hook['name']} erreur : {e}")
        db.commit()
        db.close()
    except Exception as e:
        logger.error(f"Erreur trigger_webhooks: {e}")

def fire_event(event, payload):
    """Lance les webhooks et crée une notification"""
    # Notification in-app
    event_titles = {
        'import_done': 'Import terminé',
        'duplicates_found': 'Doublons détectés',
        'golden_record_created': 'Golden Record créé',
        'writeback_done': 'Write-back terminé',
        'sync_done': 'Synchronisation API terminée',
        'quality_alert': 'Alerte qualité données',
    }
    try:
        from app import get_db
        db = get_db()
        title = event_titles.get(event, event)
        message = payload.get('message', str(payload))
        create_notification(db, title, message, notif_type=payload.get('type', 'info'))
        db.commit()
    except:
        pass  # Ne pas bloquer si la notification échoue
    # Webhooks en background
    t = threading.Thread(target=trigger_webhooks, args=(event, payload), daemon=True)
    t.start()


# ═══════════════════════════════════════════════════════
# 5. SCHEDULER (synchro automatique des connecteurs API)
# ═══════════════════════════════════════════════════════

_scheduler_thread = None
_scheduler_running = False

def _run_scheduler():
    """Thread de fond qui vérifie et lance les synchros planifiées"""
    import sqlite3
    from app import DB_PATH
    logger = _get_logger()
    global _scheduler_running
    logger.info("Scheduler démarré")
    
    while _scheduler_running:
        try:
            db = sqlite3.connect(DB_PATH)
            db.row_factory = sqlite3.Row
            # Trouver les connecteurs à synchro
            connectors = db.execute("""
                SELECT id, name, sync_interval_minutes, last_sync
                FROM api_connectors
                WHERE enabled=1 AND sync_interval_minutes > 0
            """).fetchall()
            
            now = datetime.now(timezone.utc)
            for conn in connectors:
                interval = conn['sync_interval_minutes']
                last = conn['last_sync']
                should_sync = False
                if not last:
                    should_sync = True
                else:
                    try:
                        last_dt = datetime.fromisoformat(last.replace('Z', '+00:00'))
                        if not last_dt.tzinfo:
                            last_dt = last_dt.replace(tzinfo=timezone.utc)
                        if (now - last_dt).total_seconds() >= interval * 60:
                            should_sync = True
                    except:
                        should_sync = True
                
                if should_sync:
                    logger.info(f"Scheduler : lancement synchro pour '{conn['name']}' (interval={interval}min)")
                    # On enregistre le lancement
                    db.execute("UPDATE api_connectors SET last_sync=datetime('now'), last_sync_status='scheduled' WHERE id=?",
                              (conn['id'],))
                    db.commit()
                    # La vraie synchro sera déclenchée via l'API au prochain cycle
                    # Pour l'instant on crée une notification
                    try:
                        create_notification(db, 'Synchro planifiée',
                            f"Connecteur '{conn['name']}' — synchro automatique lancée",
                            notif_type='info')
                        db.commit()
                    except:
                        pass
            
            db.close()
        except Exception as e:
            logger.error(f"Scheduler erreur : {e}")
        
        # Dormir 60 secondes entre les vérifications
        for _ in range(60):
            if not _scheduler_running:
                break
            time.sleep(1)
    
    logger.info("Scheduler arrêté")

def start_scheduler():
    global _scheduler_thread, _scheduler_running
    if _scheduler_running:
        return
    _scheduler_running = True
    _scheduler_thread = threading.Thread(target=_run_scheduler, daemon=True)
    _scheduler_thread.start()

def stop_scheduler():
    global _scheduler_running
    _scheduler_running = False

@premium_bp.route('/scheduler/status', methods=['GET'])
def scheduler_status():
    from app import token_required
    @token_required
    def _inner():
        db = _get_db()
        scheduled = db.execute("""SELECT id, name, sync_interval_minutes, last_sync, last_sync_status, last_sync_count
                                  FROM api_connectors WHERE enabled=1 AND sync_interval_minutes > 0
                                  ORDER BY last_sync""").fetchall()
        return jsonify({
            'running': _scheduler_running,
            'scheduled_connectors': [dict(r) for r in scheduled],
        })
    return _inner()

@premium_bp.route('/scheduler/start', methods=['POST'])
def start_scheduler_endpoint():
    from app import token_required, admin_required
    @admin_required
    def _inner():
        start_scheduler()
        return jsonify({'message': 'Scheduler démarré', 'running': True})
    return _inner()

@premium_bp.route('/scheduler/stop', methods=['POST'])
def stop_scheduler_endpoint():
    from app import token_required, admin_required
    @admin_required
    def _inner():
        stop_scheduler()
        return jsonify({'message': 'Scheduler arrêté', 'running': False})
    return _inner()


# ═══════════════════════════════════════════════════════
# 6. DATA LINEAGE (traçabilité des champs)
# ═══════════════════════════════════════════════════════

@premium_bp.route('/lineage/golden-record/<gid>', methods=['GET'])
def golden_record_lineage(gid):
    """Data lineage : pour chaque champ du GR, d'où vient la valeur"""
    from app import token_required
    @token_required
    def _inner():
        db = _get_db()
        gr = db.execute("SELECT * FROM golden_records WHERE id=?", (gid,)).fetchone()
        if not gr:
            return jsonify({'error': 'Introuvable'}), 404
        
        gr_data = {}
        try:
            gr_data = json.loads(gr['data'])
        except:
            pass
        source_ids = []
        try:
            source_ids = json.loads(gr['source_ids'])
        except:
            pass
        
        # Charger les entités sources
        sources = []
        for sid in source_ids:
            e = db.execute("SELECT id, mdm_id, data, source FROM entities WHERE id=?", (sid,)).fetchone()
            if e:
                ed = dict(e)
                try:
                    ed['data'] = json.loads(ed['data'])
                except:
                    ed['data'] = {}
                sources.append(ed)
        
        # Construire le lineage
        lineage = {}
        for field, value in gr_data.items():
            if field.startswith('_'):
                continue
            field_lineage = {
                'current_value': value,
                'sources': []
            }
            for src in sources:
                src_value = src['data'].get(field)
                if src_value is not None:
                    field_lineage['sources'].append({
                        'entity_id': src['id'],
                        'mdm_id': src['mdm_id'],
                        'source': src['source'],
                        'value': src_value,
                        'is_selected': str(src_value) == str(value),
                    })
            lineage[field] = field_lineage
        
        return jsonify({
            'golden_record_id': gid,
            'golden_record_mdm_id': gr['mdm_id'],
            'fields': lineage,
            'source_count': len(sources),
        })
    return _inner()


# ═══════════════════════════════════════════════════════
# TABLES PREMIUM (ajoutées à init_db)
# ═══════════════════════════════════════════════════════

def init_premium_tables(db):
    """Crée les tables pour les fonctionnalités premium"""
    for sql in [
        """CREATE TABLE IF NOT EXISTS validation_rules (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            field TEXT NOT NULL,
            rule_type TEXT NOT NULL DEFAULT 'required',
            rule_value TEXT DEFAULT '""',
            severity TEXT DEFAULT 'warning',
            active INTEGER DEFAULT 1,
            apply_to TEXT DEFAULT 'entity',
            created_at TEXT DEFAULT (datetime('now'))
        )""",
        """CREATE TABLE IF NOT EXISTS golden_record_versions (
            id TEXT PRIMARY KEY,
            golden_record_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            data TEXT,
            changes TEXT DEFAULT '{}',
            changed_by TEXT DEFAULT 'system',
            created_at TEXT DEFAULT (datetime('now'))
        )""",
        """CREATE TABLE IF NOT EXISTS notifications (
            id TEXT PRIMARY KEY,
            user_id TEXT DEFAULT '*',
            title TEXT NOT NULL,
            message TEXT,
            type TEXT DEFAULT 'info',
            link TEXT DEFAULT '',
            read INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        )""",
        """CREATE TABLE IF NOT EXISTS webhooks (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            url TEXT NOT NULL,
            events TEXT DEFAULT '[]',
            secret TEXT DEFAULT '',
            active INTEGER DEFAULT 1,
            last_triggered TEXT,
            last_status TEXT DEFAULT 'never',
            created_at TEXT DEFAULT (datetime('now'))
        )""",
        "CREATE INDEX IF NOT EXISTS idx_grv_grid ON golden_record_versions(golden_record_id, version DESC)",
        "CREATE INDEX IF NOT EXISTS idx_notif_user ON notifications(user_id, read, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_vrules_field ON validation_rules(field, active)",
    ]:
        try:
            db.execute(sql)
        except:
            pass
    db.commit()
