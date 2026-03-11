"""
O.S MDM V2 — Tests automatisés (pytest)
Couvre : Auth, Import, Entités, Doublons, Golden Records, Audit, Export, Health

Lancer : pytest tests/ -v
Lancer avec couverture : pytest tests/ -v --cov=backend --cov-report=html
"""

import os, sys, json, uuid, tempfile, io

# Ajouter le backend au path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend'))

import pytest

# ── FIXTURES ────────────────────────────────────────

@pytest.fixture(scope='function')
def app():
    """Crée une app Flask de test avec une DB temporaire"""
    # DB temporaire pour chaque test
    db_fd, db_path = tempfile.mkstemp(suffix='.db')
    
    # Patcher le DB_PATH avant l'import
    os.environ['MDM_SECRET'] = 'test-secret-key-for-pytest-only-32chars'
    os.environ['MDM_CORS_ORIGINS'] = '*'
    
    import importlib
    # On doit importer après avoir posé l'env
    if 'app' in sys.modules:
        # Recharger le module pour utiliser la nouvelle DB
        import app as app_module
        app_module.DB_PATH = db_path
        app_module.app.config['DB_PATH'] = db_path
        app_module.init_db()
        flask_app = app_module.app
    else:
        import app as app_module
        app_module.DB_PATH = db_path
        app_module.app.config['DB_PATH'] = db_path
        app_module.init_db()
        flask_app = app_module.app
    
    flask_app.config['TESTING'] = True
    
    yield flask_app
    
    # Cleanup
    os.close(db_fd)
    os.unlink(db_path)


@pytest.fixture
def client(app):
    """Client HTTP de test"""
    return app.test_client()


@pytest.fixture
def auth_token(client):
    """Obtient un token JWT valide pour l'admin"""
    resp = client.post('/api/auth/login', 
        json={'email': 'admin@osmdm.local', 'password': 'admin123'},
        content_type='application/json')
    data = resp.get_json()
    assert 'token' in data, f"Login échoué: {data}"
    return data['token']


@pytest.fixture
def auth_headers(auth_token):
    """Headers avec Authorization Bearer"""
    return {
        'Authorization': f'Bearer {auth_token}',
        'Content-Type': 'application/json'
    }


# ═══════════════════════════════════════════════════
# 1. TESTS HEALTH CHECK
# ═══════════════════════════════════════════════════

class TestHealth:
    def test_health_endpoint(self, client):
        """Health check retourne 200 et status ok"""
        resp = client.get('/api/health')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['status'] == 'ok'
        assert 'version' in data
        assert data['database'] == 'ok'

    def test_health_no_auth_required(self, client):
        """Health check fonctionne sans authentification"""
        resp = client.get('/api/health')
        assert resp.status_code == 200


# ═══════════════════════════════════════════════════
# 2. TESTS AUTHENTIFICATION
# ═══════════════════════════════════════════════════

class TestAuth:
    def test_login_success(self, client):
        """Login avec bons identifiants retourne un token"""
        resp = client.post('/api/auth/login',
            json={'email': 'admin@osmdm.local', 'password': 'admin123'})
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'token' in data
        assert data['user']['email'] == 'admin@osmdm.local'
        assert data['user']['role'] == 'admin'

    def test_login_bad_password(self, client):
        """Login avec mauvais mot de passe retourne 401"""
        resp = client.post('/api/auth/login',
            json={'email': 'admin@osmdm.local', 'password': 'wrongpassword'})
        assert resp.status_code == 401

    def test_login_bad_email(self, client):
        """Login avec email inconnu retourne 401"""
        resp = client.post('/api/auth/login',
            json={'email': 'unknown@test.com', 'password': 'admin123'})
        assert resp.status_code == 401

    def test_login_empty_fields(self, client):
        """Login avec champs vides retourne 400"""
        resp = client.post('/api/auth/login', json={'email': '', 'password': ''})
        assert resp.status_code == 400

    def test_protected_route_no_token(self, client):
        """Route protégée sans token retourne 401"""
        resp = client.get('/api/dashboard/stats')
        assert resp.status_code == 401

    def test_protected_route_invalid_token(self, client):
        """Route protégée avec token invalide retourne 401"""
        resp = client.get('/api/dashboard/stats',
            headers={'Authorization': 'Bearer fake-token-12345'})
        assert resp.status_code == 401

    def test_me_endpoint(self, client, auth_headers):
        """Endpoint /me retourne les infos utilisateur"""
        resp = client.get('/api/auth/me', headers=auth_headers)
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['email'] == 'admin@osmdm.local'


# ═══════════════════════════════════════════════════
# 3. TESTS DASHBOARD
# ═══════════════════════════════════════════════════

class TestDashboard:
    def test_dashboard_stats(self, client, auth_headers):
        """Dashboard retourne les statistiques"""
        resp = client.get('/api/dashboard/stats', headers=auth_headers)
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'total_entities' in data
        assert 'total_duplicates' in data
        assert 'total_golden' in data


# ═══════════════════════════════════════════════════
# 4. TESTS ENTITÉS (CRUD)
# ═══════════════════════════════════════════════════

class TestEntities:
    def test_create_entity(self, client, auth_headers):
        """Créer une entité retourne 201 avec id et mdm_id"""
        resp = client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'Acme Corp', 'pays': 'France', 'type': 'Client'}})
        assert resp.status_code == 201
        data = resp.get_json()
        assert 'id' in data
        assert data['mdm_id'].startswith('MDM-')

    def test_list_entities(self, client, auth_headers):
        """Lister les entités retourne la pagination"""
        # Créer une entité d'abord
        client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'Test Co'}})
        resp = client.get('/api/entities', headers=auth_headers)
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'total' in data
        assert 'entities' in data
        assert data['total'] >= 1

    def test_get_entity(self, client, auth_headers):
        """Récupérer une entité par ID"""
        # Créer
        create = client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'Fetch Test'}})
        eid = create.get_json()['id']
        # Lire
        resp = client.get(f'/api/entities/{eid}', headers=auth_headers)
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['data']['nom'] == 'Fetch Test'

    def test_update_entity(self, client, auth_headers):
        """Mettre à jour une entité"""
        create = client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'Before'}})
        eid = create.get_json()['id']
        resp = client.put(f'/api/entities/{eid}', headers=auth_headers,
            json={'data': {'nom': 'After', 'email': 'new@test.com'}})
        assert resp.status_code == 200
        # Vérifier la mise à jour
        check = client.get(f'/api/entities/{eid}', headers=auth_headers)
        assert check.get_json()['data']['nom'] == 'After'

    def test_delete_entity(self, client, auth_headers):
        """Supprimer une entité (soft delete)"""
        create = client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'ToDelete'}})
        eid = create.get_json()['id']
        resp = client.delete(f'/api/entities/{eid}', headers=auth_headers)
        assert resp.status_code == 200

    def test_entity_not_found(self, client, auth_headers):
        """Entité inexistante retourne 404"""
        resp = client.get('/api/entities/fake-uuid-not-real', headers=auth_headers)
        assert resp.status_code == 404

    def test_search_entities(self, client, auth_headers):
        """Recherche dans les entités"""
        client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'Unicorn Maritime'}})
        resp = client.get('/api/entities?search=Unicorn', headers=auth_headers)
        assert resp.status_code == 200
        assert resp.get_json()['total'] >= 1


# ═══════════════════════════════════════════════════
# 5. TESTS IMPORT CSV
# ═══════════════════════════════════════════════════

class TestImport:
    def _make_csv(self, rows):
        """Helper : crée un fichier CSV en mémoire"""
        output = io.BytesIO()
        content = "nom,pays,type\n"
        for r in rows:
            content += f"{r[0]},{r[1]},{r[2]}\n"
        output.write(content.encode('utf-8'))
        output.seek(0)
        return output

    def test_import_csv(self, client, auth_token):
        """Import d'un CSV crée les entités"""
        csv_data = self._make_csv([
            ('Acme Corp', 'France', 'Client'),
            ('Beta LLC', 'USA', 'Fournisseur'),
            ('Gamma SAS', 'Maroc', 'Partenaire'),
        ])
        resp = client.post('/api/import/csv',
            headers={'Authorization': f'Bearer {auth_token}'},
            data={'file': (csv_data, 'test.csv'), 'source_label': 'test_import'},
            content_type='multipart/form-data')
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['imported'] == 3
        assert data['errors'] == 0

    def test_import_no_file(self, client, auth_headers):
        """Import sans fichier retourne 400"""
        resp = client.post('/api/import/csv', headers=auth_headers)
        assert resp.status_code == 400

    def test_import_logs(self, client, auth_headers):
        """Les logs d'import sont accessibles"""
        resp = client.get('/api/import/logs', headers=auth_headers)
        assert resp.status_code == 200


# ═══════════════════════════════════════════════════
# 6. TESTS DOUBLONS
# ═══════════════════════════════════════════════════

class TestDuplicates:
    def test_detect_exact_duplicates(self, client, auth_headers):
        """Détection de doublons exacts"""
        # Créer des doublons
        for _ in range(2):
            client.post('/api/entities', headers=auth_headers,
                json={'data': {'nom': 'Duplicate Corp', 'pays': 'France'}})
        # Détecter
        resp = client.post('/api/duplicates/detect', headers=auth_headers,
            json={'fields': ['nom'], 'method': 'exact'})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['found'] >= 1

    def test_list_duplicates(self, client, auth_headers):
        """Lister les doublons"""
        resp = client.get('/api/duplicates?status=pending', headers=auth_headers)
        assert resp.status_code == 200


# ═══════════════════════════════════════════════════
# 7. TESTS GOLDEN RECORDS
# ═══════════════════════════════════════════════════

class TestGoldenRecords:
    def test_merge_entities(self, client, auth_headers):
        """Fusionner 2 entités crée un Golden Record"""
        # Créer 2 entités
        e1 = client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'Corp A', 'email': 'a@test.com'}})
        e2 = client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'Corp B', 'telephone': '0612345678'}})
        eid1 = e1.get_json()['id']
        eid2 = e2.get_json()['id']
        # Merge
        resp = client.post('/api/golden-records/merge', headers=auth_headers,
            json={
                'entity_ids': [eid1, eid2],
                'merged_data': {'nom': 'Corp A', 'email': 'a@test.com', 'telephone': '0612345678'}
            })
        assert resp.status_code == 201
        data = resp.get_json()
        assert data['mdm_id'].startswith('GR-')

    def test_list_golden_records(self, client, auth_headers):
        """Lister les Golden Records"""
        resp = client.get('/api/golden-records', headers=auth_headers)
        assert resp.status_code == 200


# ═══════════════════════════════════════════════════
# 8. TESTS EXPORT
# ═══════════════════════════════════════════════════

class TestExport:
    def test_export_csv(self, client, auth_headers):
        """Export CSV génère un fichier valide"""
        # Créer des données d'abord
        client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'Export Test'}})
        resp = client.get('/api/export/csv', headers=auth_headers)
        assert resp.status_code == 200
        assert 'text/csv' in resp.content_type


# ═══════════════════════════════════════════════════
# 9. TESTS AUDIT
# ═══════════════════════════════════════════════════

class TestAudit:
    def test_audit_trail(self, client, auth_headers):
        """Les actions sont tracées dans l'audit"""
        # Créer une entité (déclenche un audit)
        client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'Audit Test'}})
        resp = client.get('/api/audit', headers=auth_headers)
        assert resp.status_code == 200
        logs = resp.get_json()
        assert len(logs) >= 1
        assert logs[0]['action'] == 'create'


# ═══════════════════════════════════════════════════
# 10. TESTS UTILISATEURS (ADMIN)
# ═══════════════════════════════════════════════════

class TestUsers:
    def test_list_users(self, client, auth_headers):
        """Admin peut lister les utilisateurs"""
        resp = client.get('/api/users', headers=auth_headers)
        assert resp.status_code == 200
        users = resp.get_json()
        assert len(users) >= 1

    def test_create_user(self, client, auth_headers):
        """Admin peut créer un utilisateur"""
        resp = client.post('/api/users', headers=auth_headers,
            json={'email': 'test@osmdm.local', 'name': 'Test User', 'role': 'viewer', 'password': 'testpass123'})
        assert resp.status_code == 201

    def test_create_user_no_email(self, client, auth_headers):
        """Créer un user sans email retourne 400"""
        resp = client.post('/api/users', headers=auth_headers,
            json={'name': 'No Email'})
        assert resp.status_code == 400


# ═══════════════════════════════════════════════════
# 11. TESTS REPORTING
# ═══════════════════════════════════════════════════

class TestReporting:
    def test_reporting_overview(self, client, auth_headers):
        """Overview reporting retourne des KPIs"""
        resp = client.get('/api/reporting/overview', headers=auth_headers)
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'kpis' in data
        assert 'by_source' in data

    def test_reporting_fields(self, client, auth_headers):
        """Récupérer la liste des champs disponibles"""
        resp = client.get('/api/reporting/fields', headers=auth_headers)
        assert resp.status_code == 200


# ═══════════════════════════════════════════════════
# 12. TESTS SÉCURITÉ
# ═══════════════════════════════════════════════════

class TestSecurity:
    def test_security_headers(self, client, auth_headers):
        """Les headers de sécurité sont présents"""
        resp = client.get('/api/health')
        assert resp.headers.get('X-Content-Type-Options') == 'nosniff'
        assert resp.headers.get('X-Frame-Options') == 'DENY'

    def test_password_change_requires_old(self, client, auth_headers):
        """Changement de mot de passe nécessite l'ancien"""
        resp = client.post('/api/auth/change-password', headers=auth_headers,
            json={'old_password': 'wrong', 'new_password': 'newpassword123'})
        assert resp.status_code == 401

    def test_password_min_length(self, client, auth_headers):
        """Mot de passe trop court est refusé"""
        resp = client.post('/api/auth/change-password', headers=auth_headers,
            json={'old_password': 'admin123', 'new_password': 'short'})
        assert resp.status_code == 400


# ═══════════════════════════════════════════════════
# 13. TESTS CONNEXIONS DB
# ═══════════════════════════════════════════════════

class TestConnections:
    def test_list_connections(self, client, auth_headers):
        """Lister les connexions DB"""
        resp = client.get('/api/connections', headers=auth_headers)
        assert resp.status_code == 200

    def test_create_connection(self, client, auth_headers):
        """Créer une connexion DB"""
        resp = client.post('/api/connections', headers=auth_headers,
            json={'name': 'Test DB', 'db_type': 'postgresql', 'host': 'localhost',
                  'port': 5432, 'database_name': 'testdb', 'username': 'user', 'password': 'pass123'})
        assert resp.status_code == 201

    def test_delete_connection(self, client, auth_headers):
        """Supprimer une connexion DB"""
        create = client.post('/api/connections', headers=auth_headers,
            json={'name': 'ToDelete', 'db_type': 'sqlite', 'database_name': ':memory:'})
        cid = create.get_json()['id']
        resp = client.delete(f'/api/connections/{cid}', headers=auth_headers)
        assert resp.status_code == 200


# ═══════════════════════════════════════════════════
# 14. TESTS FUSION RULES
# ═══════════════════════════════════════════════════

class TestFusionRules:
    def test_list_rules(self, client, auth_headers):
        """Les règles de fusion par défaut existent"""
        resp = client.get('/api/fusion-rules', headers=auth_headers)
        assert resp.status_code == 200
        rules = resp.get_json()
        assert len(rules) >= 1  # Règles par défaut créées à l'init


# ═══════════════════════════════════════════════════
# 15. TESTS PREMIUM — DATA QUALITY
# ═══════════════════════════════════════════════════

class TestDataQuality:
    def test_quality_score_complete(self, client, auth_headers):
        """Score de qualité pour données complètes"""
        resp = client.post('/api/premium/quality/score', headers=auth_headers,
            json={'data': {'nom': 'Acme Corp', 'email': 'info@acme.com', 'telephone': '+33612345678', 'pays': 'France'}})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['overall'] >= 70
        assert data['completeness'] >= 80

    def test_quality_score_poor(self, client, auth_headers):
        """Score de qualité pour données incomplètes"""
        resp = client.post('/api/premium/quality/score', headers=auth_headers,
            json={'data': {'nom': ''}})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['completeness'] < 50

    def test_quality_entity(self, client, auth_headers):
        """Score de qualité d'une entité existante"""
        create = client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'Quality Test', 'email': 'test@test.com'}})
        eid = create.get_json()['id']
        resp = client.get(f'/api/premium/quality/entity/{eid}', headers=auth_headers)
        assert resp.status_code == 200
        assert 'overall' in resp.get_json()

    def test_quality_bulk(self, client, auth_headers):
        """Score de qualité global"""
        client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'Bulk1', 'email': 'a@b.com'}})
        resp = client.get('/api/premium/quality/bulk', headers=auth_headers)
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'avg_overall' in data
        assert 'quality_distribution' in data


# ═══════════════════════════════════════════════════
# 16. TESTS PREMIUM — VALIDATION RULES
# ═══════════════════════════════════════════════════

class TestValidationRules:
    def test_list_rule_types(self, client, auth_headers):
        """Lister les types de règles"""
        resp = client.get('/api/premium/validation-rules/types', headers=auth_headers)
        assert resp.status_code == 200
        types = resp.get_json()
        assert 'required' in types
        assert 'regex' in types

    def test_create_validation_rule(self, client, auth_headers):
        """Créer une règle de validation"""
        resp = client.post('/api/premium/validation-rules', headers=auth_headers,
            json={'name': 'Email obligatoire', 'field': 'email', 'rule_type': 'required', 'severity': 'error'})
        assert resp.status_code == 201

    def test_validate_entity(self, client, auth_headers):
        """Valider une entité avec les règles"""
        # Créer une règle
        client.post('/api/premium/validation-rules', headers=auth_headers,
            json={'name': 'Nom requis', 'field': 'nom', 'rule_type': 'required', 'severity': 'error'})
        # Créer une entité sans nom
        create = client.post('/api/entities', headers=auth_headers,
            json={'data': {'email': 'test@test.com'}})
        eid = create.get_json()['id']
        # Valider
        resp = client.get(f'/api/premium/validate/entity/{eid}', headers=auth_headers)
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['valid'] is False
        assert len(data['errors']) >= 1


# ═══════════════════════════════════════════════════
# 17. TESTS PREMIUM — NOTIFICATIONS
# ═══════════════════════════════════════════════════

class TestNotifications:
    def test_list_notifications(self, client, auth_headers):
        """Lister les notifications"""
        resp = client.get('/api/premium/notifications', headers=auth_headers)
        assert resp.status_code == 200

    def test_unread_count(self, client, auth_headers):
        """Compteur de non lues"""
        resp = client.get('/api/premium/notifications/unread-count', headers=auth_headers)
        assert resp.status_code == 200
        assert 'count' in resp.get_json()

    def test_mark_all_read(self, client, auth_headers):
        """Marquer toutes comme lues"""
        resp = client.put('/api/premium/notifications/read-all', headers=auth_headers)
        assert resp.status_code == 200


# ═══════════════════════════════════════════════════
# 18. TESTS PREMIUM — WEBHOOKS
# ═══════════════════════════════════════════════════

class TestWebhooks:
    def test_create_webhook(self, client, auth_headers):
        """Créer un webhook"""
        resp = client.post('/api/premium/webhooks', headers=auth_headers,
            json={'name': 'Test Hook', 'url': 'https://httpbin.org/post', 'events': ['import_done']})
        assert resp.status_code == 201

    def test_list_webhooks(self, client, auth_headers):
        """Lister les webhooks"""
        resp = client.get('/api/premium/webhooks', headers=auth_headers)
        assert resp.status_code == 200


# ═══════════════════════════════════════════════════
# 19. TESTS PREMIUM — SCHEDULER
# ═══════════════════════════════════════════════════

class TestScheduler:
    def test_scheduler_status(self, client, auth_headers):
        """Statut du scheduler"""
        resp = client.get('/api/premium/scheduler/status', headers=auth_headers)
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'running' in data


# ═══════════════════════════════════════════════════
# 20. TESTS PREMIUM — DATA LINEAGE
# ═══════════════════════════════════════════════════

class TestDataLineage:
    def test_golden_record_lineage(self, client, auth_headers):
        """Data lineage d'un Golden Record"""
        # Créer 2 entités et merge
        e1 = client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'Lineage A', 'email': 'a@test.com'}})
        e2 = client.post('/api/entities', headers=auth_headers,
            json={'data': {'nom': 'Lineage B', 'telephone': '0612345678'}})
        eid1 = e1.get_json()['id']
        eid2 = e2.get_json()['id']
        merge = client.post('/api/golden-records/merge', headers=auth_headers,
            json={'entity_ids': [eid1, eid2],
                  'merged_data': {'nom': 'Lineage A', 'email': 'a@test.com', 'telephone': '0612345678'}})
        gid = merge.get_json()['golden_record_id']
        # Lineage
        resp = client.get(f'/api/premium/lineage/golden-record/{gid}', headers=auth_headers)
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'fields' in data
        assert data['source_count'] == 2
