"""
MDM — Frontend Server avec proxy
"""
from flask import Flask, render_template, send_from_directory, request, Response
import os, urllib.request, urllib.error, json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND  = 'http://127.0.0.1:5001'

app = Flask(__name__,
            template_folder=os.path.join(BASE_DIR, 'templates'),
            static_folder=os.path.join(BASE_DIR, 'static'))
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory(os.path.join(BASE_DIR, 'static'), filename)

@app.route('/api/<path:path>', methods=['GET','POST','PUT','DELETE','OPTIONS'])
def proxy(path):
    url = f"{BACKEND}/api/{path}"
    if request.query_string:
        url += '?' + request.query_string.decode('utf-8')

    # Forward tous les headers pertinents
    headers = {}
    for key in ('Authorization', 'Content-Type', 'Accept'):
        val = request.headers.get(key)
        if val: headers[key] = val

    body = request.get_data() or None

    # OPTIONS pre-flight
    if request.method == 'OPTIONS':
        resp = Response('', status=204)
        resp.headers['Access-Control-Allow-Origin'] = '*'
        resp.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
        resp.headers['Access-Control-Allow-Methods'] = 'GET,POST,PUT,DELETE,OPTIONS'
        return resp

    try:
        req = urllib.request.Request(url, data=body, headers=headers, method=request.method)
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = resp.read()
            ct = resp.headers.get('Content-Type', 'application/json')
            # Forward le Content-Disposition pour les exports CSV/PDF
            proxy_resp = Response(data, status=resp.status, mimetype=ct)
            cd = resp.headers.get('Content-Disposition')
            if cd: proxy_resp.headers['Content-Disposition'] = cd
            return proxy_resp
    except urllib.error.HTTPError as e:
        body = e.read()
        ct = e.headers.get('Content-Type', 'application/json')
        return Response(body, status=e.code, mimetype=ct)
    except urllib.error.URLError as e:
        return Response(json.dumps({'error': f'Backend non accessible : {str(e.reason)}. Vérifiez que le backend tourne sur le port 5001.'}),
                       status=502, mimetype='application/json')
    except Exception as e:
        return Response(json.dumps({'error': str(e)}), status=500, mimetype='application/json')

if __name__ == '__main__':
    print("\n🌐  MDM Frontend + Proxy démarré !")
    print("📍  Ouvrez http://127.0.0.1:3000\n")
    app.run(host='127.0.0.1', debug=True, port=3000)
