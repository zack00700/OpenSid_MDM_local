#!/usr/bin/env python3
"""O.S MDM V2 — Script de démarrage unique"""
import subprocess, sys, os, time, signal

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
processes = []

def stop_all(sig=None, frame=None):
    print("\n🛑  Arrêt de O.S MDM...")
    for p in processes:
        try: p.terminate()
        except: pass
    sys.exit(0)

signal.signal(signal.SIGINT,  stop_all)
signal.signal(signal.SIGTERM, stop_all)

print("=" * 55)
print("   O.S MDM V2 — OpenSID Master Data Management")
print("=" * 55)

print("\n📦  Installation des dépendances...")
subprocess.run([sys.executable, '-m', 'pip', 'install', '-r',
    os.path.join(BASE_DIR, 'requirements.txt'),
    '--break-system-packages', '-q'], check=False)

print("🚀  Démarrage backend  (port 5001)...")
processes.append(subprocess.Popen(
    [sys.executable, os.path.join(BASE_DIR, 'backend', 'app.py')]))
time.sleep(2)

print("🌐  Démarrage frontend (port 3000)...")
processes.append(subprocess.Popen(
    [sys.executable, os.path.join(BASE_DIR, 'frontend', 'server.py')]))
time.sleep(1)

print("\n" + "=" * 55)
print("✅  O.S MDM V2 est démarrée !")
print("=" * 55)
print("\n  🌐  Interface  →  http://127.0.0.1:3000")
print("  🔌  API        →  http://127.0.0.1:5001/api")
print("  ❤️  Health     →  http://127.0.0.1:5001/api/health")
print("\n  ─────────────────────────────────────────")
print("  Appuyez sur Ctrl+C pour arrêter\n")

try:
    processes[0].wait()
except KeyboardInterrupt:
    stop_all()
