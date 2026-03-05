/**
 * O.S MDM V2 — Application JavaScript
 * Modules : Auth, Dashboard, Import, Entités, Doublons, Golden Records,
 *            Connexions DB, Règles de fusion, Reporting BI, Export CSV
 */

const API = `${window.location.protocol}//${window.location.hostname}:3000/api`;
let TOKEN = localStorage.getItem('mdm_token') || '';
let currentUser = null, currentPage = 1;
let currentEntityId = null, currentDupId = null, currentDupData = null;
let currentConnId = null, currentRuleId = null;
let charts = {}, debounceTimer = null;

// ── UTILS ─────────────────────────────────────────────────────────────────
function esc(s){return String(s??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function fmtDate(d){if(!d)return'—';try{return new Date(d).toLocaleString('fr-FR',{day:'2-digit',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'});}catch{return d;}}
function toast(msg,type='success'){const t=document.createElement('div');const c={success:'#059669',error:'#dc2626',warning:'#d97706',info:'#1B5EA6'};t.style.cssText=`position:fixed;bottom:24px;right:24px;background:${c[type]||c.success};color:#fff;padding:12px 20px;border-radius:10px;font-size:13px;font-weight:600;z-index:9999;box-shadow:0 4px 20px rgba(0,0,0,.18);transition:opacity .3s;max-width:360px;`;t.textContent=msg;document.body.appendChild(t);setTimeout(()=>{t.style.opacity='0';setTimeout(()=>t.remove(),300);},3500);}

async function api(path,opts={}){
  try{
    const res=await fetch(API+path,{headers:{'Content-Type':'application/json','Authorization':`Bearer ${TOKEN}`,...opts.headers},...opts});
    if(res.status===401){logout();return null;}
    const data=await res.json();
    if(!res.ok&&data.error){toast(data.error,'error');return null;}
    return data;
  }catch(e){toast('Erreur réseau — Lancez : python start.py','error');return null;}
}

// ── AUTH ──────────────────────────────────────────────────────────────────
window.onload=()=>{
  TOKEN?initApp():showLogin();
  // Wire form submit
  const form=document.getElementById('login-form');
  if(form) form.addEventListener('submit',e=>{e.preventDefault();login();});
};
function showLogin(){document.getElementById('login-page').style.display='flex';document.getElementById('app-page').style.display='none';}

async function login(){
  const email=document.getElementById('login-email').value.trim();
  const pw=document.getElementById('login-password').value;
  const btn=document.querySelector('#login-form button[type="submit"]');
  if(btn){btn.textContent='⏳';btn.disabled=true;}
  const res=await api('/auth/login',{method:'POST',body:JSON.stringify({email,password:pw})});
  if(btn){btn.textContent='Se connecter';btn.disabled=false;}
  if(!res?.token)return;
  TOKEN=res.token;localStorage.setItem('mdm_token',TOKEN);currentUser=res.user;initApp();
}
document.addEventListener('keydown',e=>{if(e.key==='Enter'&&document.getElementById('login-page').style.display!=='none')login();});
function logout(){TOKEN='';localStorage.removeItem('mdm_token');document.getElementById('login-page').style.display='flex';document.getElementById('app-page').style.display='none';}

async function initApp(){
  const me=await api('/auth/me');if(!me)return;
  currentUser=me;
  document.getElementById('login-page').style.display='none';
  document.getElementById('app-page').style.display='flex';
  const unEl=document.getElementById('user-name');if(unEl)unEl.textContent=me.name||me.email;const ueEl=document.getElementById('user-email');if(ueEl)ueEl.textContent=me.email||'';
  showSection('dashboard');
}

// ── NAVIGATION ────────────────────────────────────────────────────────────
function showSection(name){
  document.querySelectorAll('.section').forEach(s=>s.style.display='none');
  document.querySelectorAll('.sidebar-link').forEach(l=>l.classList.remove('active'));
  const sec=document.getElementById('section-'+name);if(sec)sec.style.display='block';
  const link=document.querySelector(`.sidebar-link[data-section="${name}"]`);if(link)link.classList.add('active');
  switch(name){
    case 'dashboard':   loadDashboard();break;
    case 'connections': loadConnections();break;
    case 'connectors':  loadConnectors();break;
    case 'writeback':   loadWriteback();break;
    case 'import':      loadImportLogs();break;
    case 'entities':    loadEntities();break;
    case 'duplicates':  loadDuplicates();initDupSection();break;
    case 'golden':      loadGoldenRecords();break;
    case 'rules':       loadRules();break;
    case 'reporting':   loadReporting();break;
    case 'export':      break;
    case 'admin-users': loadUsers(); break;
    case 'profile':     loadProfile(); break;
    case 'maritime-dashboard': initMaritime().then(()=>loadMaritimeDashboard());break;
    case 'maritime-vessels': initMaritime().then(()=>{loadVessels();loadMaritimeKPIs();});break;
    case 'maritime-owners':  initMaritime().then(()=>loadOwners());break;
    case 'maritime-ports':   initMaritime().then(()=>loadPorts());break;
    case 'maritime-calls':   initMaritime().then(()=>loadCalls());break;
  }
}
document.addEventListener('DOMContentLoaded',()=>{
  document.querySelectorAll('.sidebar-link').forEach(link=>{link.addEventListener('click',()=>showSection(link.dataset.section));});
});

// ── DASHBOARD ─────────────────────────────────────────────────────────────
async function loadDashboard(){
  const data=await api('/dashboard/stats');if(!data)return;
  // Update individual KPI values
  const kmap={
    'd-entities':data.total_entities,'d-dups':data.total_duplicates,
    'd-golden':data.total_golden,'d-imports':data.total_imports,
    'd-conns':data.total_connections,'d-rules':data.total_rules
  };
  Object.entries(kmap).forEach(([id,val])=>{const el=document.getElementById(id);if(el)el.textContent=val??'—';});
  const rEl=document.getElementById('recent-entities');
  if(rEl&&data.recent_entities){rEl.innerHTML=data.recent_entities.length?`<table style="width:100%;border-collapse:collapse;">${data.recent_entities.map(e=>{let d={};try{d=typeof e.data==='string'?JSON.parse(e.data):e.data;}catch{}const name=d.nom||d.name||d.company||Object.values(d)[0]||'—';return`<tr><td class="table-td"><span style="font-family:monospace;font-size:10px;color:#1B5EA6;">${e.mdm_id||''}</span></td><td class="table-td" style="font-weight:600;">${esc(name)}</td><td class="table-td"><span class="badge" style="background:#e8f0fb;color:#1B5EA6;">${esc(e.source||'')}</span></td><td class="table-td" style="font-size:11px;color:#9ca3af;">${fmtDate(e.created_at)}</td></tr>`;}).join('')}</table>`:'<p style="color:#9ca3af;font-size:13px;">Aucune entité.</p>';}
  const sEl=document.getElementById('source-breakdown');
  if(sEl&&data.sources_breakdown?.length){const max=data.sources_breakdown[0]?.cnt||1;sEl.innerHTML=data.sources_breakdown.map(s=>`<div style="margin-bottom:10px;"><div style="display:flex;justify-content:space-between;margin-bottom:3px;"><span style="font-size:12px;color:#374151;font-weight:600;">${esc(s.source||'—')}</span><span style="font-size:12px;color:#6b7280;">${s.cnt}</span></div><div style="background:#f3f4f6;border-radius:99px;height:6px;"><div style="background:#1B5EA6;border-radius:99px;height:6px;width:${Math.round(s.cnt/max*100)}%;"></div></div></div>`).join('');}
}

// ── CONNECTIONS ────────────────────────────────────────────────────────────
async function loadConnections(){
  const conns=await api('/connections');const container=document.getElementById('connections-list');
  if(!conns?.length){container.innerHTML='<div class="card" style="text-align:center;color:#9ca3af;padding:40px;"><p style="font-size:32px;">🔌</p><p>Aucune connexion. Cliquez "+ Nouvelle connexion".</p></div>';return;}
  const icons={postgresql:'🐘',mysql:'🐬',mariadb:'🐬',mssql:'🪟',oracle:'🔶',sqlite:'💾'};
  const ss=s=>s==='ok'?'background:#d1fae5;color:#065f46;':s==='error'?'background:#fee2e2;color:#991b1b;':'background:#f3f4f6;color:#6b7280;';
  const sl=s=>s==='ok'?'✅ Connecté':s==='error'?'❌ Erreur':'⏳ Non testé';
  container.innerHTML=`<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:14px;">${conns.map(c=>`<div class="card" style="padding:16px;"><div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;"><span style="font-size:28px;">${icons[c.db_type]||'🗄️'}</span><div><p style="font-size:14px;font-weight:700;margin:0;">${esc(c.name)}</p><p style="font-size:11px;color:#9ca3af;margin:0;">${c.db_type?.toUpperCase()} · ${esc(c.host||'—')}:${c.port||''}</p></div><span class="badge" style="margin-left:auto;${ss(c.status)}">${sl(c.status)}</span></div><p style="font-size:12px;color:#6b7280;margin:0 0 10px;">Base : <b>${esc(c.database_name||'—')}</b> · User : ${esc(c.username||'—')}</p><div style="display:flex;gap:6px;flex-wrap:wrap;"><button class="btn btn-secondary" style="font-size:11px;padding:4px 10px;" onclick="testConn('${c.id}')">🔌 Tester</button><button class="btn btn-primary" style="font-size:11px;padding:4px 10px;" onclick="openDBImport('${c.id}','${esc(c.name)}')">📥 Importer</button><button class="btn btn-secondary" style="font-size:11px;padding:4px 10px;" onclick='editConn(${JSON.stringify(c).replace(/"/g,"&quot;")})'>✏️</button><button class="btn btn-danger" style="font-size:11px;padding:4px 10px;" onclick="deleteConn('${c.id}')">🗑️</button></div></div>`).join('')}</div>`;
}
function openConnModal(conn=null){currentConnId=conn?.id||null;document.getElementById('conn-modal-title').textContent=conn?'Modifier connexion':'Nouvelle connexion';['name','db_type','host','port','database_name','username'].forEach((f,i)=>{const ids=['c-name','c-type','c-host','c-port','c-db','c-user'];const el=document.getElementById(ids[i]);if(el)el.value=conn?.[f]||'';});document.getElementById('c-pass').value='';document.getElementById('conn-modal').style.display='flex';}
function editConn(conn){openConnModal(conn);}
function closeConnModal(){document.getElementById('conn-modal').style.display='none';currentConnId=null;}
async function saveConn(){const body={name:document.getElementById('c-name').value.trim(),db_type:document.getElementById('c-type').value,host:document.getElementById('c-host').value.trim(),port:parseInt(document.getElementById('c-port').value)||null,database_name:document.getElementById('c-db').value.trim(),username:document.getElementById('c-user').value.trim(),password:document.getElementById('c-pass').value};if(!body.name){toast('Nom requis','error');return;}if(currentConnId)await api(`/connections/${currentConnId}`,{method:'PUT',body:JSON.stringify(body)});else await api('/connections',{method:'POST',body:JSON.stringify(body)});toast(currentConnId?'Mise à jour':'Connexion créée');closeConnModal();loadConnections();}
async function testCurrentConn(){await saveConn();if(currentConnId)await testConn(currentConnId);}
async function testConn(id){toast('Test…','info');const res=await api(`/connections/${id}/test`,{method:'POST'});if(res)toast(res.status==='ok'?'✅ Connexion réussie !':'❌ '+(res.message||'Échec'),res.status==='ok'?'success':'error');loadConnections();}
async function deleteConn(id){if(!confirm('Supprimer ?'))return;await api(`/connections/${id}`,{method:'DELETE'});toast('Supprimée','warning');loadConnections();}
async function openDBImport(connId,connName){const panel=document.getElementById('db-import-panel');if(!panel)return;panel.style.display='block';panel.dataset.connId=connId;document.getElementById('db-import-table').innerHTML='<option>Chargement…</option>';document.getElementById('db-preview').innerHTML='';const res=await api(`/connections/${connId}/tables`);const sel=document.getElementById('db-import-table');if(res?.tables?.length)sel.innerHTML=res.tables.map(t=>`<option value="${esc(t)}">${esc(t)}</option>`).join('');else sel.innerHTML='<option value="">— Aucune table —</option>';}
async function previewTable(){const connId=document.getElementById('db-import-panel').dataset.connId;const table=document.getElementById('db-import-table').value;const sql=document.getElementById('db-import-sql').value.trim();const res=await api(`/connections/${connId}/preview`,{method:'POST',body:JSON.stringify({table,sql:sql||null})});if(!res?.rows?.length){document.getElementById('db-preview').innerHTML='<p style="color:#9ca3af;font-size:12px;">Aucun résultat.</p>';return;}const cols=res.columns||Object.keys(res.rows[0]);document.getElementById('db-preview').innerHTML=`<div style="overflow-x:auto;margin-top:10px;max-height:200px;"><table style="width:100%;border-collapse:collapse;font-size:11px;"><thead><tr>${cols.map(c=>`<th class="table-th">${esc(c)}</th>`).join('')}</tr></thead><tbody>${res.rows.map(r=>`<tr>${cols.map(c=>`<td class="table-td">${esc(String(r[c]??''))}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;}
async function importFromDB(){const connId=document.getElementById('db-import-panel').dataset.connId;const table=document.getElementById('db-import-table').value;const source=document.getElementById('db-import-label').value.trim()||table;const sql=document.getElementById('db-import-sql').value.trim();toast('Import…','info');const res=await api(`/connections/${connId}/import`,{method:'POST',body:JSON.stringify({table,source_label:source,sql:sql||null})});if(res){toast(`✅ ${res.imported} lignes importées`);document.getElementById('db-import-panel').style.display='none';}}

// ── IMPORT ─────────────────────────────────────────────────────────────────
let dragFile=null;
function setupDropzone(){const dz=document.getElementById('drop-zone');if(!dz||dz._ready)return;dz._ready=true;dz.addEventListener('dragover',e=>{e.preventDefault();dz.style.borderColor='#1B5EA6';});dz.addEventListener('dragleave',()=>dz.style.borderColor='#e5e7eb');dz.addEventListener('drop',e=>{e.preventDefault();dz.style.borderColor='#e5e7eb';handleFile(e.dataTransfer.files[0]);});document.getElementById('file-input').addEventListener('change',e=>handleFile(e.target.files[0]));}
function handleFile(file){if(!file)return;dragFile=file;document.getElementById('selected-filename').textContent=`📄 ${file.name} (${(file.size/1024).toFixed(1)} KB)`;document.getElementById('file-preview').style.display='block';document.getElementById('drop-zone').style.display='none';}
function clearFile(){dragFile=null;document.getElementById('drop-zone').style.display='block';document.getElementById('file-preview').style.display='none';document.getElementById('file-input').value='';}
async function uploadFile(){if(!dragFile){toast('Choisissez un fichier','error');return;}const source=document.getElementById('import-source-label').value.trim()||dragFile.name;const fd=new FormData();fd.append('file',dragFile);fd.append('source_label',source);const btn=document.getElementById('upload-btn');btn.textContent='⏳ Import…';btn.disabled=true;try{const res=await fetch(`${API}/import/csv`,{method:'POST',headers:{'Authorization':`Bearer ${TOKEN}`},body:fd});const data=await res.json();btn.textContent='📤 Importer';btn.disabled=false;if(data.imported!==undefined){toast(`✅ ${data.imported} lignes importées`);clearFile();loadImportLogs();loadDashboard();}else toast(data.error||'Erreur','error');}catch{btn.textContent='📤 Importer';btn.disabled=false;toast('Erreur upload','error');}}
async function loadImportLogs(){setupDropzone();const logs=await api('/import/logs');const el=document.getElementById('import-logs-list');if(!el)return;if(!logs?.length){el.innerHTML='<p style="color:#9ca3af;font-size:13px;">Aucun import.</p>';return;}el.innerHTML=`<table style="width:100%;border-collapse:collapse;"><thead><tr><th class="table-th">Fichier</th><th class="table-th">Source</th><th class="table-th">Total</th><th class="table-th">Importés</th><th class="table-th">Erreurs</th><th class="table-th">Statut</th><th class="table-th">Date</th></tr></thead><tbody>${logs.map(l=>{const sc=l.status==='done'?'background:#d1fae5;color:#065f46;':l.status==='error'?'background:#fee2e2;color:#991b1b;':'background:#fef3c7;color:#92400e;';return`<tr><td class="table-td" style="font-weight:600;">${esc(l.filename)}</td><td class="table-td"><span class="badge" style="background:#e8f0fb;color:#1B5EA6;">${esc(l.source_label||'—')}</span></td><td class="table-td">${l.total_rows||0}</td><td class="table-td" style="color:#059669;font-weight:600;">${l.imported||0}</td><td class="table-td" style="color:#dc2626;">${l.errors||0}</td><td class="table-td"><span class="badge" style="${sc}">${l.status}</span></td><td class="table-td" style="font-size:11px;color:#9ca3af;">${fmtDate(l.created_at)}</td></tr>`;}).join('')}</tbody></table>`;}

// ── ENTITIES ───────────────────────────────────────────────────────────────
async function loadEntities(){
  const srch=document.getElementById('entity-search')?.value.trim()||'';
  const src=document.getElementById('entity-source-filter')?.value.trim()||'';
  const colFilter=document.getElementById('entity-cols-select')?.value.trim()||'';
  const params=new URLSearchParams({page:currentPage,per_page:20});
  if(srch)params.set('search',srch);if(src)params.set('source',src);
  const data=await api(`/entities?${params}`);if(!data)return;
  const container=document.getElementById('entities-table');
  if(!data.entities.length){container.innerHTML='<p style="color:#9ca3af;text-align:center;padding:24px;font-size:13px;">Aucune entité.</p>';return;}
  // Tous les champs sans limite
  let allKeys=[...new Set(data.entities.flatMap(e=>Object.keys(e.data||{})))];
  if(colFilter) allKeys=allKeys.filter(k=>k===colFilter);
  // Mettre à jour le select des colonnes
  const colSel=document.getElementById('entity-cols-select');
  if(colSel&&colSel.options.length<=1){
    const all=[...new Set(data.entities.flatMap(e=>Object.keys(e.data||{})))];
    all.forEach(k=>{if(![...colSel.options].find(o=>o.value===k)){const o=document.createElement('option');o.value=k;o.textContent=k;colSel.appendChild(o);}});
  }
  // Mettre à jour le filtre source
  const srcSel=document.getElementById('entity-source-filter');
  if(srcSel){
    const sources=[...new Set(data.entities.map(e=>e.source).filter(Boolean))];
    sources.forEach(s=>{if(![...srcSel.options].find(o=>o.value===s)){const o=document.createElement('option');o.value=s;o.textContent=s;srcSel.appendChild(o);}});
  }
  container.innerHTML=`<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;min-width:600px;"><thead><tr><th class="table-th" style="white-space:nowrap;">MDM ID</th>${allKeys.map(k=>`<th class="table-th" style="white-space:nowrap;">${esc(k)}</th>`).join('')}<th class="table-th">Source</th><th class="table-th">Créé</th><th class="table-th">Actions</th></tr></thead><tbody>${data.entities.map(e=>`<tr><td class="table-td"><span style="font-family:monospace;font-size:10px;color:#1B5EA6;">${e.mdm_id}</span></td>${allKeys.map(k=>`<td class="table-td" style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${esc(String(e.data?.[k]??''))}">${esc(String(e.data?.[k]??'—'))}</td>`).join('')}<td class="table-td"><span class="badge" style="background:#e8f0fb;color:#1B5EA6;font-size:10px;">${esc(e.source||'—')}</span></td><td class="table-td" style="font-size:11px;color:#9ca3af;white-space:nowrap;">${fmtDate(e.created_at)}</td><td class="table-td"><div style="display:flex;gap:4px;"><button class="btn btn-secondary" style="padding:3px 8px;font-size:11px;" onclick="editEntity('${e.id}')">✏️</button><button class="btn btn-danger" style="padding:3px 8px;font-size:11px;" onclick="deleteEntity('${e.id}')">🗑️</button></div></td></tr>`).join('')}</tbody></table></div>`;
  document.getElementById('entity-count').textContent=`${data.total} entité(s) · ${allKeys.length} colonne(s)`;
  document.getElementById('page-info').textContent=`Page ${currentPage}`;
  document.getElementById('prev-btn').disabled=currentPage<=1;
  document.getElementById('next-btn').disabled=(currentPage*20)>=data.total;
}
async function refreshEntitySources(){
  const srcSel=document.getElementById('entity-source-filter');
  if(!srcSel)return;
  while(srcSel.options.length>1)srcSel.remove(1);
  const data=await api('/entities?page=1&per_page=100');
  if(!data)return;
  const sources=[...new Set(data.entities.map(e=>e.source).filter(Boolean))];
  sources.forEach(s=>{const o=document.createElement('option');o.value=s;o.textContent=s;srcSel.appendChild(o);});
  toast(`${sources.length} source(s) chargée(s)`,'info');
}
function debounceSearch(){clearTimeout(debounceTimer);currentPage=1;debounceTimer=setTimeout(loadEntities,400);}
function prevPage(){if(currentPage>1){currentPage--;loadEntities();}}
function nextPage(){currentPage++;loadEntities();}
function openCreateEntity(){openEntityModal(null);}
async function editEntity(id){const e=await api(`/entities/${id}`);if(e)openEntityModal(e);}
function openEntityModal(entity=null){currentEntityId=entity?.id||null;document.getElementById('entity-modal-title').textContent=entity?'Modifier entité':'Nouvelle entité';const container=document.getElementById('entity-fields-container');container.innerHTML='';const data=entity?.data||{};const fields=Object.keys(data).length?Object.entries(data):[['','']];fields.forEach(([k,v])=>addEntityField(k,v));document.getElementById('entity-modal').style.display='flex';}
function addEntityField(key='',value=''){const div=document.createElement('div');div.style.cssText='display:flex;gap:8px;margin-bottom:8px;align-items:center;';div.innerHTML=`<input class="input" placeholder="Champ" value="${esc(key)}" style="flex:1;"/><input class="input" placeholder="Valeur" value="${esc(String(value))}" style="flex:2;"/><button onclick="this.parentElement.remove()" style="background:none;border:none;cursor:pointer;color:#dc2626;font-size:16px;padding:4px;">✕</button>`;document.getElementById('entity-fields-container').appendChild(div);}
function closeEntityModal(){document.getElementById('entity-modal').style.display='none';currentEntityId=null;}
async function saveEntity(){const data={};document.querySelectorAll('#entity-fields-container > div').forEach(d=>{const [k,v]=d.querySelectorAll('input');if(k.value.trim())data[k.value.trim()]=v.value;});if(currentEntityId)await api(`/entities/${currentEntityId}`,{method:'PUT',body:JSON.stringify({data})});else await api('/entities',{method:'POST',body:JSON.stringify({data})});toast(currentEntityId?'Mis à jour':'Créé');closeEntityModal();loadEntities();}
async function deleteEntity(id){if(!confirm('Supprimer ?'))return;await api(`/entities/${id}`,{method:'DELETE'});toast('Supprimée','warning');loadEntities();}

// ── DUPLICATES ─────────────────────────────────────────────────────────────
function toggleFuzzy(){document.getElementById('fuzzy-threshold-wrap').style.display=document.getElementById('dup-method').value==='fuzzy'?'block':'none';}
async function initDupSection(){
  // Charger les sources disponibles
  const srcSel=document.getElementById('dup-source-filter');
  if(!srcSel) return;
  const data=await api('/entities?page=1&per_page=200');
  if(!data) return;
  // Remplir sources
  const sources=[...new Set(data.entities.map(e=>e.source).filter(Boolean))];
  while(srcSel.options.length>1) srcSel.remove(1);
  sources.forEach(s=>{const o=document.createElement('option');o.value=s;o.textContent=s;srcSel.appendChild(o);});
  // Charger les champs initiaux (toutes sources)
  await loadDupFields();
}
async function loadDupFields(){
  const src=document.getElementById('dup-source-filter')?.value||'';
  const params=new URLSearchParams({page:1,per_page:200});if(src)params.set('source',src);
  const data=await api(`/entities?${params}`);if(!data)return;
  const fields=[...new Set(data.entities.flatMap(e=>Object.keys(e.data||{})))].sort();
  const sel=document.getElementById('dup-field-select');if(!sel)return;
  const prev=sel.value;
  sel.innerHTML='<option value="">— Sélectionner un champ —</option>';
  fields.forEach(f=>{
    const o=document.createElement('option');
    o.value=f;o.textContent=f;
    if(f===prev) o.selected=true;
    sel.appendChild(o);
  });
}
async function detectDuplicates(){
  const method=document.getElementById('dup-method').value;
  const fieldVal=document.getElementById('dup-field-select')?.value||'';
  const fields=fieldVal?[fieldVal]:[];
  const threshold=parseInt(document.getElementById('dup-threshold')?.value)||80;
  const src=document.getElementById('dup-source-filter')?.value||'';
  const btn=document.getElementById('detect-btn');btn.textContent='⏳…';btn.disabled=true;
  const res=await api('/duplicates/detect',{method:'POST',body:JSON.stringify({method,fields,threshold,source_filter:src})});
  btn.textContent='🔍 Détecter';btn.disabled=false;
  if(!res)return;
  toast(`${res.found} doublon(s) trouvé(s)`,res.found>0?'warning':'success');
  loadDuplicates();
}
async function loadDuplicates(){
  const dups=await api('/duplicates?status=pending');const badge=document.getElementById('dup-badge');
  if(dups?.length){badge.textContent=dups.length;badge.style.display='inline';}else badge.style.display='none';
  const container=document.getElementById('duplicates-list');
  if(!dups?.length){container.innerHTML='<div class="card" style="text-align:center;color:#9ca3af;padding:40px;">✅ Aucun doublon en attente.</div>';return;}
  const keys=[...new Set(dups.flatMap(d=>[...Object.keys(d.entity1?.data||{}),...Object.keys(d.entity2?.data||{})]))].slice(0,5);
  container.innerHTML=dups.map(d=>{const sc=d.score>=.9?'#dc2626':d.score>=.7?'#d97706':'#6b7280';const safeD=JSON.stringify(d).replace(/</g,'\\u003c').replace(/>/g,'\\u003e').replace(/'/g,"\\u0027");return`<div class="card" style="margin-bottom:14px;"><div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;"><div style="display:flex;gap:8px;"><span class="badge" style="background:#fee2e2;color:${sc};">Score ${Math.round(d.score*100)}%</span><span class="badge" style="background:#f3f4f6;color:#6b7280;">${d.method}</span></div><div style="display:flex;gap:6px;"><button class="btn btn-success" style="font-size:12px;" onclick='openMerge(${safeD})'>⚡ Fusionner</button><button class="btn btn-secondary" style="font-size:12px;" onclick="ignoredup('${d.id}')">Ignorer</button></div></div><div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;">${[d.entity1,d.entity2].map((e,i)=>`<div style="background:#f9fafb;border-radius:10px;padding:14px;"><p style="font-size:11px;font-weight:700;color:#1B5EA6;margin:0 0 8px;">Entité ${i+1} — <span style="font-family:monospace;">${e?.mdm_id||'?'}</span></p>${keys.map(k=>`<div style="display:flex;gap:8px;margin-bottom:4px;"><span style="font-size:11px;color:#9ca3af;min-width:70px;">${esc(k)}</span><span style="font-size:12px;color:#374151;font-weight:500;">${esc(String(e?.data?.[k]??'—'))}</span></div>`).join('')}</div>`).join('')}</div></div>`;}).join('');
}
async function ignoredup(id){await api('/duplicates/ignore',{method:'POST',body:JSON.stringify({duplicate_id:id})});toast('Ignoré','info');loadDuplicates();}

// ── MERGE MODAL ────────────────────────────────────────────────────────────
async function openMerge(dup){
  currentDupId=dup.id;currentDupData=dup;
  const e1=dup.entity1?.data||{};const e2=dup.entity2?.data||{};
  const allKeys=[...new Set([...Object.keys(e1),...Object.keys(e2)])];
  // Appliquer les règles automatiquement dès l'ouverture
  const e1full={...e1,_source:dup.entity1?.source};
  const e2full={...e2,_source:dup.entity2?.source};
  let autoChosen={};
  const rulesRes=await api('/fusion-rules/preview',{method:'POST',body:JSON.stringify({entity1:e1full,entity2:e2full})});
  if(rulesRes?.applied) rulesRes.applied.forEach(a=>{autoChosen[a.field]=String(a.chosen??'');});
  document.getElementById('merge-fields').innerHTML=allKeys.map(k=>{
    const v1=e1[k]??'';const v2=e2[k]??'';
    const chosen=autoChosen[k]??String(v1);
    const isAuto=k in autoChosen;
    const isDiff=String(v1)!==String(v2); // §4.2 — Surbrillance des différences
    const src1=esc(dup.entity1?.source||'Source 1');
    const src2=esc(dup.entity2?.source||'Source 2');
    return`<div style="border:1px solid ${isDiff?'#fbbf24':isAuto?'#d1fae5':'#f3f4f6'};border-radius:10px;padding:12px;background:${isDiff?'#fffbeb':isAuto?'#f0fdf4':'#fff'};">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">
        <p style="font-size:12px;font-weight:700;color:#374151;margin:0;">${esc(k)} ${isDiff?'<span style="color:#d97706;font-size:10px;">⚡ Différent</span>':''}</p>
        ${isAuto?'<span style="font-size:10px;background:#d1fae5;color:#065f46;padding:2px 6px;border-radius:20px;font-weight:600;">⚡ Auto</span>':'<span style="font-size:10px;background:#fef3c7;color:#92400e;padding:2px 6px;border-radius:20px;">Manuel</span>'}
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
        <label style="display:flex;align-items:flex-start;gap:8px;padding:8px 10px;border:2px solid ${chosen===String(v1)?'#1B5EA6':'#e5e7eb'};border-radius:8px;cursor:pointer;background:${chosen===String(v1)?'#e8f0fb':'#fff'};">
          <input type="radio" name="f_${esc(k)}" value="${esc(String(v1))}" class="merge-radio" data-field="${esc(k)}" ${chosen===String(v1)?'checked':''} onchange="highlightMergeChoice(this)"/>
          <div><p style="font-size:10px;color:#9ca3af;margin:0 0 2px;">${src1}</p><span style="font-size:12px;${isDiff?'font-weight:600;':''}">${esc(String(v1))||'<em style=color:#9ca3af>vide</em>'}</span></div>
        </label>
        <label style="display:flex;align-items:flex-start;gap:8px;padding:8px 10px;border:2px solid ${chosen===String(v2)?'#1B5EA6':'#e5e7eb'};border-radius:8px;cursor:pointer;background:${chosen===String(v2)?'#e8f0fb':'#fff'};">
          <input type="radio" name="f_${esc(k)}" value="${esc(String(v2))}" class="merge-radio" data-field="${esc(k)}" ${chosen===String(v2)?'checked':''} onchange="highlightMergeChoice(this)"/>
          <div><p style="font-size:10px;color:#9ca3af;margin:0 0 2px;">${src2}</p><span style="font-size:12px;${isDiff?'font-weight:600;':''}">${esc(String(v2))||'<em style=color:#9ca3af>vide</em>'}</span></div>
        </label>
      </div>
    </div>`;
  }).join('');
  document.getElementById('merge-modal').style.display='flex';
  const autoCount=Object.keys(autoChosen).length;
  if(autoCount>0) toast(`${autoCount} champ(s) sélectionné(s) automatiquement ⚡`,'info');
}
function highlightMergeChoice(radio){
  const name=radio.name;
  document.querySelectorAll(`input[name="${name}"]`).forEach(r=>{
    const lbl=r.closest('label');
    if(lbl){lbl.style.borderColor=r.checked?'#1B5EA6':'#e5e7eb';lbl.style.background=r.checked?'#e8f0fb':'#fff';}
  });
}
function closeMergeModal(){document.getElementById('merge-modal').style.display='none';currentDupId=null;currentDupData=null;}
async function applyRulesPreview(){
  if(!currentDupData)return;
  const e1={...(currentDupData.entity1?.data||{}),_source:currentDupData.entity1?.source};
  const e2={...(currentDupData.entity2?.data||{}),_source:currentDupData.entity2?.source};
  const res=await api('/fusion-rules/preview',{method:'POST',body:JSON.stringify({entity1:e1,entity2:e2})});
  if(!res?.applied)return;
  res.applied.forEach(a=>{
    document.querySelectorAll(`.merge-radio[data-field="${a.field}"]`).forEach(r=>{
      r.checked=(r.value===String(a.chosen??''));
      if(r.checked)highlightMergeChoice(r);
    });
  });
  toast(`⚡ ${res.applied.length} champ(s) appliqués automatiquement`,'info');
}
async function confirmMerge(){const mergedData={};document.querySelectorAll('.merge-radio:checked').forEach(r=>{mergedData[r.dataset.field]=r.value;});const res=await api('/golden-records/merge',{method:'POST',body:JSON.stringify({entity_ids:[currentDupData.entity1.id,currentDupData.entity2.id],merged_data:mergedData,duplicate_id:currentDupId})});if(!res)return;toast(`Golden Record créé : ${res.mdm_id}`,'success');closeMergeModal();loadDuplicates();loadDashboard();}

// ── GOLDEN RECORDS ─────────────────────────────────────────────────────────
async function loadGoldenRecords(){const records=await api('/golden-records');const container=document.getElementById('golden-list');if(!records?.length){container.innerHTML='<p style="color:#9ca3af;font-size:13px;">Aucun Golden Record.</p>';return;}const allKeys=[...new Set(records.flatMap(r=>Object.keys(r.data||{})))].slice(0,6);container.innerHTML=`<table style="width:100%;border-collapse:collapse;"><thead><tr><th class="table-th">MDM ID</th>${allKeys.map(k=>`<th class="table-th">${esc(k)}</th>`).join('')}<th class="table-th">Sources</th><th class="table-th">Créé</th></tr></thead><tbody>${records.map(r=>`<tr><td class="table-td"><span style="font-family:monospace;font-size:11px;color:#059669;font-weight:700;">${r.mdm_id}</span></td>${allKeys.map(k=>`<td class="table-td" style="max-width:140px;overflow:hidden;text-overflow:ellipsis;">${esc(String(r.data?.[k]??'—'))}</td>`).join('')}<td class="table-td" style="color:#9ca3af;">${Array.isArray(r.source_ids)?r.source_ids.length+' entités':'—'}</td><td class="table-td" style="color:#9ca3af;font-size:11px;">${fmtDate(r.created_at)}</td></tr>`).join('')}</tbody></table>`;}

// ── FUSION RULES ───────────────────────────────────────────────────────────
async function loadRules(){const rules=await api('/fusion-rules');const container=document.getElementById('rules-list');if(!rules?.length){container.innerHTML='<div class="card" style="text-align:center;color:#9ca3af;padding:40px;">Aucune règle de fusion. Cliquez "+ Nouvelle règle" pour commencer.</div>';return;}const sl={'most_complete':'📏 Plus complet','non_empty':'✅ Non vide','longest':'📐 Plus long','source_priority':'📊 Priorité source','always_entity1':'⬅️ Toujours E1','always_entity2':'➡️ Toujours E2'};container.innerHTML=`<div style="display:flex;flex-direction:column;gap:10px;">${rules.map(r=>{let prio=[];try{prio=JSON.parse(r.source_priority||'[]');}catch{}const isGlobal=r.field==='*';const safe=JSON.stringify(r).replace(/</g,'\\u003c').replace(/>/g,'\\u003e').replace(/'/g,"\\u0027");return`<div class="card" style="display:flex;align-items:center;justify-content:space-between;padding:14px 18px;border-left:3px solid ${isGlobal?'#1B5EA6':'#d97706'};"><div style="display:flex;align-items:center;gap:14px;flex:1;"><div style="width:36px;height:36px;border-radius:8px;background:${r.active?'#e8f0fb':'#f3f4f6'};display:flex;align-items:center;justify-content:center;font-size:16px;">${r.active?'⚡':'💤'}</div><div><p style="font-size:14px;font-weight:700;color:#111827;margin:0;">${esc(r.name)}</p><p style="font-size:12px;color:#9ca3af;margin:2px 0 0;"><span class="badge" style="${isGlobal?'background:#e8f0fb;color:#1B5EA6;':'background:#fef3c7;color:#92400e;'}">${isGlobal?'🌐 Tous les champs':'🎯 '+esc(r.field)}</span> · <span class="rule-strategy-tag">${sl[r.strategy]||r.strategy}</span>${prio.length?' · Sources: '+prio.join(' → '):''}</p></div></div><div style="display:flex;gap:6px;"><button class="btn btn-secondary" style="font-size:11px;padding:4px 10px;" onclick='editRule(${safe})'>✏️</button><button class="btn btn-danger" style="font-size:11px;padding:4px 10px;" onclick="deleteRule('${r.id}')">🗑️</button></div></div>`;}).join('')}</div>`;}

function updateStratStyle(radio){
  document.querySelectorAll('.strat-label').forEach(l=>{l.style.borderColor='#e5e7eb';l.style.background='#fff';});
  radio.closest('label').style.borderColor='#1B5EA6';radio.closest('label').style.background='#e8f0fb';
  toggleSourcePriority();
}
function openRuleModal(rule=null){currentRuleId=rule?.id||null;document.getElementById('rule-modal-title').textContent=rule?'Modifier règle':'Nouvelle règle';document.getElementById('r-name').value=rule?.name||'';
  // Portée
  const isGlobal=!rule||rule.field==='*';
  document.querySelector('input[name="r-scope"][value="'+(isGlobal?'all':'field')+'"]').checked=true;
  document.getElementById('r-field-custom-wrap').style.display=isGlobal?'none':'block';
  const fieldInputs=document.querySelectorAll('#r-field');fieldInputs.forEach(f=>f.value=rule?.field||'*');
  // Style portée
  document.getElementById('scope-all-label').style.borderColor=isGlobal?'#1B5EA6':'#e5e7eb';
  document.getElementById('scope-all-label').style.background=isGlobal?'#e8f0fb':'#fff';
  document.getElementById('scope-field-label').style.borderColor=isGlobal?'#e5e7eb':'#1B5EA6';
  document.getElementById('scope-field-label').style.background=isGlobal?'#fff':'#e8f0fb';
  // Stratégie
  const strat=rule?.strategy||'most_complete';
  const stratRadio=document.querySelector(`input[name="r-strat"][value="${strat}"]`);
  if(stratRadio){stratRadio.checked=true;updateStratStyle(stratRadio);}
  let prio=[];try{prio=JSON.parse(rule?.source_priority||'[]');}catch{}
  document.getElementById('r-priority').value=prio.join(', ');
  document.getElementById('r-active').value=rule?.active!==undefined?String(rule.active):'1';
  toggleSourcePriority();document.getElementById('rule-modal').style.display='flex';}
function editRule(rule){openRuleModal(rule);}
function closeRuleModal(){document.getElementById('rule-modal').style.display='none';currentRuleId=null;}
function toggleSourcePriority(){const strat=document.querySelector('input[name="r-strat"]:checked')?.value||'';document.getElementById('source-priority-wrap').style.display=strat==='source_priority'?'block':'none';}
async function saveRule(){const prio=document.getElementById('r-priority').value.split(',').map(s=>s.trim()).filter(Boolean);
  const scope=document.querySelector('input[name="r-scope"]:checked')?.value||'all';
  const fieldInputs=document.querySelectorAll('#r-field');
  let field='*';if(scope==='field'){fieldInputs.forEach(f=>{if(f.value&&f.value!=='*')field=f.value.trim();});}
  const body={name:document.getElementById('r-name').value.trim(),field:field,strategy:document.querySelector('input[name="r-strat"]:checked')?.value||'most_complete',source_priority:prio,active:parseInt(document.getElementById('r-active').value)};if(!body.name){toast('Nom requis','error');return;}if(currentRuleId)await api(`/fusion-rules/${currentRuleId}`,{method:'PUT',body:JSON.stringify(body)});else await api('/fusion-rules',{method:'POST',body:JSON.stringify(body)});toast(currentRuleId?'Mise à jour':'Règle créée');closeRuleModal();loadRules();}
async function deleteRule(id){if(!confirm('Supprimer ?'))return;await api(`/fusion-rules/${id}`,{method:'DELETE'});toast('Supprimée','warning');loadRules();}

// ── REPORTING ──────────────────────────────────────────────────────────────
let reportData=null;
async function loadReporting(){reportData=await api('/reporting/overview');if(!reportData)return;renderBIKPIs(reportData.kpis);renderCharts(reportData);const fields=await api('/reporting/fields');if(fields?.fields){['pivot-row','pivot-col','pivot-value'].forEach(id=>{const sel=document.getElementById(id);if(!sel)return;const hasNone=id!=='pivot-row';sel.innerHTML=hasNone?'<option value="">— Aucun —</option>':'';fields.fields.forEach(f=>{const o=document.createElement('option');o.value=f;o.textContent=f;sel.appendChild(o);});});}}
function renderBIKPIs(kpis){const el=document.getElementById('bi-kpis');if(!el)return;const items=[{label:'Entités actives',value:kpis.total_entities,color:'#1B5EA6'},{label:'Lignes importées',value:(kpis.total_rows_imported||0).toLocaleString(),color:'#374151'},{label:'Taux doublons',value:kpis.duplicate_rate+'%',color:'#dc2626'},{label:'Golden Records',value:kpis.total_golden_records,color:'#059669'},{label:'Couverture GR',value:kpis.golden_coverage+'%',color:'#7c3aed'},{label:'Doublons résolus',value:kpis.total_duplicates_resolved,color:'#d97706'},{label:'Ignorés',value:kpis.total_duplicates_ignored,color:'#6b7280'},{label:'Imports',value:kpis.total_imports,color:'#1B5EA6'}];el.innerHTML=items.map(i=>`<div class="kpi-card" style="border-left:3px solid ${i.color};"><p style="font-size:11px;color:#6b7280;font-weight:600;margin:0 0 4px;">${i.label}</p><p style="font-size:24px;font-weight:800;color:${i.color};margin:0;">${i.value}</p></div>`).join('');}
function renderCharts(data){const blue='#1B5EA6',green='#059669',red='#ef4444';const mk=(id,type,labels,datasets)=>{if(charts[id])charts[id].destroy();const ctx=document.getElementById(id);if(!ctx)return;charts[id]=new Chart(ctx,{type,data:{labels,datasets},options:{responsive:true,maintainAspectRatio:false,plugins:{legend:{display:type==='doughnut',position:'bottom',labels:{boxWidth:10,font:{size:11}}}},scales:type!=='doughnut'?{x:{ticks:{font:{size:10}}},y:{ticks:{font:{size:10}},beginAtZero:true}}:{}}});};mk('chart-imports','bar',data.import_trend.map(r=>r.day),[{label:'Lignes',data:data.import_trend.map(r=>r.rows),backgroundColor:blue+'33',borderColor:blue,borderWidth:2,borderRadius:4}]);mk('chart-sources','doughnut',data.by_source.slice(0,8).map(s=>s.source||'—'),[{data:data.by_source.slice(0,8).map(s=>s.count),backgroundColor:['#1B5EA6','#3b82f6','#60a5fa','#93c5fd','#059669','#34d399','#f59e0b','#ef4444'],borderWidth:2}]);mk('chart-dups','line',data.dup_trend.map(r=>r.day),[{label:'Doublons',data:data.dup_trend.map(r=>r.found),borderColor:red,backgroundColor:red+'22',fill:true,tension:.4,pointRadius:3}]);mk('chart-gr','line',data.gr_trend.map(r=>r.day),[{label:'GR créés',data:data.gr_trend.map(r=>r.created),borderColor:green,backgroundColor:green+'22',fill:true,tension:.4,pointRadius:3}]);}
function switchReportTab(tab,btn){document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));btn.classList.add('active');['overview','pivot','pdf'].forEach(t=>{const el=document.getElementById(`report-${t}`);if(el)el.style.display=t===tab?'block':'none';});if(tab==='overview'&&reportData)renderCharts(reportData);}
async function runPivot(){const row_field=document.getElementById('pivot-row').value;const col_field=document.getElementById('pivot-col').value||null;const aggregation=document.getElementById('pivot-agg').value;const value_field=document.getElementById('pivot-value').value||null;if(!row_field){toast('Champ ligne requis','error');return;}const res=await api('/reporting/pivot',{method:'POST',body:JSON.stringify({row_field,col_field,aggregation,value_field})});const container=document.getElementById('pivot-result');if(res?.error){container.innerHTML=`<p style="color:#dc2626;font-size:13px;">❌ ${res.error}</p>`;return;}if(!res?.pivot?.length){container.innerHTML='<p style="color:#9ca3af;">Aucun résultat.</p>';return;}const cols=res.columns;container.innerHTML=`<p style="font-size:12px;color:#9ca3af;margin:0 0 10px;">${res.pivot.length} ligne(s)</p><div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;"><thead><tr>${cols.map(c=>`<th class="table-th">${esc(String(c))}</th>`).join('')}</tr></thead><tbody>${res.pivot.map(row=>`<tr>${cols.map(c=>`<td class="table-td">${esc(String(row[c]??''))}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;}
async function exportPDF(){const data=await api('/reporting/export-pdf-data');if(!data)return;const html=`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>O.S MDM Rapport</title><style>body{font-family:system-ui;padding:30px;color:#111;max-width:900px;margin:0 auto;}h1{color:#1B5EA6;}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:28px;}.kpi{background:#f9fafb;border-radius:8px;padding:14px;border-left:3px solid #1B5EA6;}.kpi .val{font-size:24px;font-weight:800;color:#1B5EA6;margin:0;}.kpi .lbl{font-size:11px;color:#9ca3af;margin:0 0 4px;}table{width:100%;border-collapse:collapse;font-size:13px;margin-top:14px;}th{background:#f3f4f6;padding:8px 12px;text-align:left;font-size:11px;font-weight:700;color:#6b7280;}td{padding:8px 12px;border-top:1px solid #f3f4f6;}.print-btn{background:#1B5EA6;color:#fff;border:none;padding:10px 20px;border-radius:8px;cursor:pointer;font-size:14px;margin-top:20px;}@media print{.print-btn{display:none;}}</style></head><body><h1>O.S MDM V2 — Rapport analytique</h1><p style="color:#9ca3af;">${data.generated_at}</p><div class="grid"><div class="kpi"><p class="lbl">Entités actives</p><p class="val">${data.kpis.total_entities}</p></div><div class="kpi"><p class="lbl">Golden Records</p><p class="val">${data.kpis.total_golden}</p></div><div class="kpi"><p class="lbl">Doublons</p><p class="val">${data.kpis.total_duplicates}</p></div><div class="kpi"><p class="lbl">Imports</p><p class="val">${data.kpis.total_imports}</p></div></div><h2>Répartition par source</h2><table><thead><tr><th>Source</th><th>Entités</th></tr></thead><tbody>${(data.by_source||[]).map(s=>`<tr><td>${esc(s.source||'—')}</td><td>${s.count}</td></tr>`).join('')}</tbody></table><h2>Imports 30j</h2><table><thead><tr><th>Date</th><th>Lignes</th></tr></thead><tbody>${(data.import_trend||[]).map(r=>`<tr><td>${r.day}</td><td>${r.rows}</td></tr>`).join('')}</tbody></table><button class="print-btn" onclick="window.print()">🖨️ Imprimer / PDF</button></body></html>`;const win=window.open('','_blank');win.document.write(html);win.document.close();setTimeout(()=>win.print(),600);toast('Rapport ouvert');}

// ── EXPORT ─────────────────────────────────────────────────────────────────
async function exportEntities(){const source=document.getElementById('export-source')?.value||'';const merged=document.getElementById('export-merged')?.checked||false;const params=new URLSearchParams();if(source)params.set('source',source);if(merged)params.set('include_merged','true');const res=await fetch(`${API}/export/csv?${params}`,{headers:{'Authorization':`Bearer ${TOKEN}`}});if(!res.ok){toast('Aucune entité','error');return;}await dlBlob(res);toast('Export téléchargé ✅');}
async function exportGolden(){const res=await fetch(`${API}/export/golden-records/csv`,{headers:{'Authorization':`Bearer ${TOKEN}`}});if(!res.ok){toast('Aucun Golden Record','error');return;}await dlBlob(res);toast('Golden Records exportés ✅');}
async function dlBlob(res){const blob=await res.blob();const url=URL.createObjectURL(blob);const a=document.createElement('a');const cd=res.headers.get('Content-Disposition')||'';const m=cd.match(/filename=(.+)/);a.href=url;a.download=m?m[1]:'export.csv';a.click();URL.revokeObjectURL(url);}

// ── COPILOT IA ─────────────────────────────────────────────────────────────
let copilotHistory = [];
let copilotOpen = false;
let copilotChart = null;

function toggleCopilot(){
  copilotOpen = !copilotOpen;
  const panel = document.getElementById('copilot-panel');
  const btn = document.getElementById('copilot-toggle');
  if(copilotOpen){
    panel.style.display='flex';
    btn.style.transform='scale(1.1)';
    document.getElementById('copilot-input')?.focus();
    // Charger les champs doublons si on est sur cette section
    if(document.getElementById('dup-source-filter')&&!document.getElementById('dup-field-select').options.length){
      loadDupFields();
    }
  } else {
    panel.style.display='none';
    btn.style.transform='scale(1)';
  }
}

async function copilotSend(msg){
  const input = document.getElementById('copilot-input');
  const text = msg || input?.value.trim();
  if(!text) return;
  if(input) input.value='';

  // Message utilisateur
  appendCopilotMsg(text, 'user');
  copilotHistory.push({role:'user', content:text});

  // Indicateur chargement
  const loadId = 'cop-load-'+Date.now();
  appendCopilotMsg('⏳ Réflexion en cours…', 'assistant', loadId);

  // Contexte page actuelle
  const context = {
    current_section: document.querySelector('.section[style*="block"]')?.id || 'unknown',
    dup_fields: [...(document.getElementById('dup-field-select')?.selectedOptions||[])].map(o=>o.value),
  };

  const res = await api('/ai/chat', {method:'POST', body:JSON.stringify({messages:copilotHistory, context})});

  // Supprimer le loader
  document.getElementById(loadId)?.remove();

  if(!res){
    appendCopilotMsg('❌ Erreur de connexion au Copilot.', 'assistant');
    return;
  }

  copilotHistory.push({role:'assistant', content:res.response});
  appendCopilotMsg(res.response, 'assistant');

  // Exécuter les actions
  if(res.actions?.length){
    res.actions.forEach(action => executeCopilotAction(action));
  }
}

function appendCopilotMsg(text, role, id){
  const container = document.getElementById('copilot-messages');
  if(!container) return;
  const div = document.createElement('div');
  div.className = `cop-msg cop-${role}`;
  if(id) div.id = id;
  div.innerHTML = text.replace(/\n/g,'<br>');
  container.appendChild(div);
  container.scrollTop = container.scrollHeight;
}

function executeCopilotAction(action){
  switch(action.type){
    case 'navigate':
      showSection(action.section);
      toast(`Navigation → ${action.section}`,'info');
      break;
    case 'detect_duplicates':
      showSection('duplicates');
      const methodEl = document.getElementById('dup-method');
      if(methodEl) methodEl.value = action.method||'exact';
      // Sélectionner les champs dans le select multiple
      if(action.fields?.length){
        const sel = document.getElementById('dup-field-select');
        if(sel){
          [...sel.options].forEach(o=>{o.selected=action.fields.includes(o.value);});
        }
      }
      setTimeout(()=>detectDuplicates(), 500);
      break;
    case 'show_chart':
      showCopilotChart(action);
      break;
  }
}

function showCopilotChart(cfg){
  const container = document.getElementById('copilot-messages');
  if(!container) return;
  const canvasId = 'cop-chart-'+Date.now();
  const wrap = document.createElement('div');
  wrap.className = 'cop-msg cop-assistant cop-chart-container';
  wrap.innerHTML = `<p style="font-size:11px;font-weight:700;color:#374151;margin:0 0 8px;">${cfg.title||'Graphique'}</p><canvas id="${canvasId}" style="max-height:200px;"></canvas>`;
  container.appendChild(wrap);
  container.scrollTop = container.scrollHeight;
  setTimeout(()=>{
    const ctx = document.getElementById(canvasId)?.getContext('2d');
    if(!ctx) return;
    if(copilotChart) copilotChart.destroy();
    copilotChart = new Chart(ctx, {
      type: cfg.chart_type||'bar',
      data:{
        labels: cfg.labels||[],
        datasets:[{label:cfg.title||'',data:cfg.data||[],backgroundColor:cfg.color||'#1B5EA6',borderColor:cfg.color||'#1B5EA6',borderWidth:1}]
      },
      options:{responsive:true,plugins:{legend:{display:false}},scales:cfg.chart_type==='pie'||cfg.chart_type==='doughnut'?{}:{y:{beginAtZero:true}}}
    });
  }, 100);
}

// ── SSO ────────────────────────────────────────────────────────────────────
function loginWithGoogle(){
  const GOOGLE_CLIENT_ID = window.GOOGLE_CLIENT_ID || '';
  if(!GOOGLE_CLIENT_ID){
    toast('Clé Google Client ID non configurée — voir paramètres admin','warning');
    // Afficher instructions
    document.getElementById('login-error').style.display='block';
    document.getElementById('login-error').textContent='Google SSO : configurez GOOGLE_CLIENT_ID dans les variables d\'environnement.';
    return;
  }
  google.accounts.id.initialize({
    client_id: GOOGLE_CLIENT_ID,
    callback: async (resp) => {
      const res = await fetch(`${API}/auth/google/callback`, {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({credential: resp.credential})
      });
      const data = await res.json();
      if(data.token){ TOKEN=data.token; localStorage.setItem('mdm_token',TOKEN); currentUser=data.user; initApp(); }
      else { document.getElementById('login-error').style.display='block'; document.getElementById('login-error').textContent=data.error||'Erreur Google'; }
    }
  });
  google.accounts.id.prompt();
}

function loginWithMicrosoft(){
  const MSAL_CLIENT_ID = window.MSAL_CLIENT_ID || '';
  const MSAL_TENANT_ID = window.MSAL_TENANT_ID || 'common';
  if(!MSAL_CLIENT_ID){
    document.getElementById('login-error').style.display='block';
    document.getElementById('login-error').textContent='Microsoft SSO : configurez MSAL_CLIENT_ID dans les variables d\'environnement.';
    return;
  }
  const msalConfig = {
    auth: { clientId: MSAL_CLIENT_ID, authority: `https://login.microsoftonline.com/${MSAL_TENANT_ID}`, redirectUri: window.location.origin }
  };
  const msalInstance = new msal.PublicClientApplication(msalConfig);
  msalInstance.loginPopup({ scopes: ['User.Read'] }).then(async result => {
    const res = await fetch(`${API}/auth/microsoft/callback`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({access_token: result.accessToken})
    });
    const data = await res.json();
    if(data.token){ TOKEN=data.token; localStorage.setItem('mdm_token',TOKEN); currentUser=data.user; initApp(); }
    else { document.getElementById('login-error').style.display='block'; document.getElementById('login-error').textContent=data.error||'Erreur Microsoft'; }
  }).catch(e=>{ document.getElementById('login-error').style.display='block'; document.getElementById('login-error').textContent='Connexion Microsoft annulée'; });
}

// ── ADMIN USERS ────────────────────────────────────────────────────────────
let currentUserId = null;
const ROLE_LABELS = { admin:'⚙️ Admin', manager:'📊 Manager', viewer:'👁 Viewer' };
const ROLE_COLORS = { admin:'#fef3c7:#92400e', manager:'#e8f0fb:#1B5EA6', viewer:'#f3f4f6:#6b7280' };

async function loadUsers(){
  const rows = await api('/users');
  const container = document.getElementById('users-table');
  if(!rows?.length){ container.innerHTML='<p style="color:#9ca3af;">Aucun utilisateur.</p>'; return; }
  container.innerHTML=`<table style="width:100%;border-collapse:collapse;">
    <thead><tr>
      <th class="table-th">Utilisateur</th>
      <th class="table-th">Email</th>
      <th class="table-th">Rôle</th>
      <th class="table-th">Provider</th>
      <th class="table-th">Statut</th>
      <th class="table-th">Dernière connexion</th>
      <th class="table-th">Actions</th>
    </tr></thead>
    <tbody>${rows.map(u=>{
      const [bg,tc]=(ROLE_COLORS[u.role]||'#f3f4f6:#6b7280').split(':');
      const avatar = u.avatar ? `<img src="${esc(u.avatar)}" style="width:32px;height:32px;border-radius:50%;margin-right:8px;">` 
        : `<div style="width:32px;height:32px;border-radius:50%;background:linear-gradient(135deg,#1B5EA6,#7c3aed);display:inline-flex;align-items:center;justify-content:center;color:#fff;font-weight:700;font-size:13px;margin-right:8px;">${esc((u.name||u.email||'?')[0].toUpperCase())}</div>`;
      return`<tr>
        <td class="table-td"><div style="display:flex;align-items:center;">${avatar}<span style="font-weight:600;">${esc(u.name||'—')}</span></div></td>
        <td class="table-td" style="color:#6b7280;">${esc(u.email)}</td>
        <td class="table-td"><span style="background:${bg};color:${tc};padding:2px 8px;border-radius:20px;font-size:11px;font-weight:600;">${ROLE_LABELS[u.role]||u.role}</span></td>
        <td class="table-td"><span style="font-size:11px;color:#9ca3af;">${u.provider==='google'?'🔵 Google':u.provider==='microsoft'?'🟦 Microsoft':'🔑 Local'}</span></td>
        <td class="table-td"><span style="font-size:11px;padding:2px 8px;border-radius:20px;${u.active?'background:#d1fae5;color:#065f46;':'background:#fee2e2;color:#991b1b;'}">${u.active?'Actif':'Désactivé'}</span></td>
        <td class="table-td" style="font-size:11px;color:#9ca3af;">${u.last_login?fmtDate(u.last_login):'Jamais'}</td>
        <td class="table-td"><div style="display:flex;gap:4px;">
          <button class="btn btn-secondary" style="padding:3px 8px;font-size:11px;" onclick="openUserModal(${JSON.stringify(u).replace(/"/g,'&quot;')})">✏️</button>
          <button class="btn btn-danger" style="padding:3px 8px;font-size:11px;" onclick="deleteUser('${u.id}','${esc(u.email)}')">🗑️</button>
        </div></td>
      </tr>`;
    }).join('')}</tbody>
  </table>`;
}

function openUserModal(user){
  currentUserId = user?.id || null;
  document.getElementById('user-modal-title').textContent = user ? 'Modifier utilisateur' : 'Nouvel utilisateur';
  document.getElementById('um-name').value = user?.name||'';
  document.getElementById('um-email').value = user?.email||'';
  document.getElementById('um-email').disabled = !!user;
  document.getElementById('um-role').value = user?.role||'viewer';
  document.getElementById('um-password').value = '';
  document.getElementById('um-active-wrap').style.display = user ? 'block' : 'none';
  if(user) document.getElementById('um-active').checked = !!user.active;
  document.getElementById('user-modal-error').style.display='none';
  document.getElementById('user-modal').style.display='flex';
}
function closeUserModal(){ document.getElementById('user-modal').style.display='none'; currentUserId=null; }

async function saveUser(){
  const errEl=document.getElementById('user-modal-error');
  const body={
    name: document.getElementById('um-name').value.trim(),
    role: document.getElementById('um-role').value,
  };
  if(!currentUserId) body.email = document.getElementById('um-email').value.trim();
  const pwd = document.getElementById('um-password').value;
  if(pwd) body.password = pwd;
  if(currentUserId) body.active = document.getElementById('um-active').checked;
  if(!body.name){ errEl.textContent='Nom requis'; errEl.style.display='block'; return; }
  if(!currentUserId && !body.email){ errEl.textContent='Email requis'; errEl.style.display='block'; return; }
  const res = currentUserId
    ? await api(`/users/${currentUserId}`, {method:'PUT', body:JSON.stringify(body)})
    : await api('/users', {method:'POST', body:JSON.stringify(body)});
  if(!res) return;
  toast(currentUserId ? 'Utilisateur mis à jour' : 'Utilisateur créé ✅');
  closeUserModal();
  loadUsers();
}

async function deleteUser(id, email){
  if(!confirm(`Supprimer ${email} ?`)) return;
  await api(`/users/${id}`, {method:'DELETE'});
  toast('Utilisateur supprimé','warning');
  loadUsers();
}

// ── PROFIL ─────────────────────────────────────────────────────────────────
async function loadProfile(){
  const u = await api('/auth/me');
  if(!u) return;
  document.getElementById('profile-name').value = u.name||'';
  document.getElementById('profile-email').value = u.email||'';
  document.getElementById('profile-name-display').textContent = u.name||u.email;
  document.getElementById('profile-email-display').textContent = u.email;
  document.getElementById('profile-role-badge').textContent = ROLE_LABELS[u.role]||u.role;
  // Avatar
  const av = document.getElementById('profile-avatar');
  if(u.avatar){ av.innerHTML=`<img src="${esc(u.avatar)}" style="width:100%;height:100%;border-radius:50%;object-fit:cover;">`; }
  else { av.textContent=(u.name||u.email||'A')[0].toUpperCase(); }
  // Masquer le changement de mdp pour SSO
  const pwCard = document.getElementById('profile-password-card');
  if(pwCard) pwCard.style.display = (u.provider==='local'||!u.provider) ? 'block':'none';
}

async function saveProfile(){
  const name = document.getElementById('profile-name').value.trim();
  if(!name){ toast('Nom requis','warning'); return; }
  const res = await api('/auth/profile', {method:'PUT', body:JSON.stringify({name})});
  if(!res) return;
  document.getElementById('user-name').textContent = res.name||res.email;
  document.getElementById('profile-name-display').textContent = res.name;
  toast('Profil mis à jour ✅');
}

async function changePassword(){
  const oldPw = document.getElementById('pw-old').value;
  const newPw = document.getElementById('pw-new').value;
  const conf  = document.getElementById('pw-confirm').value;
  if(newPw !== conf){ toast('Les mots de passe ne correspondent pas','warning'); return; }
  if(newPw.length < 8){ toast('8 caractères minimum','warning'); return; }
  const res = await api('/auth/change-password', {method:'POST', body:JSON.stringify({old_password:oldPw, new_password:newPw})});
  if(!res) return;
  toast('Mot de passe mis à jour ✅');
  document.getElementById('pw-old').value='';
  document.getElementById('pw-new').value='';
  document.getElementById('pw-confirm').value='';
}

// ── CONNECTEURS API / ERP ────────────────────────────────────────────────
let currentConnectorId = null;

async function loadConnectors() {
  // Presets — avec fallback statique si l'API ne répond pas
  const FALLBACK_PRESETS = {
    marinetraffic:{name:'MarineTraffic AIS',icon:'🛰️',description:'Données AIS temps réel',auth_type:'query_param',target_type:'vessel',base_url:'https://services.marinetraffic.com/api/exportvessels/v:8',auth_config:{param_name:'apikey',apikey:''},data_path:''},
    vesselfinder:{name:'VesselFinder',icon:'🚢',description:'Données navires et tracking AIS',auth_type:'query_param',target_type:'vessel',base_url:'https://api.vesselfinder.com/vessels',auth_config:{param_name:'userkey',apikey:''},data_path:''},
    fleetmon:{name:'FleetMon',icon:'📡',description:'Monitoring de flotte',auth_type:'basic',target_type:'vessel',base_url:'https://apiv2.fleetmon.com/regional_tracking',auth_config:{username:'',password:''},data_path:'vessels'},
    odoo:{name:'Odoo ERP',icon:'🏭',description:'ERP — contacts, produits, factures',auth_type:'bearer',target_type:'entity',base_url:'https://your-instance.odoo.com/api/v1',auth_config:{token:''},data_path:'records'},
    salesforce:{name:'Salesforce CRM',icon:'☁️',description:'CRM — comptes, contacts, opportunités',auth_type:'bearer',target_type:'entity',base_url:'https://your-instance.salesforce.com/services/data/v58.0/query',auth_config:{token:''},data_path:'records'},
    sap:{name:'SAP OData',icon:'🔷',description:'ERP SAP — données maîtres via OData',auth_type:'basic',target_type:'entity',base_url:'https://your-sap-host/sap/opu/odata/sap/',auth_config:{username:'',password:''},data_path:'d.results'},
    custom_rest:{name:'API REST personnalisée',icon:'🔗',description:'Connecteur API REST générique',auth_type:'none',target_type:'entity',base_url:'',auth_config:{},data_path:''},
  };
  let presets = await api('/connectors/presets');
  if (!presets || typeof presets !== 'object') presets = FALLBACK_PRESETS;
  const presetsEl = document.getElementById('connector-presets');
  if (presetsEl) {
    presetsEl.innerHTML = Object.entries(presets).map(([key, p]) => `
      <div onclick="openConnectorFromPreset('${key}')" style="border:1px solid #e5e7eb;border-radius:10px;padding:12px;cursor:pointer;transition:all .15s;" onmouseenter="this.style.borderColor='#1B5EA6';this.style.background='#f0f7ff'" onmouseleave="this.style.borderColor='#e5e7eb';this.style.background=''">
        <span style="font-size:24px;">${p.icon||'🔗'}</span>
        <p style="font-size:13px;font-weight:700;color:#111827;margin:4px 0 2px;">${esc(p.name)}</p>
        <p style="font-size:10px;color:#9ca3af;margin:0;">${esc(p.description||'')}</p>
      </div>`).join('');
  }

  // Liste
  const connectors = await api('/connectors');
  const container = document.getElementById('connectors-list');
  if (!connectors?.length) {
    container.innerHTML = '<div class="card" style="text-align:center;color:#9ca3af;padding:40px;"><p style="font-size:32px;">🔗</p><p>Aucun connecteur. Utilisez les presets ci-dessus ou cliquez "+ Nouveau connecteur".</p></div>';
    return;
  }

  const statusStyle = s => s==='ok' ? 'background:#d1fae5;color:#065f46;' : s==='error' ? 'background:#fee2e2;color:#991b1b;' : 'background:#fef3c7;color:#92400e;';
  const statusLabel = s => s==='ok' ? '✅ Connecté' : s==='error' ? '❌ Erreur' : '⏳ Non testé';

  container.innerHTML = `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:14px;">${connectors.map(c => {
    const syncLabel = c.sync_interval_minutes > 0 ? `⏱ Toutes les ${c.sync_interval_minutes} min` : '🖐 Manuel';
    const lastSync = c.last_sync ? fmtDate(c.last_sync) : 'Jamais';
    return `<div class="card" style="padding:16px;">
      <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">
        <span style="font-size:28px;">🔗</span>
        <div style="flex:1;">
          <p style="font-size:14px;font-weight:700;margin:0;">${esc(c.name)}</p>
          <p style="font-size:11px;color:#9ca3af;margin:0;">${esc(c.connector_type)} · ${esc(c.target_type==='vessel'?'Navires':'Entités')}</p>
        </div>
        <span class="badge" style="${statusStyle(c.status)}">${statusLabel(c.status)}</span>
      </div>
      <p style="font-size:11px;color:#6b7280;margin:0 0 4px;">📡 ${esc(c.base_url||'—')}</p>
      <p style="font-size:11px;color:#6b7280;margin:0 0 8px;">${syncLabel} · Dernière synchro : ${lastSync} ${c.last_sync_count>0?`(${c.last_sync_count} lignes)`:''}</p>
      <div style="display:flex;gap:6px;flex-wrap:wrap;">
        <button class="btn btn-secondary" style="font-size:11px;padding:4px 10px;" onclick="testConnector('${c.id}')">🔌 Tester</button>
        <button class="btn btn-primary" style="font-size:11px;padding:4px 10px;" onclick="syncConnector('${c.id}','${esc(c.name)}')">🔄 Synchroniser</button>
        <button class="btn btn-secondary" style="font-size:11px;padding:4px 10px;" onclick="previewConnector('${c.id}')">👁 Aperçu</button>
        <button class="btn btn-secondary" style="font-size:11px;padding:4px 10px;" onclick="editConnector('${c.id}')">✏️</button>
        <button class="btn btn-danger" style="font-size:11px;padding:4px 10px;" onclick="deleteConnector('${c.id}')">🗑️</button>
      </div>
    </div>`;
  }).join('')}</div>`;
}

function toggleAuthFields() {
  const auth = document.getElementById('cx-auth').value;
  const wrap = document.getElementById('cx-auth-fields');
  wrap.style.display = auth === 'none' ? 'none' : 'block';
  document.getElementById('cx-auth-token').style.display = ['bearer','api_key','query_param'].includes(auth) ? 'block' : 'none';
  document.getElementById('cx-auth-header').style.display = auth === 'api_key' ? 'block' : 'none';
  document.getElementById('cx-auth-param').style.display = auth === 'query_param' ? 'block' : 'none';
  document.getElementById('cx-auth-basic').style.display = auth === 'basic' ? 'block' : 'none';
}

function openConnectorModal(conn=null) {
  currentConnectorId = conn?.id || null;
  document.getElementById('connector-modal-title').textContent = conn ? 'Modifier connecteur' : 'Nouveau connecteur';
  document.getElementById('cx-name').value = conn?.name || '';
  document.getElementById('cx-type').value = conn?.connector_type || 'rest_api';
  document.getElementById('cx-target').value = conn?.target_type || 'entity';
  document.getElementById('cx-url').value = conn?.base_url || '';
  document.getElementById('cx-auth').value = conn?.auth_type || 'none';
  document.getElementById('cx-datapath').value = conn?.data_path || '';
  document.getElementById('cx-interval').value = conn?.sync_interval_minutes || 0;
  document.getElementById('cx-mapping').value = conn?.field_mapping ? JSON.stringify(conn.field_mapping, null, 2) : '';
  // Auth fields
  const ac = conn?.auth_config || {};
  document.getElementById('cx-token').value = ac.token || ac.apikey || '';
  document.getElementById('cx-header-name').value = ac.header_name || 'X-API-Key';
  document.getElementById('cx-param-name').value = ac.param_name || 'apikey';
  document.getElementById('cx-basic-user').value = ac.username || '';
  document.getElementById('cx-basic-pass').value = ac.password || '';
  toggleAuthFields();
  document.getElementById('connector-modal').style.display = 'flex';
}

function closeConnectorModal() { document.getElementById('connector-modal').style.display='none'; currentConnectorId=null; }

async function openConnectorFromPreset(key) {
  const FALLBACK_PRESETS = {
    marinetraffic:{name:'MarineTraffic AIS',base_url:'https://services.marinetraffic.com/api/exportvessels/v:8',auth_type:'query_param',auth_config:{param_name:'apikey',apikey:''},data_path:'',target_type:'vessel'},
    vesselfinder:{name:'VesselFinder',base_url:'https://api.vesselfinder.com/vessels',auth_type:'query_param',auth_config:{param_name:'userkey',apikey:''},data_path:'',target_type:'vessel'},
    fleetmon:{name:'FleetMon',base_url:'https://apiv2.fleetmon.com/regional_tracking',auth_type:'basic',auth_config:{username:'',password:''},data_path:'vessels',target_type:'vessel'},
    odoo:{name:'Odoo ERP',base_url:'https://your-instance.odoo.com/api/v1',auth_type:'bearer',auth_config:{token:''},data_path:'records',target_type:'entity'},
    salesforce:{name:'Salesforce CRM',base_url:'https://your-instance.salesforce.com/services/data/v58.0/query',auth_type:'bearer',auth_config:{token:''},data_path:'records',target_type:'entity'},
    sap:{name:'SAP OData',base_url:'https://your-sap-host/sap/opu/odata/sap/',auth_type:'basic',auth_config:{username:'',password:''},data_path:'d.results',target_type:'entity'},
    custom_rest:{name:'API REST personnalisée',base_url:'',auth_type:'none',auth_config:{},data_path:'',target_type:'entity'},
  };
  let presets = await api('/connectors/presets');
  if (!presets || typeof presets !== 'object') presets = FALLBACK_PRESETS;
  const p = presets[key];
  if (!p) return;
  openConnectorModal({
    name: p.name, connector_type: 'rest_api', base_url: p.base_url,
    auth_type: p.auth_type, auth_config: p.auth_config,
    data_path: p.data_path, target_type: p.target_type,
    field_mapping: {}, sync_interval_minutes: 0,
  });
}

function _buildAuthConfig() {
  const auth = document.getElementById('cx-auth').value;
  if (auth === 'bearer') return { token: document.getElementById('cx-token').value };
  if (auth === 'api_key') return { apikey: document.getElementById('cx-token').value, header_name: document.getElementById('cx-header-name').value };
  if (auth === 'query_param') return { apikey: document.getElementById('cx-token').value, param_name: document.getElementById('cx-param-name').value };
  if (auth === 'basic') return { username: document.getElementById('cx-basic-user').value, password: document.getElementById('cx-basic-pass').value };
  return {};
}

async function saveConnector() {
  let mapping = {};
  const mapRaw = document.getElementById('cx-mapping').value.trim();
  if (mapRaw) { try { mapping = JSON.parse(mapRaw); } catch { toast('Mapping JSON invalide','error'); return; } }
  const body = {
    name: document.getElementById('cx-name').value.trim(),
    connector_type: document.getElementById('cx-type').value,
    base_url: document.getElementById('cx-url').value.trim(),
    auth_type: document.getElementById('cx-auth').value,
    auth_config: _buildAuthConfig(),
    headers: {},
    data_path: document.getElementById('cx-datapath').value.trim(),
    field_mapping: mapping,
    target_type: document.getElementById('cx-target').value,
    sync_interval_minutes: parseInt(document.getElementById('cx-interval').value) || 0,
  };
  if (!body.name) { toast('Nom requis','error'); return; }
  if (!body.base_url) { toast('URL requis','error'); return; }
  if (currentConnectorId)
    await api(`/connectors/${currentConnectorId}`, { method:'PUT', body:JSON.stringify(body) });
  else
    await api('/connectors', { method:'POST', body:JSON.stringify(body) });
  toast(currentConnectorId ? 'Connecteur mis à jour' : 'Connecteur créé ✅');
  closeConnectorModal(); loadConnectors();
}

async function editConnector(id) {
  const conns = await api('/connectors');
  const c = conns?.find(x => x.id === id);
  if (c) openConnectorModal(c);
}

async function deleteConnector(id) {
  if (!confirm('Supprimer ce connecteur ?')) return;
  await api(`/connectors/${id}`, { method:'DELETE' });
  toast('Connecteur supprimé','warning'); loadConnectors();
}

async function testConnector(id) {
  toast('Test de connexion…','info');
  const res = await api(`/connectors/${id}/test`, { method:'POST' });
  if (res?.status === 'ok') {
    toast(`${res.message} — ${res.total_records} enregistrement(s)`, 'success');
    if (res.available_fields?.length) {
      toast(`Champs disponibles : ${res.available_fields.slice(0,8).join(', ')}`, 'info');
    }
  } else {
    toast('❌ ' + (res?.message || 'Erreur'), 'error');
  }
  loadConnectors();
}

async function syncConnector(id, name) {
  if (!confirm(`Synchroniser "${name}" maintenant ?`)) return;
  toast('🔄 Synchronisation en cours…','info');
  const res = await api(`/connectors/${id}/sync`, { method:'POST' });
  if (res?.imported !== undefined) {
    toast(`✅ ${res.imported} enregistrement(s) importé(s) (${res.errors||0} erreurs)`, res.errors > 0 ? 'warning' : 'success');
    loadDashboard();
  } else {
    toast('❌ ' + (res?.error || 'Erreur'), 'error');
  }
  loadConnectors();
}

async function previewConnector(id) {
  toast('Chargement aperçu…','info');
  const res = await api(`/connectors/${id}/preview`, { method:'POST' });
  if (!res?.sample?.length) { toast('Aucun résultat','warning'); return; }
  const keys = res.fields?.slice(0,6) || Object.keys(res.sample[0]).slice(0,6);
  const html = `<div style="position:fixed;inset:0;background:rgba(0,0,0,.35);display:flex;align-items:center;justify-content:center;z-index:100;padding:16px;" onclick="if(event.target===this)this.remove()">
    <div style="background:#fff;border-radius:18px;padding:22px;max-width:800px;width:100%;max-height:80vh;overflow:auto;">
      <h3 style="margin:0 0 10px;font-size:15px;">👁 Aperçu — ${res.total} enregistrement(s)</h3>
      <p style="font-size:11px;color:#9ca3af;margin:0 0 10px;">Champs : ${res.fields?.join(', ')}</p>
      <table style="width:100%;border-collapse:collapse;font-size:11px;">
        <thead><tr>${keys.map(k => `<th class="table-th">${esc(k)}</th>`).join('')}</tr></thead>
        <tbody>${res.sample.map(r => `<tr>${keys.map(k => `<td class="table-td">${esc(String(r[k]??''))}</td>`).join('')}</tr>`).join('')}</tbody>
      </table>
      <button class="btn btn-secondary" onclick="this.closest('[style*=fixed]').remove()" style="margin-top:12px;">Fermer</button>
    </div>
  </div>`;
  document.body.insertAdjacentHTML('beforeend', html);
}

// ── WRITE-BACK ────────────────────────────────────────────────────────────
let currentWbId = null;

async function loadWriteback() {
  // Charger configs
  const configs = await api('/writeback/configs');
  const container = document.getElementById('writeback-list');
  if (!configs?.length) {
    container.innerHTML = '<div class="card" style="text-align:center;color:#9ca3af;padding:40px;"><p style="font-size:32px;">📤</p><p>Aucune destination configurée. Cliquez "+ Nouvelle destination" pour commencer.</p></div>';
  } else {
    const statusStyle = s => s==='ok' ? 'background:#d1fae5;color:#065f46;' : s==='error' ? 'background:#fee2e2;color:#991b1b;' : 'background:#fef3c7;color:#92400e;';
    const statusLabel = s => s==='ok' ? '✅ Dernier envoi OK' : s==='error' ? '❌ Erreur' : '⏳ Jamais lancé';
    container.innerHTML = `<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:14px;">${configs.map(c => {
      const isDb = c.target_type === 'db';
      return `<div class="card" style="padding:16px;border-left:3px solid ${isDb?'#1B5EA6':'#7c3aed'};">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">
          <span style="font-size:28px;">${isDb?'🗄️':'🔗'}</span>
          <div style="flex:1;">
            <p style="font-size:14px;font-weight:700;margin:0;">${esc(c.name)}</p>
            <p style="font-size:11px;color:#9ca3af;margin:0;">${isDb ? (c.target_name||'DB') + ' → ' + esc(c.target_table||'') : (c.target_name||'API')} · Mode: ${c.mode||'upsert'}</p>
          </div>
          <span class="badge" style="${statusStyle(c.last_run_status)}">${statusLabel(c.last_run_status)}</span>
        </div>
        ${c.last_run ? `<p style="font-size:11px;color:#6b7280;margin:0 0 8px;">Dernier envoi : ${fmtDate(c.last_run)} — ${c.last_run_count||0} enregistrement(s)</p>` : ''}
        <div style="display:flex;gap:6px;flex-wrap:wrap;">
          <button class="btn btn-secondary" style="font-size:11px;padding:5px 12px;" onclick="dryRunGR('${c.id}','${esc(c.name)}')">🔍 Simuler</button>
          <button class="btn btn-primary" style="font-size:11px;padding:5px 12px;" onclick="pushAllGR('${c.id}','${esc(c.name)}')">📤 Pousser tous les GR</button>
          <button class="btn btn-secondary" style="font-size:11px;padding:4px 10px;" onclick="editWritebackConfig('${c.id}')">✏️</button>
          <button class="btn btn-danger" style="font-size:11px;padding:4px 10px;" onclick="deleteWritebackConfig('${c.id}')">🗑️</button>
        </div>
      </div>`;
    }).join('')}</div>`;
  }
  // Charger logs
  const logs = await api('/writeback/logs');
  const logsEl = document.getElementById('writeback-logs');
  if (logs?.length) {
    logsEl.innerHTML = `<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:11px;">
      <thead><tr><th class="table-th">Date</th><th class="table-th">Destination</th><th class="table-th">Envoyés</th><th class="table-th">OK</th><th class="table-th">Erreurs</th><th class="table-th">Statut</th></tr></thead>
      <tbody>${logs.map(l => `<tr>
        <td class="table-td">${fmtDate(l.created_at)}</td>
        <td class="table-td">${esc(l.config_name||'—')}</td>
        <td class="table-td">${l.records_sent||0}</td>
        <td class="table-td" style="color:#059669;font-weight:600;">${l.records_success||0}</td>
        <td class="table-td" style="color:${l.records_error>0?'#dc2626':'#6b7280'};">${l.records_error||0}</td>
        <td class="table-td"><span class="badge" style="${l.status==='done'?'background:#d1fae5;color:#065f46;':'background:#fee2e2;color:#991b1b;'}">${l.status}</span></td>
      </tr>`).join('')}</tbody>
    </table></div>`;
  }
}

function toggleWbType() {
  const isDb = document.querySelector('input[name="wb-type"]:checked')?.value === 'db';
  document.getElementById('wb-db-fields').style.display = isDb ? 'block' : 'none';
  document.getElementById('wb-api-fields').style.display = isDb ? 'none' : 'block';
  document.getElementById('wb-type-db-label').style.borderColor = isDb ? '#1B5EA6' : '#e5e7eb';
  document.getElementById('wb-type-db-label').style.background = isDb ? '#e8f0fb' : '#fff';
  document.getElementById('wb-type-api-label').style.borderColor = isDb ? '#e5e7eb' : '#1B5EA6';
  document.getElementById('wb-type-api-label').style.background = isDb ? '#fff' : '#e8f0fb';
}

async function openWritebackModal(config=null) {
  currentWbId = config?.id || null;
  document.getElementById('wb-modal-title').textContent = config ? 'Modifier destination' : 'Nouvelle destination';
  document.getElementById('wb-name').value = config?.name || '';
  document.getElementById('wb-endpoint').value = config?.api_endpoint || '';
  document.getElementById('wb-method').value = config?.api_method || 'POST';
  document.getElementById('wb-mode').value = config?.mode || 'upsert';
  document.getElementById('wb-match-key').value = config?.match_key || '';
  document.getElementById('wb-mapping').value = config?.field_mapping ? JSON.stringify(config.field_mapping, null, 2) : '';
  const isApi = config?.target_type === 'api';
  document.querySelector(`input[name="wb-type"][value="${isApi?'api':'db'}"]`).checked = true;
  toggleWbType();
  // Charger TOUTES les connexions DB (pas juste status=ok)
  const allConns = await api('/connections');
  const dbSel = document.getElementById('wb-connection');
  const dbTypeIcons = { postgresql:'🐘', mysql:'🐬', mariadb:'🐬', mssql:'🔷', oracle:'🔶', sqlite:'📁' };
  dbSel.innerHTML = '<option value="">— Sélectionner une connexion —</option>' +
    (allConns||[]).map(d => {
      const icon = dbTypeIcons[(d.db_type||'').toLowerCase()] || '🗄️';
      const statusTag = d.status === 'ok' ? '✅' : d.status === 'error' ? '❌' : '⏳';
      return `<option value="${d.id}" ${config?.connection_id===d.id?'selected':''}>${icon} ${esc(d.name)} (${(d.db_type||'').toUpperCase()}) ${statusTag}</option>`;
    }).join('');
  // Charger les connecteurs API
  const targets = await api('/writeback/targets');
  const apiSel = document.getElementById('wb-connector');
  apiSel.innerHTML = '<option value="">— Sélectionner —</option>' + (targets?.api_connectors||[]).map(a => `<option value="${a.id}" ${config?.connector_id===a.id?'selected':''}>${esc(a.name)}</option>`).join('');
  // Si une connexion est déjà sélectionnée, charger ses tables
  const tableSel = document.getElementById('wb-table');
  tableSel.innerHTML = '<option value="">— Choisir une connexion d\'abord —</option>';
  if (config?.connection_id) {
    await loadWbTables(config.target_table);
  }
  document.getElementById('writeback-modal').style.display = 'flex';
}

async function loadWbTables(preselect) {
  const connId = document.getElementById('wb-connection').value;
  const tableSel = document.getElementById('wb-table');
  const statusEl = document.getElementById('wb-table-status');
  if (!connId) {
    tableSel.innerHTML = '<option value="">— Choisir une connexion d\'abord —</option>';
    if (statusEl) statusEl.textContent = '';
    return;
  }
  tableSel.innerHTML = '<option value="">⏳ Chargement des tables…</option>';
  if (statusEl) statusEl.textContent = 'Connexion en cours…';
  const res = await api(`/connections/${connId}/tables`);
  if (!res?.tables) {
    tableSel.innerHTML = '<option value="">❌ Erreur — connexion impossible</option>';
    if (statusEl) statusEl.textContent = 'Vérifiez que la connexion est active.';
    return;
  }
  tableSel.innerHTML = '<option value="">— Sélectionner une table —</option>' +
    res.tables.map(t => `<option value="${esc(t)}" ${t===(preselect||'') ? 'selected':''}>${esc(t)}</option>`).join('');
  if (statusEl) statusEl.textContent = `${res.tables.length} table(s) trouvée(s)`;
}
function closeWritebackModal() { document.getElementById('writeback-modal').style.display='none'; currentWbId=null; }

async function saveWritebackConfig() {
  let mapping = {};
  const mapRaw = document.getElementById('wb-mapping').value.trim();
  if (mapRaw) { try { mapping = JSON.parse(mapRaw); } catch { toast('Mapping JSON invalide','error'); return; } }
  const isDb = document.querySelector('input[name="wb-type"]:checked')?.value === 'db';
  const body = {
    name: document.getElementById('wb-name').value.trim(),
    target_type: isDb ? 'db' : 'api',
    connection_id: isDb ? document.getElementById('wb-connection').value : null,
    connector_id: isDb ? null : document.getElementById('wb-connector').value,
    target_table: document.getElementById('wb-table').value.trim(),
    api_endpoint: document.getElementById('wb-endpoint').value.trim(),
    api_method: document.getElementById('wb-method').value,
    field_mapping: mapping,
    mode: document.getElementById('wb-mode').value,
    match_key: document.getElementById('wb-match-key').value.trim(),
  };
  if (!body.name) { toast('Nom requis','error'); return; }
  if (isDb && !body.connection_id) { toast('Sélectionnez une connexion DB','error'); return; }
  if (!isDb && !body.connector_id) { toast('Sélectionnez un connecteur API','error'); return; }
  if (currentWbId)
    await api(`/writeback/configs/${currentWbId}`, { method:'PUT', body:JSON.stringify(body) });
  else
    await api('/writeback/configs', { method:'POST', body:JSON.stringify(body) });
  toast(currentWbId ? 'Destination mise à jour' : 'Destination créée ✅');
  closeWritebackModal(); loadWriteback();
}

async function editWritebackConfig(id) {
  const configs = await api('/writeback/configs');
  const c = configs?.find(x => x.id === id);
  if (c) openWritebackModal(c);
}

async function deleteWritebackConfig(id) {
  if (!confirm('Supprimer cette destination ?')) return;
  await api(`/writeback/configs/${id}`, { method:'DELETE' });
  toast('Destination supprimée','warning'); loadWriteback();
}

async function pushAllGR(configId, name) {
  if (!confirm(`Pousser TOUS les Golden Records vers "${name}" ?`)) return;
  toast('📤 Envoi en cours…','info');
  const res = await api('/writeback/push', { method:'POST', body:JSON.stringify({ config_id: configId, push_all: true }) });
  if (res?.success !== undefined) {
    toast(res.message, res.errors > 0 ? 'warning' : 'success');
  } else {
    toast('❌ ' + (res?.error || 'Erreur'), 'error');
  }
  loadWriteback();
}

async function dryRunGR(configId, name) {
  toast('🔍 Simulation en cours…','info');
  const res = await api('/writeback/push', { method:'POST', body:JSON.stringify({ config_id: configId, push_all: true, dry_run: true }) });
  if (!res) { toast('Erreur','error'); return; }
  if (res.error) { toast('❌ ' + res.error, 'error'); return; }

  const summary = res.summary || {};
  const preview = res.preview || [];
  const actionColors = { INSERT:'#059669', UPDATE:'#d97706', SKIP:'#9ca3af', ERROR:'#dc2626', POST:'#1B5EA6', PUT:'#7c3aed', PATCH:'#d97706' };

  const html = `<div style="position:fixed;inset:0;background:rgba(0,0,0,.35);display:flex;align-items:center;justify-content:center;z-index:100;padding:16px;" onclick="if(event.target===this)this.remove()">
    <div style="background:#fff;border-radius:18px;padding:22px;max-width:750px;width:100%;max-height:85vh;overflow:auto;">
      <h3 style="margin:0 0 4px;font-size:16px;">🔍 Simulation — ${esc(name)}</h3>
      <p style="font-size:12px;color:#9ca3af;margin:0 0 14px;">${res.message}</p>
      <!-- Résumé -->
      <div style="display:flex;gap:10px;margin-bottom:14px;">
        ${Object.entries(summary).map(([action, count]) => `
          <div style="background:${actionColors[action]||'#6b7280'}15;border:1px solid ${actionColors[action]||'#6b7280'}40;border-radius:10px;padding:10px 16px;text-align:center;">
            <span style="font-size:22px;font-weight:800;color:${actionColors[action]||'#6b7280'};">${count}</span>
            <p style="font-size:11px;font-weight:600;color:${actionColors[action]||'#6b7280'};margin:2px 0 0;">${action}</p>
          </div>`).join('')}
      </div>
      <!-- Aperçu détaillé -->
      ${preview.length ? `<div style="overflow-x:auto;max-height:350px;"><table style="width:100%;border-collapse:collapse;font-size:11px;">
        <thead><tr><th class="table-th">Action</th><th class="table-th">Clé</th><th class="table-th">Données</th></tr></thead>
        <tbody>${preview.map(p => `<tr>
          <td class="table-td"><span style="background:${actionColors[p.action]||'#6b7280'}20;color:${actionColors[p.action]||'#6b7280'};padding:2px 8px;border-radius:4px;font-weight:700;font-size:10px;">${p.action}</span></td>
          <td class="table-td" style="font-family:monospace;">${esc(p.match_value||p.endpoint||'—')}</td>
          <td class="table-td" style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${esc(JSON.stringify(p.data||{}).substring(0,100))}</td>
        </tr>`).join('')}</tbody>
      </table></div>` : ''}
      <div style="display:flex;gap:8px;margin-top:14px;">
        <button class="btn btn-secondary" onclick="this.closest('[style*=fixed]').remove()" style="flex:1;justify-content:center;">Fermer</button>
        <button class="btn btn-primary" onclick="this.closest('[style*=fixed]').remove();pushAllGR('${configId}','${esc(name)}')" style="flex:1;justify-content:center;">📤 Confirmer l'envoi réel</button>
      </div>
    </div>
  </div>`;
  document.body.insertAdjacentHTML('beforeend', html);
}
