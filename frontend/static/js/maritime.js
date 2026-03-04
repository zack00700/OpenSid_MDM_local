/**
 * O.S MDM V2 — Module Maritime
 * Navires · Armateurs · Ports · Escales
 */

const MAPI = `${window.location.protocol}//${window.location.hostname}:3000/api/maritime`; // via proxy
let vesselPage = 1, ownerPage = 1, portPage = 1, callPage = 1;
let currentVesselId = null, currentOwnerId = null, currentPortId = null, currentCallId = null;
let vesselTimer = null, ownerTimer = null, portTimer = null, callTimer = null;
let maritimeRefs = { flags: [], vesselTypes: [], callStatuses: [] };

// ── §6.3 CACHE SESSION STORAGE ────────────────────────────────────────────
function sessionGet(key) {
  try {
    const raw = sessionStorage.getItem('mdm_' + key);
    if (!raw) return null;
    const entry = JSON.parse(raw);
    if (Date.now() - entry.ts > 300000) { sessionStorage.removeItem('mdm_' + key); return null; } // TTL 5min
    return entry.data;
  } catch { return null; }
}
function sessionSet(key, data) {
  try { sessionStorage.setItem('mdm_' + key, JSON.stringify({ data, ts: Date.now() })); } catch {}
}

// ── INIT MARITIME ─────────────────────────────────────────────────────────
async function initMaritime() {
  // §6.3 — Charger les référentiels depuis sessionStorage si disponible
  let flags = sessionGet('ref_flags');
  let types = sessionGet('ref_types');
  let statuses = sessionGet('ref_statuses');

  if (!flags || !types || !statuses) {
    const [f, t, s] = await Promise.all([
      flags ? Promise.resolve(flags) : api('/maritime/referentials/flags'),
      types ? Promise.resolve(types) : api('/maritime/referentials/vessel-types'),
      statuses ? Promise.resolve(statuses) : api('/maritime/referentials/call-statuses'),
    ]);
    if (f && !flags) { flags = f; sessionSet('ref_flags', f); }
    if (t && !types) { types = t; sessionSet('ref_types', t); }
    if (s && !statuses) { statuses = s; sessionSet('ref_statuses', s); }
  }

  if (flags) maritimeRefs.flags = flags;
  if (types) maritimeRefs.vesselTypes = types;
  if (statuses) maritimeRefs.callStatuses = statuses;

  // Remplir les selects des filtres
  const flagFilter = document.getElementById('vessel-flag');
  if (flagFilter) flags?.forEach(f => {
    const o = document.createElement('option'); o.value=f.code; o.textContent=`${f.code} — ${f.name}`;
    flagFilter.appendChild(o);
  });
  const typeFilter = document.getElementById('vessel-type-filter');
  if (typeFilter) types?.forEach(t => {
    const o = document.createElement('option'); o.value=t; o.textContent=t;
    typeFilter.appendChild(o);
  });

  // Selects dans les modals
  ['v-flag','o-country','p-country'].forEach(id => {
    const sel = document.getElementById(id);
    if (sel) {
      flags?.forEach(f => {
        const o = document.createElement('option'); o.value=f.code; o.textContent=`${f.code} — ${f.name}`;
        sel.appendChild(o);
      });
    }
  });
  const vtSel = document.getElementById('v-type');
  if (vtSel) types?.forEach(t => {
    const o = document.createElement('option'); o.value=t; o.textContent=t;
    vtSel.appendChild(o);
  });
}

// ── MARITIME KPIs ─────────────────────────────────────────────────────────
async function loadMaritimeKPIs() {
  const stats = await api('/maritime/stats');
  if (!stats) return;
  const el = document.getElementById('maritime-kpis');
  if (!el) return;
  const items = [
    { label: 'Navires', value: stats.total_vessels,    color: '#1B5EA6', icon: '🚢' },
    { label: 'Armateurs', value: stats.total_owners,   color: '#7c3aed', icon: '🏢' },
    { label: 'Ports', value: stats.total_ports,        color: '#059669', icon: '⚓' },
    { label: 'Escales actives', value: stats.active_calls, color: '#d97706', icon: '📍' },
  ];
  el.innerHTML = items.map(i => `
    <div class="kpi-card" style="border-left:3px solid ${i.color};">
      <p style="font-size:11px;color:#6b7280;font-weight:600;margin:0 0 4px;">${i.icon} ${i.label}</p>
      <p style="font-size:28px;font-weight:800;color:${i.color};margin:0;">${i.value}</p>
    </div>`).join('');

  // Alertes qualité
  if (stats.validation_errors > 0 || stats.low_confidence > 0) {
    const alertDiv = document.createElement('div');
    alertDiv.style.cssText = 'grid-column:span 4;';
    alertDiv.innerHTML = `<div style="background:#fef3c7;border-radius:10px;padding:10px 14px;font-size:12px;color:#92400e;display:flex;gap:16px;">
      ${stats.validation_errors > 0 ? `<span>⚠️ <b>${stats.validation_errors}</b> navire(s) avec erreurs de validation</span>` : ''}
      ${stats.low_confidence > 0 ? `<span>🔶 <b>${stats.low_confidence}</b> navire(s) avec score de confiance faible</span>` : ''}
    </div>`;
    el.appendChild(alertDiv);
  }
}

// ── VESSELS ───────────────────────────────────────────────────────────────
async function loadVessels() {
  const search = document.getElementById('vessel-search')?.value.trim() || '';
  const flag   = document.getElementById('vessel-flag')?.value || '';
  const vtype  = document.getElementById('vessel-type-filter')?.value || '';
  const params = new URLSearchParams({ page: vesselPage, per_page: 20 });
  if (search) params.set('search', search);
  if (flag)   params.set('flag', flag);
  if (vtype)  params.set('vessel_type', vtype);
  const data = await api(`/maritime/vessels?${params}`);
  if (!data) return;

  const container = document.getElementById('vessels-table');
  if (!data.vessels.length) {
    container.innerHTML = '<p style="color:#9ca3af;font-size:13px;text-align:center;padding:24px;">Aucun navire.</p>';
    return;
  }

  const confBadge = score => {
    if (score >= 0.9) return '<span class="badge" style="background:#d1fae5;color:#065f46;">✅ Élevé</span>';
    if (score >= 0.7) return '<span class="badge" style="background:#fef3c7;color:#92400e;">⚠️ Moyen</span>';
    return '<span class="badge" style="background:#fee2e2;color:#991b1b;">❌ Faible</span>';
  };
  const statusBadge = status => {
    if (!status || status === 'active') return '';
    return `<span class="badge" style="background:#f3f4f6;color:#6b7280;">${status}</span>`;
  };

  container.innerHTML = `
    <div style="display:flex;gap:8px;margin-bottom:10px;align-items:center;">
      <button onclick="clearGoldenSelection()" class="btn btn-secondary" style="font-size:11px;padding:4px 10px;">☐ Désélectionner</button>
      <span style="font-size:11px;color:#9ca3af;">Cochez plusieurs navires pour créer un Golden Record</span>
    </div>
    <div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;">
    <thead><tr>
      <th class="table-th" style="width:32px;"></th>
      <th class="table-th">MDM ID</th>
      <th class="table-th">Navire</th>
      <th class="table-th">IMO</th>
      <th class="table-th">MMSI</th>
      <th class="table-th">Type</th>
      <th class="table-th">Pavillon</th>
      <th class="table-th">GT</th>
      <th class="table-th">Confiance</th>
      <th class="table-th">Actions</th>
    </tr></thead>
    <tbody>${data.vessels.map(v => `<tr>
      <td class="table-td"><span style="font-family:monospace;font-size:10px;color:#1B5EA6;">${v.mdm_id}</span></td>
      <td class="table-td"><span style="font-weight:600;color:#111827;">${esc(v.vessel_name)}</span></td>
      <td class="table-td"><span style="font-family:monospace;font-size:11px;color:#059669;">${esc(v.imo_number||'—')}</span></td>
      <td class="table-td" style="font-family:monospace;font-size:11px;color:#6b7280;">${esc(v.mmsi||'—')}</td>
      <td class="table-td"><span class="badge" style="background:#e8f0fb;color:#1B5EA6;">${esc(v.vessel_type||'—')}</span></td>
      <td class="table-td">${v.flag_code ? `<span title="${esc(v.flag_name||'')}"><b>${esc(v.flag_code)}</b></span>` : '—'}</td>
      <td class="table-td" style="color:#6b7280;">${v.gross_tonnage ? Number(v.gross_tonnage).toLocaleString() : '—'}</td>
      <td class="table-td">${confBadge(v.confidence_score||1)}</td>
      <td class="table-td">
        <div style="display:flex;gap:4px;">
          <button class="btn btn-secondary" style="padding:3px 8px;font-size:11px;" title="Vue 360°" onclick="openVessel360('${v.id}')">🔍</button>
          <button class="btn btn-secondary" style="padding:3px 8px;font-size:11px;" onclick="editVessel('${v.id}')">✏️</button>
          <button class="btn btn-danger" style="padding:3px 8px;font-size:11px;" onclick="deleteVessel('${v.id}')">🗑️</button>
        </div>
      </td>
    </tr>`).join('')}</tbody>
  </table>`;

  document.getElementById('vessel-count').textContent = `${data.total} navire(s)`;
  document.getElementById('vessel-page-info').textContent = `Page ${vesselPage}`;
  document.getElementById('vessel-prev').disabled = vesselPage <= 1;
  document.getElementById('vessel-next').disabled = (vesselPage * 20) >= data.total;
}

function debounceVessels() { clearTimeout(vesselTimer); vesselPage=1; vesselTimer=setTimeout(loadVessels, 400); }
function vesselPrevPage() { if(vesselPage>1){vesselPage--;loadVessels();} }
function vesselNextPage() { vesselPage++;loadVessels(); }

// Validation en temps réel
async function liveValidateIMO() {
  const val = document.getElementById('v-imo').value.trim();
  const el  = document.getElementById('imo-check');
  if (!val) { el.textContent=''; return; }
  const res = await api('/maritime/vessels/validate-imo', {method:'POST', body:JSON.stringify({imo:val})});
  el.textContent = res?.valid ? '✅' : '❌';
  el.title = res?.error || '';
}

async function liveValidateMMSI() {
  const val = document.getElementById('v-mmsi').value.trim();
  const el  = document.getElementById('mmsi-check');
  if (!val) { el.textContent=''; return; }
  const res = await api('/maritime/vessels/validate-mmsi', {method:'POST', body:JSON.stringify({mmsi:val})});
  el.textContent = res?.valid ? '✅' : (res?.error ? '❌' : '');
  el.title = res?.error || '';
}

async function liveValidateLOCODE() {
  const val = document.getElementById('p-locode')?.value.trim();
  const el  = document.getElementById('locode-check');
  if (!el) return;
  if (!val || val.length < 4) { el.textContent=''; return; }
  // Validation client-side simple
  const clean = val.replace(' ','');
  el.textContent = (clean.length===5 && /^[A-Z]{2}[A-Z0-9]{3}$/i.test(clean)) ? '✅' : '❌';
}

function openVesselModal(vessel=null) {
  currentVesselId = vessel?.id || null;
  document.getElementById('vessel-modal-title').textContent = vessel ? `Modifier — ${vessel.vessel_name}` : 'Nouveau navire';
  document.getElementById('v-name').value    = vessel?.vessel_name || '';
  document.getElementById('v-imo').value     = vessel?.imo_number || '';
  document.getElementById('v-mmsi').value    = vessel?.mmsi || '';
  document.getElementById('v-type').value    = vessel?.vessel_type || '';
  document.getElementById('v-flag').value    = vessel?.flag_code || '';
  document.getElementById('v-gt').value      = vessel?.gross_tonnage || '';
  document.getElementById('v-dwt').value     = vessel?.deadweight || '';
  document.getElementById('v-year').value    = vessel?.year_built || '';
  document.getElementById('v-class').value   = vessel?.class_society || '';
  document.getElementById('v-operator').value= vessel?.operator || '';
  document.getElementById('v-source').value  = vessel?.source || '';
  document.getElementById('imo-check').textContent = '';
  document.getElementById('mmsi-check').textContent = '';
  document.getElementById('vessel-modal-errors').style.display = 'none';
  document.getElementById('vessel-modal').style.display = 'flex';
}

async function editVessel(id) {
  const v = await api(`/maritime/vessels/${id}`);
  if (v) openVesselModal(v);
}

function closeVesselModal() {
  document.getElementById('vessel-modal').style.display = 'none'; currentVesselId = null;
}

async function saveVessel() {
  const body = {
    vessel_name:   document.getElementById('v-name').value.trim(),
    imo_number:    document.getElementById('v-imo').value.trim(),
    mmsi:          document.getElementById('v-mmsi').value.trim(),
    vessel_type:   document.getElementById('v-type').value,
    flag_code:     document.getElementById('v-flag').value,
    gross_tonnage: parseFloat(document.getElementById('v-gt').value) || null,
    deadweight:    parseFloat(document.getElementById('v-dwt').value) || null,
    year_built:    parseInt(document.getElementById('v-year').value) || null,
    class_society: document.getElementById('v-class').value.trim(),
    operator:      document.getElementById('v-operator').value.trim(),
    source:        document.getElementById('v-source').value.trim() || 'manuel',
  };
  if (!body.vessel_name) { toast('Nom du navire requis', 'error'); return; }

  const errEl = document.getElementById('vessel-modal-errors');
  let res;
  if (currentVesselId) {
    res = await api(`/maritime/vessels/${currentVesselId}`, { method:'PUT', body:JSON.stringify(body) });
  } else {
    res = await api('/maritime/vessels', { method:'POST', body:JSON.stringify(body) });
  }
  if (!res) return;

  // Afficher avertissements
  if (res.validation_errors?.length) {
    errEl.innerHTML = '⚠️ ' + res.validation_errors.join('<br>⚠️ ');
    errEl.style.display = 'block';
  }
  if (res.potential_duplicates?.length) {
    toast(`⚠️ ${res.potential_duplicates.length} doublon(s) potentiel(s) détecté(s)`, 'warning');
  }
  if (res.error) {
    errEl.textContent = '❌ ' + res.error; errEl.style.display = 'block';
    if (res.existing_id) errEl.innerHTML += ` — <a href="#" onclick="editVessel('${res.existing_id}')">Voir navire existant</a>`;
    return;
  }

  toast(currentVesselId ? 'Navire mis à jour' : 'Navire créé ✅');
  if (!res.validation_errors?.length) closeVesselModal();
  loadVessels(); loadMaritimeKPIs();
}

async function deleteVessel(id) {
  if (!confirm('Supprimer ce navire ?')) return;
  await api(`/maritime/vessels/${id}`, { method:'DELETE' });
  toast('Navire supprimé', 'warning'); loadVessels(); loadMaritimeKPIs();
}

// ── OWNERS ────────────────────────────────────────────────────────────────
async function loadOwners() {
  const search  = document.getElementById('owner-search')?.value.trim() || '';
  const country = document.getElementById('owner-country')?.value || '';
  const params  = new URLSearchParams({ page: ownerPage, per_page: 20 });
  if (search)  params.set('search', search);
  if (country) params.set('country', country);
  const data = await api(`/maritime/owners?${params}`);
  if (!data) return;
  const container = document.getElementById('owners-table');
  if (!data.owners.length) { container.innerHTML = '<p style="color:#9ca3af;font-size:13px;text-align:center;padding:24px;">Aucun armateur.</p>'; return; }
  container.innerHTML = `<table style="width:100%;border-collapse:collapse;">
    <thead><tr>
      <th class="table-th">MDM ID</th><th class="table-th">Armateur</th>
      <th class="table-th">Type</th><th class="table-th">Pays</th>
      <th class="table-th">Ville</th><th class="table-th">Actions</th>
    </tr></thead>
    <tbody>${data.owners.map(o => `<tr>
      <td class="table-td"><span style="font-family:monospace;font-size:10px;color:#7c3aed;">${o.mdm_id}</span></td>
      <td class="table-td"><span style="font-weight:600;">${esc(o.owner_name)}</span></td>
      <td class="table-td"><span class="badge" style="background:#ede9fe;color:#7c3aed;">${esc(o.owner_type||'—')}</span></td>
      <td class="table-td">${o.country_code ? `<b>${esc(o.country_code)}</b> ${esc(o.country_name||'')}` : '—'}</td>
      <td class="table-td" style="color:#6b7280;">${esc(o.city||'—')}</td>
      <td class="table-td"><div style="display:flex;gap:4px;">
        <button class="btn btn-secondary" style="padding:3px 8px;font-size:11px;" onclick="editOwner('${o.id}')">✏️</button>
        <button class="btn btn-danger" style="padding:3px 8px;font-size:11px;" onclick="deleteOwner('${o.id}')">🗑️</button>
      </div></td>
    </tr>`).join('')}</tbody>
  </table>`;
}

function debounceOwners() { clearTimeout(ownerTimer); ownerPage=1; ownerTimer=setTimeout(loadOwners,400); }
function openOwnerModal(owner=null) {
  currentOwnerId = owner?.id || null;
  document.getElementById('owner-modal-title').textContent = owner ? 'Modifier armateur' : 'Nouvel armateur';
  document.getElementById('o-name').value    = owner?.owner_name || '';
  document.getElementById('o-type').value    = owner?.owner_type || 'Shipowner';
  document.getElementById('o-country').value = owner?.country_code || '';
  document.getElementById('o-city').value    = owner?.city || '';
  document.getElementById('o-email').value   = owner?.contact_email || '';
  document.getElementById('o-address').value = owner?.address || '';
  document.getElementById('owner-modal').style.display = 'flex';
}
async function editOwner(id) { const o=await api(`/maritime/owners/${id}`); if(o)openOwnerModal(o); }
function closeOwnerModal() { document.getElementById('owner-modal').style.display='none'; currentOwnerId=null; }
async function saveOwner() {
  const body = { owner_name:document.getElementById('o-name').value.trim(), owner_type:document.getElementById('o-type').value, country_code:document.getElementById('o-country').value, city:document.getElementById('o-city').value.trim(), contact_email:document.getElementById('o-email').value.trim(), address:document.getElementById('o-address').value.trim() };
  if(!body.owner_name){toast('Nom requis','error');return;}
  if(currentOwnerId) await api(`/maritime/owners/${currentOwnerId}`,{method:'PUT',body:JSON.stringify(body)});
  else await api('/maritime/owners',{method:'POST',body:JSON.stringify(body)});
  toast(currentOwnerId?'Armateur mis à jour':'Armateur créé ✅');
  closeOwnerModal(); loadOwners(); loadMaritimeKPIs();
}
async function deleteOwner(id) { if(!confirm('Supprimer ?'))return; await api(`/maritime/owners/${id}`,{method:'DELETE'}); toast('Supprimé','warning'); loadOwners(); }

// ── PORTS ─────────────────────────────────────────────────────────────────
async function loadPorts() {
  const search  = document.getElementById('port-search')?.value.trim() || '';
  const country = document.getElementById('port-country')?.value || '';
  const params  = new URLSearchParams({ page: portPage, per_page: 20 });
  if (search)  params.set('search', search);
  if (country) params.set('country', country);
  const data = await api(`/maritime/ports?${params}`);
  if (!data) return;
  const container = document.getElementById('ports-table');
  if (!data.ports.length) { container.innerHTML = '<p style="color:#9ca3af;font-size:13px;text-align:center;padding:24px;">Aucun port.</p>'; return; }
  container.innerHTML = `<table style="width:100%;border-collapse:collapse;">
    <thead><tr>
      <th class="table-th">MDM ID</th><th class="table-th">Port</th>
      <th class="table-th">UN/LOCODE</th><th class="table-th">Pays</th>
      <th class="table-th">Tirant max</th><th class="table-th">Marées</th><th class="table-th">Actions</th>
    </tr></thead>
    <tbody>${data.ports.map(p => `<tr>
      <td class="table-td"><span style="font-family:monospace;font-size:10px;color:#059669;">${p.mdm_id}</span></td>
      <td class="table-td"><span style="font-weight:600;">${esc(p.port_name)}</span></td>
      <td class="table-td"><span style="font-family:monospace;font-size:12px;color:#059669;font-weight:700;">${esc(p.un_locode||'—')}</span></td>
      <td class="table-td">${p.country_code?`<b>${esc(p.country_code)}</b>`:''} ${esc(p.country_name||'')}</td>
      <td class="table-td" style="color:#6b7280;">${p.max_draft ? p.max_draft+'m' : '—'}</td>
      <td class="table-td">${p.tide_dependent ? '<span class="badge" style="background:#fef3c7;color:#92400e;">⚓ Oui</span>' : '<span style="color:#9ca3af;font-size:12px;">Non</span>'}</td>
      <td class="table-td"><div style="display:flex;gap:4px;">
        <button class="btn btn-secondary" style="padding:3px 8px;font-size:11px;" onclick="editPort('${p.id}')">✏️</button>
        <button class="btn btn-danger" style="padding:3px 8px;font-size:11px;" onclick="deletePort('${p.id}')">🗑️</button>
      </div></td>
    </tr>`).join('')}</tbody>
  </table>`;
}

function debouncePorts() { clearTimeout(portTimer); portPage=1; portTimer=setTimeout(loadPorts,400); }
function openPortModal(port=null) {
  currentPortId = port?.id || null;
  document.getElementById('port-modal-title').textContent = port ? 'Modifier port' : 'Nouveau port';
  document.getElementById('p-name').value   = port?.port_name || '';
  document.getElementById('p-locode').value = port?.un_locode || '';
  document.getElementById('p-country').value= port?.country_code || '';
  document.getElementById('p-lat').value    = port?.latitude || '';
  document.getElementById('p-lon').value    = port?.longitude || '';
  document.getElementById('p-draft').value  = port?.max_draft || '';
  document.getElementById('p-maxsize').value= port?.max_vessel_size || '';
  document.getElementById('p-tide').checked = !!port?.tide_dependent;
  document.getElementById('p-pilot').checked= port?.pilotage_required !== 0;
  document.getElementById('locode-check').textContent = '';
  document.getElementById('port-modal').style.display = 'flex';
}
async function editPort(id) { const p=await api(`/maritime/ports/${id}`); if(p)openPortModal(p); }
function closePortModal() { document.getElementById('port-modal').style.display='none'; currentPortId=null; }
async function savePort() {
  const body = { port_name:document.getElementById('p-name').value.trim(), un_locode:document.getElementById('p-locode').value.trim().toUpperCase(), country_code:document.getElementById('p-country').value, latitude:parseFloat(document.getElementById('p-lat').value)||null, longitude:parseFloat(document.getElementById('p-lon').value)||null, max_draft:parseFloat(document.getElementById('p-draft').value)||null, max_vessel_size:document.getElementById('p-maxsize').value.trim(), tide_dependent:document.getElementById('p-tide').checked, pilotage_required:document.getElementById('p-pilot').checked };
  if(!body.port_name){toast('Nom requis','error');return;}
  let res;
  if(currentPortId) res=await api(`/maritime/ports/${currentPortId}`,{method:'PUT',body:JSON.stringify(body)});
  else res=await api('/maritime/ports',{method:'POST',body:JSON.stringify(body)});
  if(res?.error){toast('❌ '+res.error,'error');return;}
  toast(currentPortId?'Port mis à jour':'Port créé ✅');
  closePortModal(); loadPorts(); loadMaritimeKPIs();
}
async function deletePort(id) { if(!confirm('Supprimer ce port ?'))return; await api(`/maritime/ports/${id}`,{method:'DELETE'}); toast('Port supprimé','warning'); loadPorts(); }

// ── PORT CALLS ────────────────────────────────────────────────────────────
async function loadCalls() {
  const search  = document.getElementById('call-search')?.value.trim() || '';
  const status  = document.getElementById('call-status-filter')?.value || '';
  const params  = new URLSearchParams({ page: callPage, per_page: 20 });
  if (search) params.set('search', search);
  if (status) params.set('status', status);
  const data = await api(`/maritime/port-calls?${params}`);
  if (!data) return;
  const container = document.getElementById('calls-table');
  if (!data.port_calls.length) { container.innerHTML = '<p style="color:#9ca3af;font-size:13px;text-align:center;padding:24px;">Aucune escale.</p>'; return; }

  const statusColor = s => ({ 'Planned':'#1B5EA6,#e8f0fb', 'In Transit':'#d97706,#fef3c7', 'At Anchor':'#7c3aed,#ede9fe', 'Berthed':'#059669,#d1fae5', 'Departed':'#6b7280,#f3f4f6', 'Cancelled':'#dc2626,#fee2e2' }[s] || '#6b7280,#f3f4f6');

  container.innerHTML = `<table style="width:100%;border-collapse:collapse;">
    <thead><tr>
      <th class="table-th">MDM ID</th><th class="table-th">Navire</th><th class="table-th">IMO</th>
      <th class="table-th">Port</th><th class="table-th">LOCODE</th>
      <th class="table-th">ETA</th><th class="table-th">ETD</th>
      <th class="table-th">Statut</th><th class="table-th">Cargaison</th><th class="table-th">Actions</th>
    </tr></thead>
    <tbody>${data.port_calls.map(c => {
      const [fg,bg] = statusColor(c.call_status).split(',');
      return `<tr>
        <td class="table-td"><span style="font-family:monospace;font-size:10px;color:#d97706;">${c.mdm_id}</span></td>
        <td class="table-td"><span style="font-weight:600;">${esc(c.vessel_name||'—')}</span></td>
        <td class="table-td"><span style="font-family:monospace;font-size:11px;color:#059669;">${esc(c.imo_number||'—')}</span></td>
        <td class="table-td">${esc(c.port_name||'—')}</td>
        <td class="table-td"><span style="font-family:monospace;font-size:11px;font-weight:700;color:#059669;">${esc(c.un_locode||'—')}</span></td>
        <td class="table-td" style="font-size:11px;color:#6b7280;white-space:nowrap;">${c.eta ? new Date(c.eta).toLocaleString('fr-FR',{day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'}) : '—'}</td>
        <td class="table-td" style="font-size:11px;color:#6b7280;white-space:nowrap;">${c.etd ? new Date(c.etd).toLocaleString('fr-FR',{day:'2-digit',month:'short',hour:'2-digit',minute:'2-digit'}) : '—'}</td>
        <td class="table-td"><span class="badge" style="color:${fg};background:${bg};">${c.call_status}</span></td>
        <td class="table-td" style="color:#6b7280;font-size:12px;">${esc(c.cargo_type||'—')} ${c.cargo_quantity?`(${Number(c.cargo_quantity).toLocaleString()} ${c.cargo_unit||'MT'})`:''}  </td>
        <td class="table-td"><div style="display:flex;gap:4px;">
          <button class="btn btn-secondary" style="padding:3px 8px;font-size:11px;" onclick="editCall('${c.id}')">✏️</button>
          <button class="btn btn-danger" style="padding:3px 8px;font-size:11px;" onclick="deleteCall('${c.id}')">🗑️</button>
        </div></td>
      </tr>`;
    }).join('')}</tbody>
  </table>`;
}

function debounceCalls() { clearTimeout(callTimer); callPage=1; callTimer=setTimeout(loadCalls,400); }
function openCallModal(call=null) {
  currentCallId = call?.id || null;
  document.getElementById('call-modal-title').textContent = call ? 'Modifier escale' : 'Nouvelle escale';
  document.getElementById('c-vessel').value  = call?.vessel_name || call?.imo_number || '';
  document.getElementById('c-port').value    = call?.port_name || call?.un_locode || '';
  document.getElementById('c-eta').value     = call?.eta ? call.eta.slice(0,16) : '';
  document.getElementById('c-etd').value     = call?.etd ? call.etd.slice(0,16) : '';
  document.getElementById('c-status').value  = call?.call_status || 'Planned';
  document.getElementById('c-cargo').value   = call?.cargo_type || '';
  document.getElementById('c-qty').value     = call?.cargo_quantity || '';
  document.getElementById('c-voyage').value  = call?.voyage_number || '';
  document.getElementById('c-terminal').value= call?.terminal || '';
  document.getElementById('c-agent').value   = call?.agent_name || '';
  document.getElementById('call-modal').style.display = 'flex';
}
async function editCall(id) { const c=await api(`/maritime/port-calls/${id}`); if(c)openCallModal(c); }
function closeCallModal() { document.getElementById('call-modal').style.display='none'; currentCallId=null; }
async function saveCall() {
  const vesselRaw = document.getElementById('c-vessel').value.trim();
  const portRaw   = document.getElementById('c-port').value.trim();
  const body = {
    vessel_name: vesselRaw.startsWith('IMO') ? undefined : vesselRaw,
    imo_number:  vesselRaw.startsWith('IMO') ? vesselRaw : undefined,
    port_name:   portRaw.length===5||portRaw.includes(' ') ? undefined : portRaw,
    un_locode:   (portRaw.length===5||portRaw.includes(' ')) ? portRaw.replace(' ','').replace(/(.{2})/,'$1 ').trim().toUpperCase() : undefined,
    eta:         document.getElementById('c-eta').value || undefined,
    etd:         document.getElementById('c-etd').value || undefined,
    call_status: document.getElementById('c-status').value,
    cargo_type:  document.getElementById('c-cargo').value,
    cargo_quantity:parseFloat(document.getElementById('c-qty').value)||null,
    voyage_number:document.getElementById('c-voyage').value.trim(),
    terminal:    document.getElementById('c-terminal').value.trim(),
    agent_name:  document.getElementById('c-agent').value.trim(),
  };
  let res;
  if(currentCallId) res=await api(`/maritime/port-calls/${currentCallId}`,{method:'PUT',body:JSON.stringify(body)});
  else res=await api('/maritime/port-calls',{method:'POST',body:JSON.stringify(body)});
  if(res?.error){toast('❌ '+res.error,'error');return;}
  if(res?.validation_errors?.length) toast('⚠️ '+res.validation_errors[0],'warning');
  else toast(currentCallId?'Escale mise à jour':'Escale créée ✅');
  closeCallModal(); loadCalls(); loadMaritimeKPIs();
}
async function deleteCall(id) { if(!confirm('Supprimer cette escale ?'))return; await api(`/maritime/port-calls/${id}`,{method:'DELETE'}); toast('Escale supprimée','warning'); loadCalls(); }

// ══════════════════════════════════════════════════════════════════════════
// DASHBOARD MARITIME
// ══════════════════════════════════════════════════════════════════════════
let marCharts = {};

async function loadMaritimeDashboard() {
  await initMaritime();
  const data = await api('/maritime/dashboard').catch(() => null);
  if (!data) return;

  // ── KPIs ──
  const kpis = data.kpis || {};
  const kpiDefs = [
    { label:'Navires actifs',     val:kpis.total_vessels,  color:'#1B5EA6', icon:'🚢' },
    { label:'Escales actives',    val:kpis.active_calls,   color:'#059669', icon:'📍' },
    { label:'Escales planifiées', val:kpis.planned_calls,  color:'#d97706', icon:'📅' },
    { label:'Armateurs',          val:kpis.total_owners,   color:'#7c3aed', icon:'🏢' },
    { label:'Ports',              val:kpis.total_ports,    color:'#0891b2', icon:'⚓' },
    { label:'Total escales',      val:kpis.total_calls,    color:'#374151', icon:'📊' },
    { label:'Qualité faible',     val:kpis.low_confidence, color:'#dc2626', icon:'⚠️' },
    { label:'Doublons potentiels',val:kpis.duplicates,     color:'#ea580c', icon:'🔄' },
  ];
  const kpiEl = document.getElementById('mar-dash-kpis');
  if (kpiEl) {
    kpiEl.style.gridTemplateColumns = 'repeat(4,1fr)';
    kpiEl.innerHTML = kpiDefs.slice(0,8).map(k => `
      <div class="kpi-card" style="border-left:3px solid ${k.color};">
        <p style="font-size:11px;color:#6b7280;font-weight:600;margin:0 0 4px;">${k.icon} ${k.label}</p>
        <p style="font-size:28px;font-weight:800;color:${k.color};margin:0;">${k.val ?? 0}</p>
      </div>`).join('');
  }

  // ── Alertes ──
  const alertEl = document.getElementById('mar-dash-alerts');
  if (alertEl) {
    const alerts = [];
    if (kpis.low_confidence > 0) alerts.push(`⚠️ <b>${kpis.low_confidence}</b> navire(s) avec score de confiance faible`);
    if (kpis.duplicates > 0) alerts.push(`🔄 <b>${kpis.duplicates}</b> doublon(s) potentiel(s) — <a href="#" onclick="detectMaritimeDuplicates()" style="color:#92400e;font-weight:700;">Détecter</a>`);
    alertEl.innerHTML = alerts.length
      ? `<div style="background:#fef3c7;border-radius:10px;padding:12px 16px;font-size:12px;color:#92400e;display:flex;gap:20px;flex-wrap:wrap;">${alerts.join(' &nbsp;|&nbsp; ')}</div>`
      : '';
  }

  // ── Charts ──
  const COLORS = ['#1B5EA6','#7c3aed','#059669','#d97706','#dc2626','#0891b2','#ea580c','#6b7280'];

  function renderChart(id, type, labels, values, opts = {}) {
    const canvas = document.getElementById(id);
    if (!canvas) return;
    if (marCharts[id]) marCharts[id].destroy();
    marCharts[id] = new Chart(canvas.getContext('2d'), {
      type,
      data: {
        labels,
        datasets: [{ label: opts.label||'', data: values,
          backgroundColor: type==='line' ? 'rgba(27,94,166,.15)' : COLORS,
          borderColor: type==='line' ? '#1B5EA6' : COLORS,
          borderWidth: type==='line' ? 2 : 1,
          fill: type==='line', tension: 0.4, pointRadius: 3 }]
      },
      options: { responsive: true, plugins: { legend: { display: type!=='bar' && type!=='line' } },
        scales: (type==='pie'||type==='doughnut') ? {} : { y: { beginAtZero: true } }
      }
    });
  }

  // Timeline escales
  if (data.calls_timeline?.length) {
    renderChart('mar-calls-timeline', 'line',
      data.calls_timeline.map(r => r.day),
      data.calls_timeline.map(r => r.count),
      { label: 'Escales' }
    );
  }

  // Statuts escales
  if (data.calls_by_status?.length) {
    renderChart('mar-calls-status', 'doughnut',
      data.calls_by_status.map(r => r.call_status),
      data.calls_by_status.map(r => r.count)
    );
  }

  // Pavillons
  if (data.vessels_by_flag?.length) {
    renderChart('mar-flag-chart', 'bar',
      data.vessels_by_flag.map(r => r.flag_name),
      data.vessels_by_flag.map(r => r.count)
    );
  }

  // Top ports
  if (data.top_ports?.length) {
    renderChart('mar-ports-chart', 'bar',
      data.top_ports.map(r => r.port_name || r.locode || '—'),
      data.top_ports.map(r => r.call_count)
    );
  }

  // Cargaisons
  if (data.cargo_distribution?.length) {
    renderChart('mar-cargo-chart', 'doughnut',
      data.cargo_distribution.map(r => r.cargo_type),
      data.cargo_distribution.map(r => r.count)
    );
  }

  // Escales récentes
  const recentEl = document.getElementById('mar-recent-calls');
  if (recentEl && data.recent_calls?.length) {
    const STATUS_COLOR = { Berthed:'#059669', 'At Anchor':'#d97706', 'In Transit':'#1B5EA6', Planned:'#6b7280', Departed:'#9ca3af', Cancelled:'#dc2626' };
    recentEl.innerHTML = `<table style="width:100%;border-collapse:collapse;">
      <thead><tr>
        <th class="table-th">Navire</th><th class="table-th">Port</th>
        <th class="table-th">ETA</th><th class="table-th">ETD</th>
        <th class="table-th">Statut</th><th class="table-th">Cargaison</th>
      </tr></thead>
      <tbody>${data.recent_calls.map(c => `<tr>
        <td class="table-td" style="font-weight:600;">${esc(c.vessel_name_snapshot||'—')}</td>
        <td class="table-td">${esc(c.port_name_snapshot||'—')}</td>
        <td class="table-td" style="font-size:11px;">${c.eta ? c.eta.substring(0,16) : '—'}</td>
        <td class="table-td" style="font-size:11px;">${c.etd ? c.etd.substring(0,16) : '—'}</td>
        <td class="table-td"><span style="background:${(STATUS_COLOR[c.call_status]||'#6b7280')}20;color:${STATUS_COLOR[c.call_status]||'#6b7280'};padding:2px 8px;border-radius:20px;font-size:11px;font-weight:600;">${esc(c.call_status||'—')}</span></td>
        <td class="table-td" style="font-size:11px;color:#6b7280;">${esc(c.cargo_type||'—')}</td>
      </tr>`).join('')}</tbody>
    </table>`;
  } else if (recentEl) {
    recentEl.innerHTML = '<p style="color:#9ca3af;font-size:13px;text-align:center;padding:20px;">Aucune escale enregistrée.</p>';
  }
  // §4.3 — Charger le dashboard qualité
  loadQualityDashboard();
}

// ══════════════════════════════════════════════════════════════════════════
// MIGRATION ENTITÉS → MARITIME (avec mapping visuel)
// ══════════════════════════════════════════════════════════════════════════
let currentMigType = 'vessel';
let detectedColumns = [];

// Champs cibles par type avec label et si obligatoire
const MIG_FIELDS = {
  vessel: [
    { key:'vessel_name',  label:'Nom du navire',      required:true  },
    { key:'imo_number',   label:'Numéro IMO',          required:false },
    { key:'mmsi',         label:'MMSI',                required:false },
    { key:'flag_code',    label:'Code pavillon',       required:false },
    { key:'vessel_type',  label:'Type de navire',      required:false },
    { key:'gross_tonnage',label:'Jauge brute (GT)',    required:false },
    { key:'owner_name',   label:'Armateur / Opérateur',required:false },
  ],
  port_call: [
    { key:'vessel_name',  label:'Nom du navire',       required:true  },
    { key:'imo_number',   label:'Numéro IMO',          required:false },
    { key:'port_name',    label:'Nom du port',         required:false },
    { key:'un_locode',    label:'Code LOCODE',         required:false },
    { key:'eta',          label:'ETA (arrivée)',        required:false },
    { key:'etd',          label:'ETD (départ)',         required:false },
    { key:'cargo_type',   label:'Type de cargaison',   required:false },
    { key:'cargo_qty',    label:'Quantité (tonnes)',    required:false },
    { key:'agent',        label:'Agent maritime',       required:false },
    { key:'call_status',  label:'Statut escale',        required:false },
  ],
  owner: [
    { key:'owner_name',   label:'Nom armateur',        required:true  },
    { key:'country',      label:'Pays',                required:false },
    { key:'contact',      label:'Contact',             required:false },
  ],
  port: [
    { key:'port_name',    label:'Nom du port',         required:true  },
    { key:'un_locode',    label:'Code LOCODE',         required:false },
    { key:'country_code', label:'Code pays',           required:false },
  ],
};

function selectMigType(type, el) {
  currentMigType = type;
  document.querySelectorAll('.mig-type-card').forEach(c => {
    c.style.border = '2px solid #e5e7eb';
    c.style.background = '#fff';
    c.querySelector('p:nth-child(2)').style.color = '#374151';
  });
  el.style.border = '2px solid #1B5EA6';
  el.style.background = '#e8f0fb';
  el.querySelector('p:nth-child(2)').style.color = '#1B5EA6';
}

async function openMigrationModal() {
  // Charger les sources disponibles
  document.getElementById('mig-step-1').style.display = 'flex';
  document.getElementById('mig-step-1').style.flexDirection = 'column';
  document.getElementById('mig-step-2').style.display = 'none';
  document.getElementById('mig-step-result').style.display = 'none';
  document.getElementById('mig-step-label').textContent = 'Étape 1 — Choisir la source';
  document.getElementById('migration-modal').style.display = 'flex';

  // Charger les sources
  const TOKEN = localStorage.getItem('mdm_token');
  document.getElementById('mig-step-1').style.display = 'flex';
  document.getElementById('mig-step-1').style.flexDirection = 'column';
  document.getElementById('mig-step-2').style.display = 'none';
  document.getElementById('mig-step-result').style.display = 'none';
  document.getElementById('mig-step-label').textContent = 'Étape 1 — Choisir la source';
  document.getElementById('migration-modal').style.display = 'flex';

  // Charger les sources via api() (gère token + port automatiquement)
  const srcSel = document.getElementById('mig-source-select');
  srcSel.innerHTML = '<option value="">⏳ Chargement…</option>';
  try {
    const sources = await api('/entities/sources');
    srcSel.innerHTML = '<option value="">— Toutes les sources —</option>';
    if (sources?.length) {
      sources.forEach(s => {
        const o = document.createElement('option');
        o.value = s; o.textContent = '📂 ' + s;
        srcSel.appendChild(o);
      });
    } else {
      srcSel.innerHTML = '<option value="">— Aucune source trouvée —</option>';
    }
  } catch(e) {
    srcSel.innerHTML = '<option value="">— Erreur chargement sources —</option>';
  }
}

function closeMigrationModal() {
  document.getElementById('migration-modal').style.display = 'none';
}

async function goMigStep2() {
  document.getElementById('mig-step-label').textContent = 'Étape 2 — Mapper les champs';
  
  // Récupérer un échantillon des entités pour détecter les colonnes
  const TOKEN = localStorage.getItem('mdm_token');
  const src = document.getElementById('mig-source-select').value;
  const params = new URLSearchParams({ page: 1, per_page: 50 });
  if (src) params.set('source', src);
  
  const _t2 = TOKEN || localStorage.getItem('mdm_token');
  const data = await api(`/entities?${params}`).catch(() => null);

  if (!data?.entities?.length) {
    alert('Aucune entité trouvée pour cette source'); return;
  }

  // Détecter toutes les colonnes disponibles
  detectedColumns = [...new Set(data.entities.flatMap(e => Object.keys(e.data || {})))].sort();

  // Essayer de pré-mapper automatiquement (matching approximatif)
  const autoMap = autoDetectMapping(detectedColumns, currentMigType);

  // Afficher le tableau de mapping
  const fields = MIG_FIELDS[currentMigType] || [];
  const tableEl = document.getElementById('mig-mapping-table');
  tableEl.innerHTML = fields.map(f => {
    const suggested = autoMap[f.key] || '';
    return `<div style="display:grid;grid-template-columns:1fr 20px 1fr;gap:8px;align-items:center;padding:8px 10px;background:#f9fafb;border-radius:8px;">
      <div>
        <p style="font-size:12px;font-weight:700;color:#374151;margin:0;">${f.label}${f.required?' <span style=color:#dc2626>*</span>':''}</p>
        <p style="font-size:10px;color:#9ca3af;margin:0;">Champ MDM Maritime</p>
      </div>
      <span style="color:#9ca3af;text-align:center;">→</span>
      <select class="select" id="map_${f.key}" style="font-size:12px;">
        <option value="">— Ignorer ce champ —</option>
        ${detectedColumns.map(c => `<option value="${c}" ${c===suggested?'selected':''}>${c}${suggested===c?' ✓':''}</option>`).join('')}
      </select>
    </div>`;
  }).join('');

  // Afficher un aperçu des valeurs pour la première entité
  const sample = data.entities[0]?.data || {};
  const preview = document.createElement('div');
  preview.style.cssText = 'background:#fffbeb;border-radius:8px;padding:10px 12px;border-left:3px solid #f59e0b;font-size:11px;color:#92400e;margin-top:8px;';
  preview.innerHTML = `<b>Aperçu des données (1ère ligne) :</b><br>${Object.entries(sample).slice(0,6).map(([k,v])=>`<b>${k}</b>: ${v}`).join(' &nbsp;|&nbsp; ')}`;
  tableEl.appendChild(preview);

  document.getElementById('mig-step-1').style.display = 'none';
  document.getElementById('mig-step-2').style.display = 'flex';
  document.getElementById('mig-step-2').style.flexDirection = 'column';
}

function autoDetectMapping(columns, type) {
  const map = {};
  const rules = {
    vessel_name:   ['nom_navire','vessel_name','name','nom','navire','ship_name','vessel'],
    imo_number:    ['imo','imo_number','numero_imo','imo_no','imonumber'],
    mmsi:          ['mmsi','mmsi_number'],
    flag_code:     ['pavillon','flag','flag_code','pays_pavillon','nationalite'],
    vessel_type:   ['type_navire','vessel_type','type','ship_type','type_ship'],
    gross_tonnage: ['gt','gross_tonnage','jauge','tonnage','jauge_brute'],
    owner_name:    ['armateur','owner','operateur','compagnie','owner_name','company'],
    port_name:     ['port','port_name','nom_port','port_escale','destination'],
    un_locode:     ['locode','un_locode','code_port','port_code'],
    eta:           ['eta','date_arrivee','arrivee','arrival','arrival_date'],
    etd:           ['etd','date_depart','depart','departure','departure_date'],
    cargo_type:    ['cargaison','cargo','cargo_type','type_cargaison','marchandise'],
    cargo_qty:     ['quantite','quantity','cargo_qty','tonnage_cargo','qt'],
    agent:         ['agent','shipping_agent','agent_maritime','consignataire'],
    call_status:   ['statut','status','call_status','etat'],
    country:       ['pays','country','nation','country_code'],
    contact:       ['contact','email','telephone','tel'],
    country_code:  ['pays','country','code_pays','country_code'],
  };

  const colsLower = columns.map(c => c.toLowerCase());
  for (const [field, aliases] of Object.entries(rules)) {
    for (const alias of aliases) {
      const idx = colsLower.indexOf(alias);
      if (idx !== -1) { map[field] = columns[idx]; break; }
      // Matching partiel
      const partial = colsLower.findIndex(c => c.includes(alias) || alias.includes(c));
      if (partial !== -1 && !map[field]) map[field] = columns[partial];
    }
  }
  return map;
}

function goMigStep1() {
  document.getElementById('mig-step-1').style.display = 'flex';
  document.getElementById('mig-step-1').style.flexDirection = 'column';
  document.getElementById('mig-step-2').style.display = 'none';
  document.getElementById('mig-step-label').textContent = 'Étape 1 — Choisir la source';
}

async function runMigrationWithMapping() {
  // Récupérer le mapping défini par l'utilisateur
  const fields = MIG_FIELDS[currentMigType] || [];
  const mapping = {};
  fields.forEach(f => {
    const val = document.getElementById(`map_${f.key}`)?.value;
    if (val) mapping[f.key] = val;
  });

  // Vérifier les champs obligatoires
  const missing = fields.filter(f => f.required && !mapping[f.key]);
  if (missing.length) {
    alert(`Champs obligatoires manquants :\n${missing.map(f=>f.label).join('\n')}`);
    return;
  }

  const btn = document.getElementById('mig-btn');
  btn.textContent = '⏳ Migration en cours…'; btn.disabled = true;

  const src = document.getElementById('mig-source-select').value;
  const TOKEN = localStorage.getItem('mdm_token');

  const res = await api('/maritime/migrate-from-entities', {
    method: 'POST',
    body: JSON.stringify({ entity_type: currentMigType, source_filter: src, mapping })
  });

  btn.textContent = '⬆️ Lancer la migration'; btn.disabled = false;

  if (!res?.success) { alert('Erreur lors de la migration'); return; }

  const d = res.details;
  const errDetails = d.errors?.length 
    ? `<br><details style="margin-top:6px;"><summary style="cursor:pointer;color:#dc2626;">⚠️ ${d.errors.length} erreur(s) — cliquer pour voir</summary><div style="font-size:10px;color:#7f1d1d;margin-top:4px;background:#fff;padding:6px;border-radius:4px;max-height:80px;overflow-y:auto;">${d.errors.map(e=>typeof e==='object'?e.error:e).join('<br>')}</div></details>` 
    : '';
  document.getElementById('mig-result-details').innerHTML = `
    🚢 Navires migrés : <b>${d.vessels || 0}</b><br>
    📍 Escales migrées : <b>${d.port_calls || 0}</b><br>
    🏢 Armateurs migrés : <b>${d.owners || 0}</b><br>
    ⚓ Ports migrés : <b>${d.ports || 0}</b><br>
    ⏭ Ignorés (doublons) : <b>${d.skipped || 0}</b>${errDetails}
  `;
  document.getElementById('mig-step-2').style.display = 'none';
  document.getElementById('mig-step-result').style.display = 'block';
  document.getElementById('mig-step-label').textContent = '✅ Migration terminée';
  setTimeout(() => { loadMaritimeDashboard(); loadVessels(); loadCalls(); }, 500);
}

// ══════════════════════════════════════════════════════════════════════════
// GOLDEN RECORD MARITIME
// ══════════════════════════════════════════════════════════════════════════
let selectedForGolden = [];

function toggleVesselSelect(vid, name) {
  const idx = selectedForGolden.indexOf(vid);
  if (idx > -1) {
    selectedForGolden.splice(idx, 1);
  } else {
    selectedForGolden.push(vid);
  }
  updateGoldenBar();
}

function updateGoldenBar() {
  let bar = document.getElementById('golden-maritime-bar');
  if (!bar) {
    bar = document.createElement('div');
    bar.id = 'golden-maritime-bar';
    bar.style.cssText = 'position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:#1f2937;color:#fff;padding:12px 20px;border-radius:12px;display:flex;align-items:center;gap:14px;z-index:990;box-shadow:0 4px 20px rgba(0,0,0,.3);font-size:13px;';
    document.body.appendChild(bar);
  }
  if (selectedForGolden.length < 2) {
    bar.style.display = 'none'; return;
  }
  bar.style.display = 'flex';
  bar.innerHTML = `<span>🔄 <b>${selectedForGolden.length}</b> navires sélectionnés</span>
    <button onclick="createMaritimeGoldenRecord()" style="background:#059669;border:none;color:#fff;padding:6px 14px;border-radius:8px;cursor:pointer;font-weight:600;font-size:12px;">✅ Créer Golden Record</button>
    <button onclick="clearGoldenSelection()" style="background:#374151;border:none;color:#fff;padding:6px 10px;border-radius:8px;cursor:pointer;font-size:12px;">✕</button>`;
}

function clearGoldenSelection() {
  selectedForGolden = [];
  document.querySelectorAll('.vessel-select-cb').forEach(cb => cb.checked = false);
  updateGoldenBar();
}

async function createMaritimeGoldenRecord() {
  if (selectedForGolden.length < 2) { alert('Sélectionnez au moins 2 navires'); return; }
  if (!confirm(`Fusionner ${selectedForGolden.length} navires en un Golden Record ?`)) return;
  const res = await fetch(`${MAPI}/golden-records/vessels`, {
    method: 'POST',
    headers: { 'Content-Type':'application/json', 'Authorization':`Bearer ${localStorage.getItem('mdm_token')}` },
    body: JSON.stringify({ vessel_ids: selectedForGolden })
  }).then(r => r.json()).catch(() => null);
  if (!res?.success) { alert('Erreur lors de la fusion'); return; }
  alert(`✅ Golden Record créé : ${res.golden_record_id}\n${res.merged_count} navires fusionnés`);
  clearGoldenSelection();
  loadVessels();
  loadMaritimeDashboard();
}

async function detectMaritimeDuplicates() {
  showSection('maritime-vessels');
  const res = await fetch(`${MAPI}/duplicates/detect`, {
    method: 'POST',
    headers: { 'Content-Type':'application/json', 'Authorization':`Bearer ${localStorage.getItem('mdm_token')}` },
    body: JSON.stringify({})
  }).then(r => r.json()).catch(() => null);
  if (!res) return;
  if (res.found === 0) { alert('Aucun doublon détecté ✅'); return; }
  alert(`🔄 ${res.found} doublon(s) détecté(s). Sélectionnez les navires à fusionner dans le tableau.`);
  // Highlighter les doublons dans le tableau
  res.duplicates?.forEach(d => {
    [d.vessel1_id, d.vessel2_id].forEach(id => {
      const row = document.querySelector(`tr[data-vessel-id="${id}"]`);
      if (row) row.style.background = '#fef9c3';
    });
  });
}

// ══════════════════════════════════════════════════════════════════════════
// §4.3 DASHBOARD QUALITÉ MDM
// ══════════════════════════════════════════════════════════════════════════
async function loadQualityDashboard() {
  const data = await api('/maritime/quality');
  const el = document.getElementById('mar-quality-panel');
  if (!data || !el) return;
  const comp = data.completeness || {};
  const alerts = data.alerts || {};
  const fieldLabels = { imo_number:'IMO', mmsi:'MMSI', vessel_type:'Type', flag_code:'Pavillon', gross_tonnage:'Tonnage', year_built:'Année', owner_id:'Armateur' };
  const barColor = pct => pct >= 80 ? '#059669' : pct >= 50 ? '#d97706' : '#dc2626';
  el.innerHTML = `<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
    <div>
      <p style="font-size:12px;font-weight:700;color:#374151;margin:0 0 10px;">📊 Complétude par champ</p>
      ${Object.entries(comp).map(([k, pct]) => `<div style="margin-bottom:8px;">
        <div style="display:flex;justify-content:space-between;margin-bottom:3px;">
          <span style="font-size:11px;color:#374151;font-weight:600;">${fieldLabels[k]||k}</span>
          <span style="font-size:11px;color:${barColor(pct)};font-weight:700;">${pct}%</span>
        </div>
        <div style="background:#f3f4f6;border-radius:99px;height:6px;">
          <div style="background:${barColor(pct)};border-radius:99px;height:6px;width:${pct}%;transition:width .5s;"></div>
        </div>
      </div>`).join('')}
      <div style="margin-top:10px;padding:8px 12px;background:#e8f0fb;border-radius:8px;">
        <span style="font-size:12px;font-weight:700;color:#1B5EA6;">Score global : ${data.overall_completeness}%</span>
        <span style="font-size:11px;color:#6b7280;margin-left:8px;">— Confiance moyenne : ${data.average_confidence}</span>
      </div>
    </div>
    <div>
      <p style="font-size:12px;font-weight:700;color:#374151;margin:0 0 10px;">⚠️ Alertes proactives</p>
      ${Object.entries(alerts).filter(([,v])=>v>0).map(([k,v])=>{
        const labels={vessels_no_imo:'🚢 Navires sans IMO',vessels_no_owner:'🏢 Navires sans armateur',ports_no_coordinates:'📍 Ports sans coordonnées',owners_no_fleet:'🏢 Armateurs sans flotte',orphan_port_calls:'📍 Escales sans navire lié'};
        return `<div style="display:flex;align-items:center;gap:8px;padding:8px 12px;background:#fef3c7;border-radius:8px;margin-bottom:6px;">
          <span style="font-size:12px;color:#92400e;font-weight:600;">${labels[k]||k}</span>
          <span style="font-size:14px;font-weight:800;color:#dc2626;margin-left:auto;">${v}</span>
        </div>`;
      }).join('')||'<p style="font-size:12px;color:#059669;">✅ Aucune alerte — données en bon état</p>'}
    </div>
  </div>`;
}

// ══════════════════════════════════════════════════════════════════════════
// §4.5 VUE 360° NAVIRE
// ══════════════════════════════════════════════════════════════════════════
async function openVessel360(vid) {
  document.getElementById('vessel-360-modal').style.display = 'flex';
  document.getElementById('v360-content').innerHTML = '<p style="color:#9ca3af;text-align:center;padding:40px;">⏳ Chargement…</p>';
  const v = await api(`/maritime/vessels/${vid}/360`);
  if (!v) { document.getElementById('v360-content').innerHTML = '<p style="color:#dc2626;">Erreur chargement</p>'; return; }
  document.getElementById('v360-title').textContent = `🚢 ${v.vessel_name} — Vue 360°`;
  const confDetail = v.confidence_detail || {};
  const confItems = [
    {key:'imo_valid',label:'IMO',pct:35},{key:'mmsi_valid',label:'MMSI',pct:15},
    {key:'name_present',label:'Nom',pct:15},{key:'type_present',label:'Type',pct:10},
    {key:'flag_present',label:'Pavillon',pct:10},{key:'tonnage_present',label:'Tonnage',pct:5},
    {key:'year_present',label:'Année',pct:5},{key:'no_errors',label:'Sans erreur',pct:5}
  ];
  const calls = v.port_calls || [];
  const dups = v.potential_duplicates || [];
  const comments = v.comments || [];
  const owner = v.owner;
  document.getElementById('v360-content').innerHTML = `
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px;">
      <div class="card" style="padding:14px;">
        <p style="font-size:11px;color:#9ca3af;font-weight:600;margin:0 0 8px;">📋 Identité</p>
        <table style="width:100%;font-size:12px;">
          <tr><td style="color:#9ca3af;padding:3px 0;">MDM ID</td><td style="font-family:monospace;color:#1B5EA6;font-weight:700;">${v.mdm_id||'—'}</td></tr>
          <tr><td style="color:#9ca3af;padding:3px 0;">IMO</td><td style="font-family:monospace;color:#059669;">${v.imo_number||'—'}</td></tr>
          <tr><td style="color:#9ca3af;padding:3px 0;">MMSI</td><td style="font-family:monospace;">${v.mmsi||'—'}</td></tr>
          <tr><td style="color:#9ca3af;padding:3px 0;">Type</td><td>${v.vessel_type||'—'}</td></tr>
          <tr><td style="color:#9ca3af;padding:3px 0;">Pavillon</td><td>${v.flag_code||''} ${v.flag_name||'—'}</td></tr>
          <tr><td style="color:#9ca3af;padding:3px 0;">GT</td><td>${v.gross_tonnage?Number(v.gross_tonnage).toLocaleString():'—'}</td></tr>
          <tr><td style="color:#9ca3af;padding:3px 0;">Armateur</td><td>${owner?owner.owner_name:'—'}</td></tr>
          <tr><td style="color:#9ca3af;padding:3px 0;">Source</td><td><span class="badge" style="background:#e8f0fb;color:#1B5EA6;">${v.source||'—'}</span></td></tr>
        </table>
      </div>
      <div class="card" style="padding:14px;">
        <p style="font-size:11px;color:#9ca3af;font-weight:600;margin:0 0 8px;">🎯 Score de confiance : ${(v.confidence_score*100).toFixed(0)}%</p>
        ${confItems.map(c=>`<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;">
          <span style="font-size:16px;">${confDetail[c.key]?'✅':'❌'}</span>
          <span style="font-size:11px;color:#374151;flex:1;">${c.label}</span>
          <span style="font-size:10px;color:#9ca3af;">${c.pct}%</span>
        </div>`).join('')}
        ${dups.length?`<div style="margin-top:10px;padding:8px;background:#fef3c7;border-radius:8px;">
          <span style="font-size:11px;color:#92400e;font-weight:600;">⚠️ ${dups.length} doublon(s) potentiel(s)</span>
        </div>`:'<div style="margin-top:10px;padding:8px;background:#d1fae5;border-radius:8px;font-size:11px;color:#065f46;">✅ Aucun doublon</div>'}
      </div>
    </div>
    <div class="card" style="padding:14px;margin-bottom:16px;">
      <p style="font-size:11px;color:#9ca3af;font-weight:600;margin:0 0 8px;">📍 Historique escales (${calls.length})</p>
      ${calls.length?`<div style="overflow-x:auto;max-height:200px;"><table style="width:100%;border-collapse:collapse;font-size:11px;">
        <thead><tr><th class="table-th">Port</th><th class="table-th">LOCODE</th><th class="table-th">ETA</th><th class="table-th">ETD</th><th class="table-th">Statut</th><th class="table-th">Cargaison</th></tr></thead>
        <tbody>${calls.map(c=>`<tr>
          <td class="table-td">${esc(c.port_name||'—')}</td>
          <td class="table-td" style="font-family:monospace;color:#059669;">${esc(c.un_locode||'—')}</td>
          <td class="table-td">${c.eta?c.eta.substring(0,16):'—'}</td>
          <td class="table-td">${c.etd?c.etd.substring(0,16):'—'}</td>
          <td class="table-td"><span class="badge">${c.call_status||'—'}</span></td>
          <td class="table-td">${esc(c.cargo_type||'—')}</td>
        </tr>`).join('')}</tbody></table></div>`:'<p style="font-size:12px;color:#9ca3af;">Aucune escale.</p>'}
    </div>
    <div class="card" style="padding:14px;">
      <p style="font-size:11px;color:#9ca3af;font-weight:600;margin:0 0 8px;">💬 Commentaires (${comments.length})</p>
      ${comments.map(c=>`<div style="background:#f9fafb;border-radius:8px;padding:8px 10px;margin-bottom:6px;">
        <span style="font-size:11px;font-weight:600;color:#1B5EA6;">${esc(c.user_name)}</span>
        <span style="font-size:10px;color:#9ca3af;margin-left:8px;">${fmtDate(c.created_at)}</span>
        <p style="font-size:12px;color:#374151;margin:4px 0 0;">${esc(c.comment)}</p>
      </div>`).join('')||'<p style="font-size:12px;color:#9ca3af;">Aucun commentaire.</p>'}
      <div style="display:flex;gap:8px;margin-top:8px;">
        <input class="input" id="v360-comment" placeholder="Ajouter un commentaire…" style="flex:1;font-size:12px;"/>
        <button class="btn btn-primary" style="font-size:11px;padding:6px 12px;" onclick="addVessel360Comment('${v.id}')">Envoyer</button>
      </div>
    </div>`;
}
async function addVessel360Comment(vid) {
  const comment = document.getElementById('v360-comment')?.value.trim();
  if (!comment) return;
  await api('/comments', {method:'POST', body:JSON.stringify({entity_type:'vessel',entity_id:vid,comment})});
  toast('Commentaire ajouté ✅');
  openVessel360(vid);
}

// ══════════════════════════════════════════════════════════════════════════
// §6.2 ASYNC IMPORT — PROGRESS BAR
// ══════════════════════════════════════════════════════════════════════════
async function uploadFileAsync() {
  if (!dragFile) { toast('Choisissez un fichier','error'); return; }
  const source = document.getElementById('import-source-label')?.value.trim() || dragFile.name;
  const fd = new FormData();
  fd.append('file', dragFile); fd.append('source_label', source);
  document.getElementById('async-import-modal').style.display = 'flex';
  document.getElementById('async-progress-bar').style.width = '0%';
  document.getElementById('async-progress-text').textContent = 'Démarrage…';
  try {
    const res = await fetch(`${API}/import/async`, {method:'POST',headers:{'Authorization':`Bearer ${TOKEN}`},body:fd});
    const data = await res.json();
    if (!data?.job_id) { toast('Erreur','error'); document.getElementById('async-import-modal').style.display='none'; return; }
    const poll = setInterval(async()=>{
      const st = await api(`/import/status/${data.job_id}`);
      if (!st) { clearInterval(poll); return; }
      document.getElementById('async-progress-bar').style.width = (st.progress||0)+'%';
      document.getElementById('async-progress-text').textContent = `${st.progress||0}% — ${st.imported||0} lignes`;
      if (st.status==='done'||st.status==='error') {
        clearInterval(poll);
        setTimeout(()=>{
          document.getElementById('async-import-modal').style.display='none';
          st.status==='done' ? (toast(`✅ ${st.imported} lignes importées`), clearFile(), loadImportLogs(), loadDashboard())
                             : toast('❌ '+( st.error||'Erreur'),'error');
        },500);
      }
    }, 800);
  } catch { document.getElementById('async-import-modal').style.display='none'; toast('Erreur réseau','error'); }
}
