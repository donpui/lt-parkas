/**
 * LT Transporto Priemonių Parko Analizė
 * Main application module
 */

import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

const PAGE_SIZE = 50;

const FILTERS = [
  { id: 'f-marke', col: 'AGR_MARKE' },
  { id: 'f-komercinis', col: 'KOMERCINIS_PAV' },
  { id: 'f-kategorija', col: 'KATEGORIJA_KLASE' },
  { id: 'f-metai', col: 'AGR_CAR_YEAR' },
];

const NUMERIC_FILTERS = [
  { id: 'f-turis', col: 'DARBINIS_TURIS' },
  { id: 'f-galia', col: 'GALIA' },
  { id: 'f-rida', col: 'RIDA' },
  { id: 'f-greitis', col: 'MAKS_GREITIS' },
];

const EXTRA_COMBO_FILTERS = [
  { id: 'f-apskritis', col: 'APSKRITIS' },
  { id: 'f-savivaldybe', col: 'SAVIVALDYBE' },
  { id: 'f-vald-tipas', col: 'VALD_TIPAS' },
  { id: 'f-kilmes-salis', col: 'KILMES_SALIS' },
  { id: 'f-spalva', col: 'SPALVA' },
];

const ALL_COMBO_FILTERS = [...FILTERS, ...EXTRA_COMBO_FILTERS];

const DISPLAY_COLS = [
  'AGR_MARKE', 'KOMERCINIS_PAV', 'VALD_TIPAS', 'KILMES_SALIS',
  'KATEGORIJA_KLASE', 'KEB_PAVADINIMAS', 'DEGALAI', 'GALIA',
  'DARBINIS_TURIS', 'AGR_CAR_YEAR', 'PIRM_REG_DATA', 'RIDA',
  'SPALVA', 'SAVIVALDYBE'
];

// Kategorijos pagal [Transporto priemonių kategorijos ir klasės](https://rekvizitai.vz.lt/istatymai/transporto-priemoniu-kategorijos-ir-klases/)
const KATEGORIJA_LABELS = {
  L: 'L — mopedai, motociklai',
  L1: 'L1 — mopedai iki 50cm³',
  L2: 'L2 — triračiai mopedai',
  L3: 'L3 — motociklai',
  L4: 'L4 — motociklai su šonine priekaba',
  L5: 'L5 — triračiai motociklai',
  L6: 'L6 — lengvieji keturračiai',
  L7: 'L7 — keturračiai',
  L1e: 'L1e — mopedas',
  L2e: 'L2e — mopedas su priekaba',
  L3e: 'L3e — motociklas',
  L4e: 'L4e — motociklas su priekaba',
  L5e: 'L5e — triratis',
  L6e: 'L6e — keturratis iki 350kg',
  L7e: 'L7e — keturratis iki 400kg',
  M: 'M — keleivinis transportas',
  M1: 'M1 — automobiliai iki 8 vietų',
  M2: 'M2 — keleivinis transportas iki 5t',
  M3: 'M3 — keleivinis transportas nuo 5t',
  N: 'N — krovininis transportas',
  N1: 'N1 — krovininis transportas iki 3.5t',
  N2: 'N2 — krovininis transportas 3.5–12t',
  N3: 'N3 — krovininis transportas nuo 12t',
  G: 'G — visureigiai',
  GK: 'GK — visureigiai',
  O: 'O — priekabos, puspriekabės',
  O1: 'O1 — priekabos iki 0.75t',
  O2: 'O2 — priekabos 0.75–3.5t',
  O3: 'O3 — priekabos 3.5–10t',
  O4: 'O4 — priekabos nuo 10t',
};

const APSKRITIS_LABELS = {
  ALY: 'Alytus',
  KAU: 'Kaunas',
  KLA: 'Klaipėda',
  MAR: 'Marijampolė',
  PAN: 'Panevėžys',
  ŠIA: 'Šiauliai',
  TAU: 'Tauragė',
  TEL: 'Telšiai',
  UTE: 'Utena',
  VIL: 'Vilnius',
};

const APSKRITIS_MAP = {
  ALY: 'LT-AL',
  KAU: 'LT-KU',
  KLA: 'LT-KL',
  MAR: 'LT-MR',
  PAN: 'LT-PN',
  ŠIA: 'LT-SA',
  TAU: 'LT-TA',
  TEL: 'LT-TE',
  UTE: 'LT-UT',
  VIL: 'LT-VL',
};

const MAP_LEVELS = 5;

const GRAPH_DIMENSIONS = {
  SPALVA: 'Spalva',
  AGR_CAR_YEAR: 'Metai',
  VALD_TIPAS: 'Valdymo tipas',
  KILMES_SALIS: 'Kilmės šalis',
  DEGALAI: 'Degalai',
  KATEGORIJA_KLASE: 'Kategorija',
  APSKRITIS: 'Apskritis',
  SAVIVALDYBE: 'Savivaldybė',
};

// Kilmės šalis: kodas → pilnas šalies pavadinimas (ISO 3166-1 alpha-2 / alpha-3)
const KILMES_SALIS_LABELS = {
  // Alpha-2
  LT: 'Lietuva', LV: 'Latvija', EE: 'Estija', PL: 'Lenkija', BY: 'Baltarusija', RU: 'Rusija',
  DE: 'Vokietija', FR: 'Prancūzija', IT: 'Italija', ES: 'Ispanija', GB: 'Jungtinė Karalystė', UK: 'Jungtinė Karalystė',
  JP: 'Japonija', CN: 'Kinija', KR: 'Pietų Korėja', US: 'JAV', CZ: 'Čekija', SK: 'Slovakija',
  HU: 'Vengrija', RO: 'Rumunija', BG: 'Bulgarija', NL: 'Nyderlandai', BE: 'Belgija', AT: 'Austrija',
  SE: 'Švedija', FI: 'Suomija', NO: 'Norvegija', DK: 'Danija', PT: 'Portugalija', GR: 'Graikija',
  TR: 'Turkija', UA: 'Ukraina', IN: 'Indija', BR: 'Brazilija', MX: 'Meksika', CA: 'Kanada', AU: 'Australija',
  AL: 'Albanija', AD: 'Andora', AM: 'Armėnija', AZ: 'Azerbaidžanas', BH: 'Bahreinas', BD: 'Bangladešas',
  BA: 'Bosnija ir Hercegovina', HR: 'Kroatija', CY: 'Kipras', GE: 'Gruzija', IL: 'Izraelis', JO: 'Jordanija',
  KZ: 'Kazachstanas', KW: 'Kuveitas', KG: 'Kirgizija', LB: 'Libanas', LY: 'Libija', MK: 'Šiaurės Makedonija',
  MT: 'Malta', MD: 'Moldavija', ME: 'Juodkalnija', MA: 'Marokas', NP: 'Nepalas', OM: 'Omanas',
  PK: 'Pakistanas', QA: 'Kataras', SA: 'Saudo Arabija', RS: 'Serbija', SI: 'Slovėnija', LK: 'Šri Lanka',
  SY: 'Sirija', TJ: 'Tadžikija', TH: 'Tailandas', TM: 'Turkmenistanas', AE: 'JAE', UZ: 'Uzbekistanas',
  VN: 'Vietnamas', YE: 'Jemenas', EG: 'Egiptas', ZA: 'Pietų Afrika', NG: 'Nigerija', KE: 'Kenija',
  AR: 'Argentina', CL: 'Čilė', CO: 'Kolumbija', PE: 'Peru', VE: 'Venesuela', ID: 'Indonezija',
  MY: 'Malaizija', PH: 'Filipinai', SG: 'Singapūras', TW: 'Taivanas', NZ: 'Naujoji Zelandija',
  IE: 'Airija', LU: 'Liuksemburgas', CH: 'Šveicarija', IS: 'Islandija', EC: 'Ekvadoras',
  // Alpha-3
  LTU: 'Lietuva', LVA: 'Latvija', EST: 'Estija', POL: 'Lenkija', BLR: 'Baltarusija', RUS: 'Rusija',
  DEU: 'Vokietija', FRA: 'Prancūzija', ITA: 'Italija', ESP: 'Ispanija', GBR: 'Jungtinė Karalystė',
  JPN: 'Japonija', CHN: 'Kinija', KOR: 'Pietų Korėja', USA: 'JAV', CZE: 'Čekija', SVK: 'Slovakija',
  HUN: 'Vengrija', ROU: 'Rumunija', BGR: 'Bulgarija', NLD: 'Nyderlandai', BEL: 'Belgija', AUT: 'Austrija',
  SWE: 'Švedija', FIN: 'Suomija', NOR: 'Norvegija', DNK: 'Danija', PRT: 'Portugalija', GRC: 'Graikija',
  TUR: 'Turkija', UKR: 'Ukraina', IND: 'Indija', BRA: 'Brazilija', MEX: 'Meksika', CAN: 'Kanada', AUS: 'Australija',
  ALB: 'Albanija', AND: 'Andora', ARM: 'Armėnija', AZE: 'Azerbaidžanas', BHR: 'Bahreinas', BGD: 'Bangladešas',
  BIH: 'Bosnija ir Hercegovina', HRV: 'Kroatija', CYP: 'Kipras', GEO: 'Gruzija', ISR: 'Izraelis', JOR: 'Jordanija',
  KAZ: 'Kazachstanas', KWT: 'Kuveitas', KGZ: 'Kirgizija', LBN: 'Libanas', LBY: 'Libija', MKD: 'Šiaurės Makedonija',
  MLT: 'Malta', MDA: 'Moldavija', MNE: 'Juodkalnija', MAR: 'Marokas', NPL: 'Nepalas', OMN: 'Omanas',
  PAK: 'Pakistanas', QAT: 'Kataras', SAU: 'Saudo Arabija', SRB: 'Serbija', SVN: 'Slovėnija', LKA: 'Šri Lanka',
  SYR: 'Sirija', TJK: 'Tadžikija', THA: 'Tailandas', TKM: 'Turkmenistanas', ARE: 'JAE', UZB: 'Uzbekistanas',
  VNM: 'Vietnamas', YEM: 'Jemenas', EGY: 'Egiptas', ZAF: 'Pietų Afrika', NGA: 'Nigerija', KEN: 'Kenija',
  ARG: 'Argentina', CHL: 'Čilė', COL: 'Kolumbija', PER: 'Peru', VEN: 'Venesuela', IDN: 'Indonezija',
  MYS: 'Malaizija', PHL: 'Filipinai', SGP: 'Singapūras', TWN: 'Taivanas', NZL: 'Naujoji Zelandija',
  IRL: 'Airija', LUX: 'Liuksemburgas', CHE: 'Šveicarija', ISL: 'Islandija', ECU: 'Ekvadoras',
  ANT: 'Nyderlandų Antilai', CUW: 'Kiurasao', SXM: 'Sint Martenas', AFG: 'Afganistanas', DZA: 'Alžyras',
  AGO: 'Angola', ATG: 'Antigva ir Barbuda', BHS: 'Bahamos', BLR: 'Baltarusija', BLZ: 'Belizas',
  BEN: 'Beninas', BTN: 'Butanas', BOL: 'Bolivija', BWA: 'Botsvana', BRN: 'Brunėjus', BFA: 'Burkina Fasas',
  BDI: 'Burundis', CPV: 'Žaliasis Kyšulys', CMR: 'Kamerūnas', CAF: 'Centrinės Afrikos Respublika',
  TCD: 'Čadas', COM: 'Komorai', COG: 'Kongas', COD: 'Kongo DR', CIV: 'Dramblio Kaulo Krantas',
  CUB: 'Kuba', DJI: 'Džibutis', DMA: 'Dominika', DOM: 'Dominikos Respublika', GNQ: 'Pusiaujo Gvinėja',
  ERI: 'Eritrėja', SWZ: 'Svazilendas', ETH: 'Etiopija', FJI: 'Fidžis', GAB: 'Gabonas', GMB: 'Gambija',
  GHA: 'Gana', GIN: 'Gvinėja', GNB: 'Bisau Gvinėja', GUY: 'Gajana', HTI: 'Haitis', HND: 'Hondūras',
  IRQ: 'Irakas', IRN: 'Iranas', JAM: 'Jamaika', KIR: 'Kiribatis', PRK: 'Šiaurės Korėja', LAO: 'Laosas',
  LSO: 'Lesotas', LBR: 'Liberija', LIE: 'Lichtenšteinas', MDG: 'Madagaskaras', MWI: 'Malavis',
  MDV: 'Maldyvai', MLI: 'Malis', MHL: 'Maršalo Salos', MRT: 'Mauritanija', MUS: 'Mauricijus',
  FSM: 'Mikronezija', MNG: 'Mongolija', MMR: 'Mianmaras', NAM: 'Namibija', NRU: 'Nauru', NIC: 'Nikaragva',
  NER: 'Nigeris', MNP: 'Marianos Šiaurinės Salos', PAN: 'Panama', PNG: 'Papua Naujoji Gvinėja',
  PRY: 'Paragvajus', RWA: 'Ruanda', KNA: 'Sent Kitsas ir Nevis', LCA: 'Sent Lusija', VCT: 'Sent Vinsentas ir Grenadinai',
  WSM: 'Samoa', SMR: 'San Marinas', STP: 'San Tomė ir Prinsipė', SEN: 'Senegalas', SLE: 'Siera Leonė',
  SOL: 'Saliamono Salos', SOM: 'Somalis', SSD: 'Pietų Sudanas', LKA: 'Šri Lanka', SDN: 'Sudanas',
  SUR: 'Surinamas', SYR: 'Sirija', TLS: 'Rytų Timoras', TGO: 'Togas', TON: 'Tonga', TTO: 'Trinidadas ir Tobagas',
  TUN: 'Tunisas', UGA: 'Uganda', URY: 'Urugvajus', VUT: 'Vanuatu', ZMB: 'Zambija', ZWE: 'Zimbabvė',
};

// ─────────────────────────────────────────────────────────────────────────────
// Application state
// ─────────────────────────────────────────────────────────────────────────────

let db;
let conn;
let currentPage = 0;
let totalRows = 0;
let allColumns = [];
let lastResultRows = [];
let sortCol = null;
let sortDir = 'ASC';
let refreshId = 0;
let mapSvg = null;
let lastApskritisCounts = null;
let lastGraphRows = null;

const filterState = {};
for (const f of ALL_COMBO_FILTERS) {
  filterState[f.id] = { allOptions: [], selectedValues: [] };
}

// ─────────────────────────────────────────────────────────────────────────────
// DOM helpers
// ─────────────────────────────────────────────────────────────────────────────

const $ = (selector) => document.querySelector(selector);

function getLabel(filterId, val) {
  if (filterId === 'f-kategorija' && KATEGORIJA_LABELS[val]) return KATEGORIJA_LABELS[val];
  if (filterId === 'f-apskritis' && APSKRITIS_LABELS[val]) return APSKRITIS_LABELS[val];
  if (filterId === 'f-kilmes-salis' && val != null) {
    const name = KILMES_SALIS_LABELS[String(val).toUpperCase()];
    return name ? `${val} — ${name}` : val;
  }
  return val;
}

function formatSelection(filterId, values) {
  if (!values || values.length === 0) return '';
  const label = getLabel(filterId, values[0]) || values[0];
  return values.length === 1 ? label : `${label} +${values.length - 1}`;
}

function setComboSelection(filterId, values) {
  filterState[filterId].selectedValues = [...values];
  const comboEl = $('#' + filterId);
  if (!comboEl) return;
  const input = comboEl.querySelector('input');
  input.value = formatSelection(filterId, values);
  comboEl.classList.toggle('has-value', values.length > 0);
}

// ─────────────────────────────────────────────────────────────────────────────
// Query building
// ─────────────────────────────────────────────────────────────────────────────

function parseNumericCondition(col, raw) {
  const s = raw.trim();
  if (!s) return null;
  const m = s.match(/^(>=|<=|>|<|=)?\s*(\d+)$/);
  if (!m) return null;
  const op = m[1] || '=';
  const num = m[2];
  return `CAST("${col}" AS INTEGER) ${op} ${num}`;
}

function buildWhereForFilter(f) {
  const val = filterState[f.id].selectedValues;
  if (val.length === 0) return null;
  if (val.length === 1) return `"${f.col}" = '${val[0].replace(/'/g, "''")}'`;
  const list = val.map(v => `'${v.replace(/'/g, "''")}'`).join(', ');
  return `"${f.col}" IN (${list})`;
}

function buildWhere() {
  const conditions = [];
  for (const f of ALL_COMBO_FILTERS) {
    const cond = buildWhereForFilter(f);
    if (cond) conditions.push(cond);
  }
  for (const f of NUMERIC_FILTERS) {
    const cond = parseNumericCondition(f.col, $('#' + f.id).value);
    if (cond) conditions.push(cond);
  }
  return conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';
}

function appendWhere(baseWhere, extraCondition) {
  if (!extraCondition) return baseWhere;
  if (baseWhere) return `${baseWhere} AND ${extraCondition}`;
  return `WHERE ${extraCondition}`;
}

// ─────────────────────────────────────────────────────────────────────────────
// Combo box (click = single select, Shift+click = add to selection)
// ─────────────────────────────────────────────────────────────────────────────

function setupCombo(comboEl, filterId) {
  const input = comboEl.querySelector('input');
  const dropdown = comboEl.querySelector('.dropdown');
  const clearBtn = comboEl.querySelector('.clear-btn');
  let activeIdx = -1;
  let preventBlur = false;
  let shiftTimer = null;

  function getSelected() { return filterState[filterId].selectedValues; }
  function setSelected(arr) { filterState[filterId].selectedValues = [...arr]; }

  function displayValue() {
    return formatSelection(filterId, getSelected());
  }

  function renderDropdown() {
    const search = input.value.toLowerCase();
    const opts = filterState[filterId].allOptions;
    const sel = getSelected();
    const filtered = opts.filter((o) => {
      const raw = (o || '').toString().toLowerCase();
      const label = (getLabel(filterId, o) || '').toString().toLowerCase();
      return raw.includes(search) || label.includes(search);
    });

    dropdown.innerHTML = '';
    activeIdx = -1;

    if (filtered.length === 0) {
      dropdown.innerHTML = '<div class="no-match">Nieko nerasta</div>';
      return;
    }

    for (const val of filtered) {
      const div = document.createElement('div');
      div.className = 'opt' + (sel.includes(val) ? ' selected' : '');
      div.dataset.value = val;
      div.textContent = getLabel(filterId, val);
      div.addEventListener('mousedown', (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.shiftKey) preventBlur = true;
        selectOption(val, e.shiftKey);
        if (e.shiftKey) {
          requestAnimationFrame(() => { input.focus(); preventBlur = false; });
        }
      });
      dropdown.appendChild(div);
    }
  }

  function selectOption(val, addToSelection) {
    const sel = getSelected();
    if (addToSelection) {
      if (sel.includes(val)) {
        setSelected(sel.filter(v => v !== val));
      } else {
        setSelected([...sel, val]);
      }
      comboEl.classList.toggle('has-value', getSelected().length > 0);
      renderDropdown();
      clearTimeout(shiftTimer);
      shiftTimer = setTimeout(async () => {
        currentPage = 0;
        await queryCount();
        await queryResults();
      }, 400);
    } else {
      setSelected([val]);
      input.value = displayValue();
      comboEl.classList.toggle('has-value', true);
      closeDropdown();
      currentPage = 0;
      refresh();
    }
  }

  function clearSelection() {
    setSelected([]);
    input.value = '';
    comboEl.classList.remove('has-value');
    closeDropdown();
    currentPage = 0;
    refresh();
  }

  function openDropdown() { renderDropdown(); comboEl.classList.add('open'); }
  function closeDropdown() { comboEl.classList.remove('open'); activeIdx = -1; }

  function updateActive(items) {
    items.forEach((el, i) => el.classList.toggle('active', i === activeIdx));
    if (items[activeIdx]) items[activeIdx].scrollIntoView({ block: 'nearest' });
  }

  input.addEventListener('focus', () => { if (!preventBlur && getSelected().length) input.value = ''; openDropdown(); });
  input.addEventListener('blur', () => {
    if (preventBlur) return;
    input.value = displayValue();
    comboEl.classList.toggle('has-value', getSelected().length > 0);
    closeDropdown();
    clearTimeout(shiftTimer);
    currentPage = 0;
    refresh();
  });
  input.addEventListener('input', () => { activeIdx = -1; renderDropdown(); });
  input.addEventListener('keydown', (e) => {
    const items = dropdown.querySelectorAll('.opt');
    if (e.key === 'ArrowDown') { e.preventDefault(); activeIdx = Math.min(activeIdx + 1, items.length - 1); updateActive(items); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); activeIdx = Math.max(activeIdx - 1, 0); updateActive(items); }
    else if (e.key === 'Enter') {
      e.preventDefault();
      const search = input.value.trim().toLowerCase();
      if (search && items.length > 0) {
        // Select all matching options (LIKE %search%)
        const allMatching = [...items].map(el => el.dataset.value || el.textContent);
        setSelected(allMatching);
        input.value = displayValue();
        comboEl.classList.toggle('has-value', true);
        closeDropdown();
        currentPage = 0;
        refresh();
      } else if (activeIdx >= 0 && items[activeIdx]) {
        const val = items[activeIdx].dataset.value || items[activeIdx].textContent;
        selectOption(val, e.shiftKey);
      }
    }
    else if (e.key === 'Escape') input.blur();
  });
  clearBtn.addEventListener('mousedown', (e) => { e.preventDefault(); clearSelection(); });
}

function setupCloseDropdownsOnOutsideClick() {
  document.addEventListener('mousedown', (e) => {
    if (!e.target.closest('.combo')) {
      document.querySelectorAll('.combo.open').forEach(c => c.classList.remove('open'));
    }
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Data queries
// ─────────────────────────────────────────────────────────────────────────────

async function populateFilterOptions() {
  for (let i = 0; i < FILTERS.length; i++) {
    const f = FILTERS[i];
    const state = filterState[f.id];

    const conditions = [];
    for (let j = 0; j < i; j++) {
      const above = FILTERS[j];
      const cond = buildWhereForFilter(above);
      if (cond) conditions.push(cond);
    }
    const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';

    const result = await conn.query(
      `SELECT DISTINCT "${f.col}" AS v FROM vehicles ${where} ORDER BY v`
    );
    const rows = result.toArray();

    const options = rows.filter(r => r.v != null && r.v !== '').map(r => r.v);
    state.allOptions = options;

    const valid = state.selectedValues.filter(v => options.includes(v));
    if (valid.length !== state.selectedValues.length) {
      setComboSelection(f.id, valid);
    }
  }

  for (const f of EXTRA_COMBO_FILTERS) {
    const state = filterState[f.id];
    const where = buildWhere();

    const result = await conn.query(
      `SELECT DISTINCT "${f.col}" AS v FROM vehicles ${where} ORDER BY v`
    );
    const rows = result.toArray();

    const options = rows.filter(r => r.v != null && r.v !== '').map(r => r.v);
    state.allOptions = options;

    const valid = state.selectedValues.filter(v => options.includes(v));
    if (valid.length !== state.selectedValues.length) {
      setComboSelection(f.id, valid);
    }
  }
}

async function queryCount() {
  const where = buildWhere();
  const result = await conn.query(`SELECT COUNT(*) AS c FROM vehicles ${where}`);
  totalRows = Number(result.toArray()[0].c);
  const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
  if (currentPage >= totalPages) currentPage = totalPages - 1;
  $('#count').textContent = `Rasta: ${totalRows.toLocaleString('lt-LT')} įrašų`;
}

async function queryResults() {
  const where = buildWhere();
  const orderBy = sortCol ? `ORDER BY "${sortCol}" ${sortDir}` : '';
  const offset = currentPage * PAGE_SIZE;
  const result = await conn.query(
    `SELECT * FROM vehicles ${where} ${orderBy} LIMIT ${PAGE_SIZE} OFFSET ${offset}`
  );
  const rows = result.toArray();
  lastResultRows = rows;

  const tbody = $('#tbody');
  tbody.innerHTML = '';
  rows.forEach((row, idx) => {
    const tr = document.createElement('tr');
    for (const col of DISPLAY_COLS) {
      const td = document.createElement('td');
      const v = row[col];
      let cellText = v != null ? v : '';
      if (col === 'KATEGORIJA_KLASE') cellText = KATEGORIJA_LABELS[v] || v || '';
      if (col === 'KILMES_SALIS' && v) {
        const name = KILMES_SALIS_LABELS[String(v).toUpperCase()];
        cellText = name ? `${v} — ${name}` : v;
      }
      td.textContent = cellText;
      tr.appendChild(td);
    }
    tr.addEventListener('click', () => showDetail(idx));
    tbody.appendChild(tr);
  });

  const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
  $('#page-info').textContent = `${currentPage + 1} / ${totalPages}`;
  $('#btn-prev').disabled = currentPage <= 0;
  $('#btn-next').disabled = currentPage >= totalPages - 1;
}

// ─────────────────────────────────────────────────────────────────────────────
// UI updates
// ─────────────────────────────────────────────────────────────────────────────

function updateSortArrows() {
  for (const th of $('#thead-row').children) {
    const existing = th.querySelector('.sort-arrow');
    if (existing) existing.remove();
    if (th.dataset.col === sortCol) {
      const arrow = document.createElement('span');
      arrow.className = 'sort-arrow';
      arrow.textContent = sortDir === 'ASC' ? '\u25B2' : '\u25BC';
      th.appendChild(arrow);
    }
  }
}

function showDetail(idx) {
  const row = lastResultRows[idx];
  if (!row) return;

  const marke = row.AGR_MARKE || '';
  const model = row.KOMERCINIS_PAV || '';
  $('#modal-title').textContent = [marke, model].filter(Boolean).join(' ') || 'Informacija';

  $('#modal-info').hidden = true;
  $('#modal-table').hidden = false;

  const tbody = $('#modal-body');
  tbody.innerHTML = '';
  for (const col of allColumns) {
    const tr = document.createElement('tr');
    const th = document.createElement('th');
    th.textContent = col;
    const td = document.createElement('td');
    const v = row[col];
    let cellText = v != null ? v : '';
    if (col === 'KATEGORIJA_KLASE') cellText = KATEGORIJA_LABELS[v] || v || '';
    if (col === 'KILMES_SALIS' && v) {
      const name = KILMES_SALIS_LABELS[String(v).toUpperCase()];
      cellText = name ? `${v} — ${name}` : v;
    }
    td.textContent = cellText;
    tr.appendChild(th);
    tr.appendChild(td);
    tbody.appendChild(tr);
  }

  $('#modal').classList.add('open');
}

function closeModal() {
  $('#modal').classList.remove('open');
}

function openInfoModal(title, description) {
  $('#modal-title').textContent = title;
  $('#modal-info-text').textContent = description;
  $('#modal-info').hidden = false;
  $('#modal-table').hidden = true;
  $('#modal').classList.add('open');
}

// ─────────────────────────────────────────────────────────────────────────────
// Map (Apskritys)
// ─────────────────────────────────────────────────────────────────────────────

function ensureMapCountLabels() {
  if (!mapSvg) return;
  if (mapSvg.getClientRects().length === 0) return;
  let group = mapSvg.querySelector('#apskritis-counts');
  if (!group) {
    group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    group.setAttribute('id', 'apskritis-counts');
    mapSvg.appendChild(group);
  }

  for (const [code, id] of Object.entries(APSKRITIS_MAP)) {
    const path = mapSvg.querySelector(`#${id}`);
    if (!path) continue;
    let label = group.querySelector(`text[data-aps="${code}"]`);
    if (!label) {
      label = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      label.setAttribute('data-aps', code);
      label.setAttribute('class', 'map-count');
      label.setAttribute('text-anchor', 'middle');
      label.setAttribute('dominant-baseline', 'central');
      group.appendChild(label);
    }
    const box = path.getBBox();
    label.setAttribute('x', (box.x + box.width / 2).toFixed(2));
    label.setAttribute('y', (box.y + box.height / 2).toFixed(2));
  }
}

function updateMapSelection() {
  if (!mapSvg) return;
  const selected = new Set(filterState['f-apskritis']?.selectedValues || []);
  for (const [code, id] of Object.entries(APSKRITIS_MAP)) {
    const path = mapSvg.querySelector(`#${id}`);
    if (!path) continue;
    path.classList.toggle('selected', selected.has(code));
  }
}

function updateMapCounts(counts) {
  lastApskritisCounts = counts;
  if (!mapSvg) return;

  const knownCounts = Object.keys(APSKRITIS_MAP).map(code => Number(counts[code] || 0));
  const maxCount = Math.max(0, ...knownCounts);

  let unknown = 0;
  for (const [code, id] of Object.entries(APSKRITIS_MAP)) {
    const count = Number(counts[code] || 0);
    const label = mapSvg.querySelector(`text.map-count[data-aps="${code}"]`);
    if (label) label.textContent = count.toLocaleString('lt-LT');
    const path = mapSvg.querySelector(`#${id}`);
    if (path) {
      for (let i = 0; i <= MAP_LEVELS; i++) {
        path.classList.remove(`level-${i}`);
      }
      let level = 0;
      if (count > 0 && maxCount > 0) {
        const intensity = Math.log10(count + 1) / Math.log10(maxCount + 1);
        level = Math.min(MAP_LEVELS, Math.max(1, Math.ceil(intensity * MAP_LEVELS)));
      }
      path.classList.add(`level-${level}`);
      const name = APSKRITIS_LABELS[code] || code;
      path.setAttribute('data-count', String(count));
      path.setAttribute('title', `${name}: ${count.toLocaleString('lt-LT')}`);
    }
  }

  for (const [code, count] of Object.entries(counts)) {
    if (!APSKRITIS_MAP[code]) unknown += Number(count || 0);
  }

  const unknownEl = $('#map-unknown');
  if (unknownEl) {
    unknownEl.textContent = unknown > 0 ? `Nepriskirta apskritis: ${unknown.toLocaleString('lt-LT')}` : '';
  }
}

function applyMapSelection(code, addToSelection) {
  const state = filterState['f-apskritis'];
  if (!state) return;
  const selected = new Set(state.selectedValues);
  if (addToSelection) {
    if (selected.has(code)) selected.delete(code);
    else selected.add(code);
  } else {
    selected.clear();
    selected.add(code);
  }
  setComboSelection('f-apskritis', [...selected]);
  currentPage = 0;
  refresh();
}

async function initMap() {
  const container = $('#lt-map');
  if (!container) return;
  try {
    const resp = await fetch('images/lithuania.svg');
    const svgText = await resp.text();
    container.innerHTML = svgText;
    mapSvg = container.querySelector('svg');
    if (!mapSvg) return;

    const width = parseFloat(mapSvg.getAttribute('width'));
    const height = parseFloat(mapSvg.getAttribute('height'));
    if (!mapSvg.getAttribute('viewBox') && width && height) {
      mapSvg.setAttribute('viewBox', `0 0 ${width} ${height}`);
    }
    mapSvg.removeAttribute('width');
    mapSvg.removeAttribute('height');

    for (const [code, id] of Object.entries(APSKRITIS_MAP)) {
      const path = mapSvg.querySelector(`#${id}`);
      if (!path) continue;
      path.classList.add('apskritis-path');
      path.setAttribute('data-aps', code);
      path.setAttribute('tabindex', '0');
      path.setAttribute('role', 'button');
      path.addEventListener('click', (e) => applyMapSelection(code, e.shiftKey));
      path.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          applyMapSelection(code, e.shiftKey);
        }
      });
    }

    ensureMapCountLabels();
    if (lastApskritisCounts) updateMapCounts(lastApskritisCounts);
    updateMapSelection();
  } catch (err) {
    container.textContent = 'Nepavyko įkelti žemėlapio.';
  }
}

async function queryApskritisCounts() {
  const where = buildWhere();
  const result = await conn.query(
    `SELECT APSKRITIS AS a, COUNT(*) AS c FROM vehicles ${where} GROUP BY APSKRITIS`
  );
  const rows = result.toArray();
  const counts = {};
  for (const row of rows) {
    const code = row.a ?? '';
    counts[code] = Number(row.c);
  }
  updateMapCounts(counts);
  updateMapSelection();
}

// ─────────────────────────────────────────────────────────────────────────────
// Graph
// ─────────────────────────────────────────────────────────────────────────────

function getGraphLabel(col, val) {
  if (val == null) return 'Nežinoma';
  const text = String(val);
  if (col === 'KATEGORIJA_KLASE') return KATEGORIJA_LABELS[text] || text;
  if (col === 'APSKRITIS') return APSKRITIS_LABELS[text] || text;
  return text;
}

function getGraphConfigFromUI() {
  const dimensionEl = $('#graph-dimension');
  if (!dimensionEl) return null;
  const dimension = dimensionEl.value;
  const sort = $('#graph-sort')?.value || 'count';
  const includeUnknown = $('#graph-include-unknown')?.checked ?? true;
  const rawLimit = ($('#graph-limit')?.value || '').trim();
  const limitNum = rawLimit ? Number(rawLimit) : 0;
  const limit = Number.isFinite(limitNum) && limitNum > 0 ? Math.floor(limitNum) : 0;
  const safeDimension = GRAPH_DIMENSIONS[dimension] ? dimension : 'SPALVA';
  return { dimension: safeDimension, sort, includeUnknown, limit };
}

function renderGraph(rows, config) {
  lastGraphRows = rows;
  const titleEl = $('#graph-title');
  const subtitleEl = $('#graph-subtitle');
  const barsEl = $('#graph-bars');
  if (!barsEl) return;

  const dimLabel = GRAPH_DIMENSIONS[config.dimension] || config.dimension;
  if (titleEl) titleEl.textContent = `Pagal: ${dimLabel}`;

  const sortLabel = config.sort === 'label' ? 'pagal pavadinimą' : 'pagal skaičių';
  const limitLabel = config.limit > 0 ? `, rodomos ${rows.length} iš ${config.limit}` : '';
  if (subtitleEl) {
    subtitleEl.textContent = `Rikiavimas ${sortLabel}${limitLabel}. Iš viso: ${totalRows.toLocaleString('lt-LT')} įrašų.`;
  }

  if (!rows.length) {
    barsEl.innerHTML = '<div class="graph-empty">Nėra duomenų pasirinktiems filtrams.</div>';
    return;
  }

  const max = Math.max(...rows.map(r => r.count), 1);
  barsEl.innerHTML = '';
  for (const row of rows) {
    const label = getGraphLabel(config.dimension, row.key);
    const pct = totalRows > 0 ? Math.round((row.count / totalRows) * 1000) / 10 : 0;

    const rowEl = document.createElement('div');
    rowEl.className = 'graph-row';

    const labelEl = document.createElement('div');
    labelEl.className = 'graph-label';
    labelEl.title = label;
    labelEl.textContent = label;

    const trackEl = document.createElement('div');
    trackEl.className = 'graph-track';

    const fillEl = document.createElement('div');
    fillEl.className = 'graph-fill';
    fillEl.style.width = `${(row.count / max) * 100}%`;
    trackEl.appendChild(fillEl);

    const valueEl = document.createElement('div');
    valueEl.className = 'graph-value';
    valueEl.textContent = `${row.count.toLocaleString('lt-LT')} (${pct}%)`;

    rowEl.appendChild(labelEl);
    rowEl.appendChild(trackEl);
    rowEl.appendChild(valueEl);
    barsEl.appendChild(rowEl);
  }
}

async function queryGraph() {
  const config = getGraphConfigFromUI();
  if (!config) return;
  const col = config.dimension;
  const colExpr = `CAST("${col}" AS VARCHAR)`;
  const keyExpr = config.includeUnknown
    ? `COALESCE(NULLIF(TRIM(${colExpr}), ''), 'Nežinoma')`
    : `NULLIF(TRIM(${colExpr}), '')`;

  const extraCondition = config.includeUnknown ? null : `${keyExpr} IS NOT NULL`;
  const where = appendWhere(buildWhere(), extraCondition);
  const orderBy = config.sort === 'label'
    ? 'ORDER BY TRY_CAST(k AS INTEGER) NULLS LAST, k ASC'
    : 'ORDER BY c DESC, k ASC';
  const limitSql = config.limit > 0 ? `LIMIT ${config.limit}` : '';

  const result = await conn.query(
    `SELECT ${keyExpr} AS k, COUNT(*) AS c FROM vehicles ${where} GROUP BY k ${orderBy} ${limitSql}`
  );
  const rows = result.toArray().map(r => ({ key: r.k, count: Number(r.c) }));
  renderGraph(rows, config);
}

async function refresh() {
  const id = ++refreshId;
  $('#app').classList.add('querying');
  await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
  if (id !== refreshId) return;

  const t0 = performance.now();

  await populateFilterOptions();
  if (id !== refreshId) return;
  updateMapSelection();
  await queryCount();
  if (id !== refreshId) return;
  await queryApskritisCounts();
  if (id !== refreshId) return;
  await queryGraph();
  if (id !== refreshId) return;
  await queryResults();

  const elapsed = ((performance.now() - t0) / 1000).toFixed(2);
  $('#query-time').textContent = `${elapsed}s`;
  $('#app').classList.remove('querying');
}

// ─────────────────────────────────────────────────────────────────────────────
// Event handlers setup
// ─────────────────────────────────────────────────────────────────────────────

function setupEventHandlers() {
  for (const f of NUMERIC_FILTERS) {
    let timer;
    $('#' + f.id).addEventListener('input', () => {
      clearTimeout(timer);
      timer = setTimeout(() => { currentPage = 0; refresh(); }, 400);
    });
  }

  $('#clear-filters').addEventListener('click', () => {
    for (const f of ALL_COMBO_FILTERS) {
      setComboSelection(f.id, []);
    }
    for (const f of NUMERIC_FILTERS) {
      $('#' + f.id).value = '';
    }
    currentPage = 0;
    refresh();
  });

  $('#more-toggle').addEventListener('click', () => {
    const extra = $('#extra-filters');
    const open = extra.classList.toggle('open');
    $('#more-toggle').innerHTML = open ? 'Mažiau filtrų &#9652;' : 'Daugiau filtrų &#9662;';
  });

  $('#btn-prev').addEventListener('click', () => { currentPage--; queryResults(); });
  $('#btn-next').addEventListener('click', () => { currentPage++; queryResults(); });

  for (const th of $('#thead-row').children) {
    th.addEventListener('click', () => {
      const col = th.dataset.col;
      if (sortCol === col) {
        sortDir = sortDir === 'ASC' ? 'DESC' : 'ASC';
      } else {
        sortCol = col;
        sortDir = 'ASC';
      }
      updateSortArrows();
      currentPage = 0;
      queryResults();
    });
  }

  $('#modal-close').addEventListener('click', closeModal);
  $('#modal').addEventListener('click', (e) => { if (e.target === $('#modal')) closeModal(); });
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeModal(); });

  const metaiHint = document.getElementById('metai-hint');
  if (metaiHint) {
    metaiHint.addEventListener('click', (e) => {
      e.preventDefault();
      const desc = metaiHint.getAttribute('data-description');
      if (desc) openInfoModal('Metai', desc);
    });
  }

  const tabs = document.querySelectorAll('.view-tab');
  if (tabs.length) {
    tabs.forEach((btn) => {
      btn.addEventListener('click', () => {
        const view = btn.dataset.view;
        if (!view) return;
        $('#app').setAttribute('data-view', view);
        tabs.forEach(tab => tab.classList.toggle('active', tab === btn));
        if (view === 'map') {
          requestAnimationFrame(() => {
            ensureMapCountLabels();
            if (lastApskritisCounts) updateMapCounts(lastApskritisCounts);
            updateMapSelection();
          });
        }
        if (view === 'graph') {
          queryGraph();
        }
      });
    });
  }

  const graphDimension = $('#graph-dimension');
  const graphSort = $('#graph-sort');
  const graphLimit = $('#graph-limit');
  const graphIncludeUnknown = $('#graph-include-unknown');

  if (graphDimension) {
    graphDimension.addEventListener('change', () => queryGraph());
  }
  if (graphSort) {
    graphSort.addEventListener('change', () => queryGraph());
  }
  if (graphIncludeUnknown) {
    graphIncludeUnknown.addEventListener('change', () => queryGraph());
  }
  if (graphLimit) {
    let limitTimer;
    graphLimit.addEventListener('input', () => {
      clearTimeout(limitTimer);
      limitTimer = setTimeout(() => queryGraph(), 300);
    });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// DuckDB initialization
// ─────────────────────────────────────────────────────────────────────────────

async function initDuckDB() {
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  );
  const worker = new Worker(worker_url);
  const logger = new duckdb.VoidLogger();
  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(worker_url);

  return db;
}

async function loadParquetFile() {
  $('#loading-text').textContent = 'Kraunamas Parquet failas...';
  $('#progress-wrap').style.display = 'block';

  const resp = await fetch('vehicles.parquet');
  const contentLength = Number(resp.headers.get('Content-Length') || 0);
  const reader = resp.body.getReader();
  const chunks = [];
  let received = 0;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
    received += value.length;
    if (contentLength) {
      const pct = Math.round((received / contentLength) * 100);
      $('#progress-bar').style.width = pct + '%';
      $('#progress-text').textContent = `${(received / 1048576).toFixed(1)} / ${(contentLength / 1048576).toFixed(1)} MB (${pct}%)`;
    } else {
      $('#progress-text').textContent = `${(received / 1048576).toFixed(1)} MB`;
    }
  }

  const buf = new Uint8Array(received);
  let pos = 0;
  for (const chunk of chunks) {
    buf.set(chunk, pos);
    pos += chunk.length;
  }
  await db.registerFileBuffer('vehicles.parquet', buf);

  $('#progress-wrap').style.display = 'none';
  $('#progress-text').textContent = '';
}

// ─────────────────────────────────────────────────────────────────────────────
// Theme toggle
// ─────────────────────────────────────────────────────────────────────────────

function setupThemeToggle() {
  const btn = $('#theme-toggle');
  if (!btn) return;
  btn.addEventListener('click', () => {
    const html = document.documentElement;
    const current = html.getAttribute('data-theme') || 'light';
    const next = current === 'light' ? 'dark' : 'light';
    html.setAttribute('data-theme', next);
    localStorage.setItem('theme', next);
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Main entry point
// ─────────────────────────────────────────────────────────────────────────────

async function init() {
  try {
    $('#loading-text').textContent = 'Inicializuojama DuckDB...';

    await initDuckDB();
    await loadParquetFile();

    conn = await db.connect();
    await conn.query(`CREATE VIEW vehicles AS SELECT * FROM 'vehicles.parquet'`);

    const colResult = await conn.query(
      `SELECT column_name FROM information_schema.columns WHERE table_name = 'vehicles' ORDER BY ordinal_position`
    );
    allColumns = colResult.toArray().map(r => r.column_name);

    $('#loading').style.display = 'none';
    $('#app').style.display = 'block';

    for (const f of ALL_COMBO_FILTERS) {
      setupCombo($('#' + f.id), f.id);
    }
    setupCloseDropdownsOnOutsideClick();
    setupEventHandlers();
    initMap();

    await refresh();
  } catch (err) {
    $('#loading-text').textContent = 'Klaida: ' + err.message;
  }
}

setupThemeToggle();
init();
