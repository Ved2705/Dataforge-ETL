/* ════════════════════════════════════════════
   DataForge ETL — Frontend
════════════════════════════════════════════ */

let currentFile = null;
let lastRunData = null;
let currentRunId = null;
let chartInstances = [];
let authMode = 'login';

const PIPELINE_STEPS = [
  { type: 'snake_case_columns', label: 'Snake Case Cols', tool: 'pandas', icon: '🔤', cat: 'string', catLabel: 'String' },
  { type: 'drop_duplicates', label: 'Drop Duplicates', tool: 'pandas', icon: '🧹', cat: 'cleaning', catLabel: 'Cleaning' },
  { type: 'drop_missing', label: 'Drop High-Missing', tool: 'pandas', icon: '🗑', cat: 'cleaning', catLabel: 'Cleaning' },
  { type: 'fill_missing', label: 'Fill Missing', tool: 'pandas', icon: '🩹', cat: 'cleaning', catLabel: 'Cleaning' },
  { type: 'remove_outliers', label: 'Remove Outliers', tool: 'pandas', icon: '🎯', cat: 'filter', catLabel: 'Filter' },
  { type: 'cast_types', label: 'Auto Cast Types', tool: 'pandas', icon: '🔄', cat: 'type', catLabel: 'Typing' },
];

const DAG_TASKS = [
  { id: 'extract', label: 'Extract', tool: 'pandas' },
  { id: 'profile', label: 'Profile', tool: 'pandas' },
  { id: 'quality_check', label: 'Quality (GE)', tool: 'great_expectations' },
  { id: 'transform', label: 'Transform', tool: 'pandas' },
  { id: 'dbt_generate', label: 'dbt Model', tool: 'dbt' },
  { id: 'load', label: 'Load', tool: 'pandas' },
];

const PALETTE = ['#00e5a0', '#4f8eff', '#ff9442', '#ff4d6d', '#a78bfa', '#22d3ee', '#f59e0b'];

/* ── INIT ──────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  buildStepsList();
  buildDagFlow('dagFlow', {});
  checkSession();
});

/* ── NAV ──────────────────────────────────────────── */
function switchView(name, btn) {
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  document.querySelectorAll('.side-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('view-' + name).classList.add('active');
  btn.classList.add('active');
  if (name === 'history') loadHistory();
  if (name === 'datasets') loadDatasets();
}

/* ── STEPS BUILDER ──────────────────────────────── */
function buildStepsList() {
  const list = document.getElementById('stepsList');
  list.innerHTML = PIPELINE_STEPS.map((s, i) => `
    <div class="step-chip active cat-${s.cat}" id="chip-${s.type}" onclick="toggleStep(this)">
      <span class="step-icon">${s.icon}</span>
      <span>${s.label}</span>
      <span class="cat-pill">${s.catLabel}</span>
    </div>`).join('');
  updateStepCount();
}

function toggleStep(el) {
  el.classList.toggle('active');
  updateStepCount();
}

function toggleToolCard(cb, cardId) {
  const card = document.getElementById(cardId);
  if (cb.checked) { card.classList.add('on'); } else { card.classList.remove('on'); }
}

function updateStepCount() {
  const n = document.querySelectorAll('.step-chip.active').length;
  document.getElementById('stepsCount').textContent = `${n} selected`;
}

function getSelectedSteps() {
  const steps = [];
  document.querySelectorAll('.step-chip.active').forEach(chip => {
    const type = chip.id.replace('chip-', '');
    const def = PIPELINE_STEPS.find(s => s.type === type);
    if (def) steps.push({ type, config: {} });
  });
  return steps;
}

/* ── DAG FLOW BUILDER ───────────────────────────── */
function buildDagFlow(containerId, taskStatuses) {
  const container = document.getElementById(containerId);
  if (!container) return;
  const tasks = DAG_TASKS.filter(t => {
    if (t.id === 'dbt_generate') return document.getElementById('tDbt')?.checked;
    return true;
  });
  container.innerHTML = tasks.map((t, i) => `
    <div class="dag-node">
      <div class="dag-box ${taskStatuses[t.id] || 'pending'}" id="dag-${t.id}">
        ${t.label}
      </div>
      <span class="dag-tool-tag">${t.tool}</span>
    </div>
    ${i < tasks.length - 1 ? '<div class="dag-arrow">→</div>' : ''}`
  ).join('');
}

function updateDagNode(taskName, status) {
  const el = document.getElementById('dag-' + taskName);
  if (el) { el.className = 'dag-box ' + status; }
}

/* ── FILE HANDLING ──────────────────────────────── */
const dropZone = document.getElementById('dropZone');
const fileInput = document.getElementById('fileInput');

dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('drag'); });
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag'));
dropZone.addEventListener('drop', e => {
  e.preventDefault(); dropZone.classList.remove('drag');
  if (e.dataTransfer.files[0]) setFile(e.dataTransfer.files[0]);
});
dropZone.addEventListener('click', e => {
  if (!e.target.closest('button') && !e.target.closest('.file-chosen')) fileInput.click();
});
fileInput.addEventListener('change', () => { if (fileInput.files[0]) setFile(fileInput.files[0]); });

function setFile(f) {
  currentFile = f;
  document.getElementById('fileChosen').style.display = 'flex';
  document.getElementById('fcName').textContent = `${f.name}  (${formatBytes(f.size)})`;
  document.getElementById('runBtn').disabled = false;
}

function clearFile() {
  currentFile = null; fileInput.value = '';
  document.getElementById('fileChosen').style.display = 'none';
  document.getElementById('runBtn').disabled = true;
}

/* ── SAMPLE DATA ────────────────────────────────── */
function loadSample() {
  const csv = [
    'id,name,department,salary,hire_date,location,years_exp,performance_score,skills',
    '1,Alice Chen,Engineering,145000,2019-03-15,San Francisco,6,4.8,"Python,Spark,SQL,Airflow"',
    '2,Bob Smith,Data Science,128000,2020-07-22,New York,4,4.2,"R,Python,TensorFlow"',
    '3,Carol Wu,Engineering,158000,2018-01-10,Seattle,8,4.9,"Java,Scala,Kafka,Spark"',
    '4,David Kim,Analytics,95000,2021-11-01,Chicago,3,3.8,"SQL,Tableau,Excel"',
    '5,Eva Patel,Engineering,142000,2019-08-30,San Francisco,5,4.6,"Python,dbt,Airflow"',
    '6,Frank Liu,Data Science,135000,2020-02-14,Remote,5,4.4,"Python,PyTorch,NLP"',
    '7,Grace Park,Analytics,88000,2022-04-01,Austin,2,3.9,"SQL,Power BI"',
    '8,Henry Brown,Engineering,155000,2017-09-20,Seattle,9,4.7,"Go,Kubernetes,Spark"',
    '9,Iris Zhao,Data Science,131000,2020-11-15,New York,4,4.3,"Python,Scikit-learn,SQL"',
    '10,Jack Wilson,Analytics,,2021-06-10,Chicago,3,3.7,"SQL,Looker"',
    '11,Kate Johnson,Engineering,148000,2019-05-22,Remote,6,4.5,"Python,Airflow,dbt,SQL"',
    '12,Leo Martinez,Data Science,138000,2020-01-08,San Francisco,5,4.1,"Python,Spark,MLflow"',
    '13,Mia Thompson,Engineering,161000,2016-12-01,Seattle,10,4.8,"Java,Scala,Flink,Kafka"',
    '14,Nathan Davis,Analytics,91000,2021-09-15,Austin,3,4.0,"SQL,Tableau,Python"',
    '15,Olivia Lee,Engineering,143000,2019-10-30,New York,6,4.6,"Python,Spark,Delta Lake"',
    '3,Carol Wu,Engineering,158000,2018-01-10,Seattle,8,4.9,"Java,Scala,Kafka,Spark"',
    '16,Paul Garcia,Data Science,,2020-06-18,Remote,,4.2,"Python,R,Stan"',
    '17,Quinn Adams,Engineering,152000,2018-07-22,San Francisco,7,4.5,"Python,Beam,BigQuery"',
    '18,Rachel Singh,Analytics,86000,2022-01-10,Chicago,2,3.6,"SQL,Excel,Power BI"',
    '19,Sam Chen,Engineering,149000,2019-03-05,Seattle,6,4.7,"Scala,Spark,Kafka,Flink"',
    '20,Tina Wu,Data Science,133000,2020-09-20,New York,4,4.0,"Python,TensorFlow,Keras"',
  ].join('\n');

  const file = new File([csv], 'employee_data_sample.csv', { type: 'text/csv' });
  setFile(file);
}

/* ── RUN PIPELINE ────────────────────────────────── */
async function runPipeline() {
  if (!currentFile) return;

  // Reset UI
  chartInstances.forEach(c => c.destroy()); chartInstances = [];
  document.getElementById('progressSection').style.display = 'block';
  document.getElementById('resultsSection').style.display = 'none';
  document.getElementById('runBtn').disabled = true;
  setStatus('amber', 'Running pipeline...');

  const taskStatuses = {};
  DAG_TASKS.forEach(t => taskStatuses[t.id] = 'pending');
  buildDagFlow('dagFlow', taskStatuses);
  renderTaskList(taskStatuses);

  const steps = getSelectedSteps();
  const genDbt = document.getElementById('tDbt').checked;

  let elapsed = 0;
  const timer = setInterval(() => {
    elapsed++;
    document.getElementById('progressEta').textContent = `${elapsed}s elapsed`;
  }, 1000);

  try {
    // Step 1: upload file
    updateTask('extract', 'running'); updateDagNode('extract', 'running');
    const fd = new FormData();
    fd.append('file', currentFile);

    const uploadRes = await fetch('/api/datasets/upload', { method: 'POST', body: fd });
    const uploadData = await uploadRes.json();

    if (!uploadRes.ok) throw new Error(uploadData.error || 'Upload failed');

    updateTask('extract', 'success', `${uploadData.shape.rows} rows × ${uploadData.shape.cols} cols via ${uploadData.extract_meta.engine} in ${uploadData.extract_meta.duration_s}s`);
    updateDagNode('extract', 'success');

    updateTask('profile', 'running'); updateDagNode('profile', 'running');
    await sleep(300);
    updateTask('profile', 'success', `Profiled ${uploadData.shape.cols} columns`);
    updateDagNode('profile', 'success');

    // Step 2: run pipeline
    updateTask('quality_check', 'running'); updateDagNode('quality_check', 'running');

    const pipeRes = await fetch('/api/pipeline/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ds_id: uploadData.ds_id,
        steps, use_spark: false,
        trigger_airflow: false,
        generate_dbt: genDbt,
      }),
    });
    const pipeData = await pipeRes.json();

    if (!pipeRes.ok) throw new Error(pipeData.error || 'Pipeline failed');

    // Update tasks from server response
    pipeData.tasks.forEach(t => {
      updateTask(t.task_name, t.status, t.logs);
      updateDagNode(t.task_name, t.status);
    });

    clearInterval(timer);
    lastRunData = { ...uploadData, ...pipeData };
    currentRunId = pipeData.run_id;

    await sleep(600);
    document.getElementById('progressSection').style.display = 'none';
    renderResults(lastRunData, uploadData);
    setStatus('green', 'Pipeline complete');
    updateRunCountBadge();

  } catch (e) {
    clearInterval(timer);
    setStatus('red', 'Pipeline failed');
    document.getElementById('progressSection').style.display = 'none';
    alert('Pipeline error: ' + e.message);
  }

  document.getElementById('runBtn').disabled = false;
}

/* ── RENDER TASK LIST ────────────────────────────── */
function renderTaskList(statuses) {
  const list = document.getElementById('taskList');
  list.innerHTML = DAG_TASKS.map(t => `
    <div class="task-row ${statuses[t.id] || 'pending'}" id="taskrow-${t.id}">
      <div class="task-icon ${statuses[t.id] || 'pending'}" id="taskicon-${t.id}">
        ${{ pending: '○', running: '◎', success: '✓', failed: '✗' }[statuses[t.id]] || '○'}
      </div>
      <span class="task-name">${t.label}</span>
      <span class="task-tool ${t.tool}">${t.tool}</span>
      <span class="task-dur" id="taskdur-${t.id}">—</span>
    </div>
    <div class="task-log" id="tasklog-${t.id}" style="display:none"></div>`).join('');
}

function updateTask(name, status, log) {
  const row = document.getElementById('taskrow-' + name);
  const icon = document.getElementById('taskicon-' + name);
  const logEl = document.getElementById('tasklog-' + name);
  if (!row) return;
  row.className = `task-row ${status}`;
  icon.className = `task-icon ${status}`;
  icon.textContent = { pending: '○', running: '◎', success: '✓', failed: '✗', skipped: '—' }[status] || '○';
  if (log) { logEl.style.display = 'block'; logEl.textContent = log; }
}

/* ── RENDER RESULTS ─────────────────────────────── */
function renderResults(d, uploadData) {
  document.getElementById('resultsSection').style.display = 'block';
  renderStatStrip(d, uploadData);
  renderDagResult(d);
  renderQualityCard(d.quality);
  renderMissingCard(d.missing, uploadData.shape.rows);
  renderTransformCard(d.transform_log);
  renderProfileCard(d.profile);
  renderDbtCard(d.dbt);
  renderCharts(d, uploadData);
  renderPreview(d.preview);
  renderDownload(d);
  document.getElementById('resultsSection').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function renderStatStrip(d, ud) {
  const cards = [
    { label: 'Input Rows', val: (ud.shape.rows).toLocaleString(), cls: 'green', sub: 'original records' },
    { label: 'Output Rows', val: (d.output_rows || 0).toLocaleString(), cls: 'blue', sub: 'after cleaning' },
    { label: 'Rows Removed', val: (d.rows_removed || 0).toLocaleString(), cls: 'orange', sub: 'duplicates + outliers' },
    { label: 'Quality Score', val: (d.quality?.success_pct || 0) + '%', cls: d.quality?.success_pct >= 80 ? 'green' : 'orange', sub: `${d.quality?.passed}/${d.quality?.total} checks` },
    { label: 'Transform Steps', val: (d.transform_log || []).filter(t => t.status === 'success').length, cls: 'purple', sub: 'applied successfully' },
    { label: 'Duration', val: d.duration_s + 's', cls: 'cyan', sub: 'total pipeline time' },
  ];
  document.getElementById('statStrip').innerHTML = cards.map((c, i) => `
    <div class="stat" style="animation-delay:${i * .05}s">
      <div class="stat-label">${c.label}</div>
      <div class="stat-val ${c.cls}">${c.val}</div>
      <div class="stat-sub">${c.sub}</div>
    </div>`).join('');
}

function renderDagResult(d) {
  const statuses = {};
  (d.tasks || []).forEach(t => statuses[t.task_name] = t.status);
  document.getElementById('dagRunId').textContent = d.run_id ? d.run_id.slice(0, 12) + '…' : '';
  buildDagFlow('dagFlowResult', statuses);
}

function renderQualityCard(q) {
  if (!q) { document.getElementById('qualityCard').innerHTML = '<div class="empty">No quality data</div>'; return; }
  const passColor = q.success_pct >= 80 ? 'green' : q.success_pct >= 60 ? 'orange' : 'red';
  const items = (q.results || []).slice(0, 12).map(r => `
    <div class="qc-item">
      <div class="qc-status ${r.success ? 'pass' : 'fail'}">${r.success ? '✓' : '✗'}</div>
      <span class="qc-exp">${r.expectation?.replace('expect_', '') || r.expectation}</span>
      ${r.column ? `<span class="qc-col">${r.column}</span>` : ''}
    </div>`).join('');

  document.getElementById('qualityCard').innerHTML = `
    <div class="card-hdr">
      <div class="card-icon green">✅</div>
      <div class="card-title">Great Expectations Quality</div>
      <div class="card-meta">${q.engine}</div>
    </div>
    <div class="card-body">
      <div style="display:flex;align-items:center;gap:1rem;margin-bottom:1rem">
        <div style="font-size:2.2rem;font-weight:800;color:var(--${passColor})">${q.success_pct}%</div>
        <div>
          <div style="font-size:.82rem;font-weight:600">${q.success ? '✓ All checks passed' : '⚠ Some checks failed'}</div>
          <div style="font-family:var(--mono);font-size:.65rem;color:var(--text2)">${q.passed} passed · ${q.failed} failed · ${q.total} total</div>
        </div>
      </div>
      <div class="qc-list">${items}</div>
    </div>`;
}

function renderMissingCard(missing, total) {
  const entries = Object.entries(missing || {}).filter(([, v]) => v.count > 0);
  const html = entries.length === 0
    ? '<div class="empty">✓ No missing values detected</div>'
    : entries.map(([col, v]) => {
      const cls = v.pct < 10 ? 'low' : v.pct < 40 ? 'med' : 'high';
      return `<div class="miss-row">
          <div class="miss-col" title="${col}">${col}</div>
          <div class="bar-track"><div class="bar-fill ${cls}" style="width:${Math.min(v.pct, 100)}%"></div></div>
          <div class="miss-pct">${v.pct}%</div>
        </div>`;
    }).join('');

  document.getElementById('missingCard').innerHTML = `
    <div class="card-hdr"><div class="card-icon orange">🕳</div><div class="card-title">Missing Values</div><div class="card-meta">${entries.length} cols</div></div>
    <div class="card-body">${html}</div>`;
}

function renderTransformCard(log) {
  if (!log || !log.length) { document.getElementById('transformCard').innerHTML = ''; return; }
  const items = log.map(t => `
    <div class="tlog-item">
      <div class="tlog-dot ${t.status}"></div>
      <span class="tlog-step">${t.step}</span>
      <span class="tlog-msg">${t.status === 'success'
      ? `${t.rows_before}→${t.rows_after} rows  (–${t.rows_delta || 0})  ${t.duration_s}s`
      : t.error || t.reason || ''}</span>
    </div>`).join('');

  document.getElementById('transformCard').innerHTML = `
    <div class="card-hdr"><div class="card-icon purple">⚙</div><div class="card-title">Transform Log</div><div class="card-meta">${log.filter(t => t.status === 'success').length}/${log.length} steps</div></div>
    <div class="card-body"><div class="tlog-list">${items}</div></div>`;
}

function renderProfileCard(profile) {
  if (!profile) return;
  const cols = Object.entries(profile).filter(([k]) => k !== '__meta__');
  const rows = cols.map(([, p]) => `
    <tr>
      <td><strong>${p.name}</strong></td>
      <td><span class="kpill ${p.kind}">${p.kind}</span></td>
      <td>${p.dtype}</td>
      <td>${(p.total || 0).toLocaleString()}</td>
      <td style="color:${p.missing > 0 ? 'var(--orange)' : 'var(--green)'}">
        ${p.missing} <small style="color:var(--text3)">(${p.missing_pct}%)</small>
      </td>
      <td>${p.unique}</td>
      <td>${p.kind === 'numeric' ? (p.mean ?? '-') : (p.top_values?.[0]?.value || '-')}</td>
      <td>${p.kind === 'numeric' ? (p.min ?? '-') : '-'}</td>
      <td>${p.kind === 'numeric' ? (p.max ?? '-') : '-'}</td>
      <td>${p.kind === 'numeric' ? (p.std ?? '-') : '-'}</td>
    </tr>`).join('');

  document.getElementById('profileCard').innerHTML = `
    <div class="card-hdr"><div class="card-icon blue">🔬</div><div class="card-title">Column Profiles</div><div class="card-meta">${cols.length} columns</div></div>
    <div class="card-body" style="padding:0">
      <div class="tbl-wrap">
        <table>
          <thead><tr><th>Column</th><th>Kind</th><th>Dtype</th><th>Total</th><th>Missing</th><th>Unique</th><th>Mean/Top</th><th>Min</th><th>Max</th><th>Std</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </div>`;
}

function renderDbtCard(dbt) {
  if (!dbt) { document.getElementById('dbtCard').style.display = 'none'; return; }
  document.getElementById('dbtCard').style.display = '';

  const sqlHighlighted = dbt.sql
    .replace(/\b(SELECT|FROM|WHERE|WITH|AS|JOIN|ON|GROUP BY|ORDER BY|CAST|TRIM|LEFT JOIN|INNER JOIN|HAVING|LIMIT|DISTINCT)\b/g,
      '<span class="kw">$1</span>')
    .replace(/'([^']*)'/g, "<span class='str'>'$1'</span>")
    .replace(/--.*/g, m => `<span class='cmt'>${m}</span>`);

  document.getElementById('dbtCard').innerHTML = `
    <div class="dbt-tabs">
      <div class="dbt-tab active" onclick="switchDbtTab('sql',this)">model.sql</div>
      <div class="dbt-tab" onclick="switchDbtTab('yaml',this)">schema.yml</div>
    </div>
    <div class="dbt-pane active" id="dbt-sql">
      <div style="font-family:var(--mono);font-size:.6rem;color:var(--text3);margin-bottom:.5rem">
        dbt model: <span style="color:var(--purple)">${dbt.model_name}</span> · ${dbt.row_count} rows · ${dbt.columns.length} cols
      </div>
      <pre class="dbt-sql">${sqlHighlighted}</pre>
    </div>
    <div class="dbt-pane" id="dbt-yaml">
      <pre class="dbt-sql">${dbt.schema_yaml}</pre>
    </div>`;
}

function switchDbtTab(id, btn) {
  document.querySelectorAll('.dbt-tab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.dbt-pane').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('dbt-' + id).classList.add('active');
}

function renderCharts(d, ud) {
  const grid = document.getElementById('chartsGrid');
  grid.innerHTML = '';
  chartInstances.forEach(c => c.destroy()); chartInstances = [];

  const opts = {
    responsive: true, maintainAspectRatio: false,
    plugins: {
      legend: { display: false, labels: { color: '#7a9ab0', font: { family: 'IBM Plex Mono', size: 9 }, boxWidth: 10 } },
      tooltip: { backgroundColor: '#161e2a', titleColor: '#dce8f0', bodyColor: '#7a9ab0', borderColor: 'rgba(255,255,255,.08)', borderWidth: 1 }
    },
  };
  const scaleOpts = {
    x: { ticks: { color: '#3a5568', font: { family: 'IBM Plex Mono', size: 8 }, maxRotation: 30 }, grid: { color: 'rgba(255,255,255,.03)' } },
    y: { ticks: { color: '#3a5568', font: { family: 'IBM Plex Mono', size: 8 } }, grid: { color: 'rgba(255,255,255,.03)' } },
  };

  function addChart(title, sub, type, data, extraOpts = {}) {
    const id = 'chart-' + Math.random().toString(36).slice(2);
    const div = document.createElement('div');
    div.className = 'chart-card';
    div.innerHTML = `
      <div class="chart-hdr"><span class="chart-title">${title}</span><span class="chart-sub">${sub}</span></div>
      <div class="chart-body"><div class="chart-wrap"><canvas id="${id}"></canvas></div></div>`;
    grid.appendChild(div);
    const ctx = document.getElementById(id).getContext('2d');
    const chart = new Chart(ctx, {
      type, data,
      options: { ...opts, scales: (type === 'bar' || type === 'line') ? scaleOpts : {}, ...extraOpts }
    });
    chartInstances.push(chart);
  }

  const profile = d.profile || {};
  const numCols = Object.entries(profile).filter(([k, v]) => k !== '__meta__' && v.kind === 'numeric');
  const catCols = Object.entries(profile).filter(([k, v]) => k !== '__meta__' && v.kind === 'categorical');

  // Numeric histograms
  numCols.slice(0, 3).forEach(([col, info]) => {
    if (!info.hist_counts) return;
    const labels = info.hist_bins.slice(0, -1).map((b, i) => {
      const mid = (b + info.hist_bins[i + 1]) / 2;
      return Math.abs(mid) >= 1000 ? (mid / 1000).toFixed(1) + 'K' : mid.toFixed(1);
    });
    addChart(`${col} — Distribution`, 'histogram', 'bar', {
      labels,
      datasets: [{ label: col, data: info.hist_counts, backgroundColor: '#4f8effbb', borderColor: '#4f8eff', borderWidth: 1, borderRadius: 4 }]
    });
  });

  // Categorical bar charts
  catCols.slice(0, 3).forEach(([col, info]) => {
    if (!info.top_values?.length) return;
    const top = info.top_values.slice(0, 10);
    addChart(`${col} — Top Values`, 'frequency', 'bar', {
      labels: top.map(v => v.value.length > 16 ? v.value.slice(0, 14) + '…' : v.value),
      datasets: [{ label: 'Count', data: top.map(v => v.count), backgroundColor: '#00e5a0bb', borderColor: '#00e5a0', borderWidth: 1, borderRadius: 4 }]
    });
  });

  // Missing values chart
  const missingData = Object.entries(d.missing || {}).filter(([, v]) => v.count > 0).slice(0, 10);
  if (missingData.length) {
    addChart('Missing Values by Column', '%', 'bar', {
      labels: missingData.map(([c]) => c.length > 12 ? c.slice(0, 10) + '…' : c),
      datasets: [{ label: 'Missing %', data: missingData.map(([, v]) => v.pct), backgroundColor: '#ff944288', borderColor: '#ff9442', borderWidth: 1, borderRadius: 4 }]
    });
  }

  // Quality pie
  if (d.quality) {
    addChart('Quality Check Results', 'GE expectations', 'doughnut', {
      labels: ['Passed', 'Failed'],
      datasets: [{ data: [d.quality.passed, d.quality.failed], backgroundColor: ['#00e5a0bb', '#ff4d6dbb'], borderColor: '#0b1018', borderWidth: 2 }]
    }, { plugins: { legend: { display: true } }, cutout: '65%' });
  }

  // Correlation heatmap as bar
  if (d.correlation?.columns?.length >= 2) {
    const cols = d.correlation.columns;
    const matrix = d.correlation.matrix;
    // take first col's correlations with others
    const labels = cols.slice(1);
    const vals = matrix[0].slice(1);
    addChart(`Correlation with ${cols[0]}`, 'Pearson r', 'bar', {
      labels,
      datasets: [{ label: 'r', data: vals, backgroundColor: vals.map(v => v > 0 ? '#00e5a0bb' : '#ff4d6dbb'), borderRadius: 4 }]
    });
  }

  if (grid.children.length === 0) {
    grid.innerHTML = '<div style="grid-column:1/-1"><div class="empty">No chart data available</div></div>';
  }
}

function renderPreview(preview) {
  if (!preview) return;
  const { columns, rows } = preview;
  document.getElementById('previewCard').innerHTML = `
    <div class="card-hdr"><div class="card-icon green">👁</div><div class="card-title">Cleaned Data Preview</div><div class="card-meta">First ${rows.length} rows</div></div>
    <div class="card-body" style="padding:0">
      <div class="preview-wrap">
        <table>
          <thead><tr>${columns.map(c => `<th>${c}</th>`).join('')}</tr></thead>
          <tbody>${rows.slice(0, 50).map(row => `<tr>${row.map(cell => `<td>${String(cell).length > 40 ? String(cell).slice(0, 38) + '…' : cell}</td>`).join('')}</tr>`).join('')}</tbody>
        </table>
      </div>
    </div>`;
}

function renderDownload(d) {
  document.getElementById('dlSection').innerHTML = `
    <div class="dl-info">
      <h3>🎉 Pipeline complete</h3>
      <p>${d.input_rows || 0} → ${d.output_rows || 0} rows · ${d.rows_removed || 0} removed · ${d.quality?.success_pct || 0}% quality score</p>
    </div>
    <div class="dl-btns">
      <button class="btn-reset" onclick="resetApp()">↩ New Pipeline</button>
      <button class="btn-dl csv" onclick="downloadFile('/api/pipeline/download/${d.run_id}','cleaned_${(d.run_id || '').slice(0, 8)}.csv')">
        ⬇ CSV
      </button>
      <button class="btn-dl parquet" onclick="downloadFile('/api/pipeline/download/${d.run_id}?format=parquet','cleaned_${(d.run_id || '').slice(0, 8)}.parquet')">
        ⬇ Parquet
      </button>
      ${d.dbt ? `<button class="btn-dl dbt" onclick="copyDbt()">🔧 Copy dbt SQL</button>` : ''}
    </div>`;
}

function copyDbt() {
  if (lastRunData?.dbt?.sql) {
    navigator.clipboard.writeText(lastRunData.dbt.sql).then(() => alert('dbt SQL copied!'));
  }
}

/* ── HISTORY VIEW ───────────────────────────────── */
async function loadHistory() {
  const statsRes = await apiFetch('/api/history/stats');
  if (statsRes) {
    const s = statsRes;
    document.getElementById('historyStats').innerHTML = [
      { label: 'Total Runs', val: s.total_runs, cls: 'blue' },
      { label: 'Successful', val: s.success_runs, cls: 'green' },
      { label: 'Failed', val: s.failed_runs, cls: 'red' },
      { label: 'Success Rate', val: s.success_rate + '%', cls: 'green' },
      { label: 'Rows Processed', val: (s.total_rows_processed || 0).toLocaleString(), cls: 'cyan' },
      { label: 'Rows Cleaned', val: (s.rows_cleaned || 0).toLocaleString(), cls: 'purple' },
    ].map(c => `<div class="stat"><div class="stat-label">${c.label}</div><div class="stat-val ${c.cls}">${c.val}</div></div>`).join('');
  }

  const runsRes = await apiFetch('/api/history/runs');
  const runs = runsRes || [];
  const tableHTML = runs.length === 0
    ? '<div class="empty">No pipeline runs yet</div>'
    : `<div class="tbl-wrap">
        <table>
          <thead><tr><th>Run ID</th><th>Dataset</th><th>Status</th><th>In→Out Rows</th><th>Duration</th><th>Quality</th><th>Started</th></tr></thead>
          <tbody>${runs.map(r => `
            <tr>
              <td style="font-family:var(--mono);color:var(--blue)">${r.run_id.slice(0, 12)}…</td>
              <td>${r.pipeline_config?.ds_id || '—'}</td>
              <td><span class="kpill" style="background:${statusBg(r.status)};color:${statusFg(r.status)}">${r.status}</span></td>
              <td>${(r.input_rows || 0).toLocaleString()} → ${(r.output_rows || 0).toLocaleString()}</td>
              <td>${r.duration_s ? r.duration_s + 's' : '—'}</td>
              <td>—</td>
              <td>${r.started_at ? new Date(r.started_at).toLocaleString() : '—'}</td>
            </tr>`).join('')}
          </tbody>
        </table>
      </div>`;

  document.getElementById('historyTable').innerHTML = tableHTML;
  document.getElementById('runCountBadge').textContent = runs.length;
}

/* ── DATASETS VIEW ──────────────────────────────── */
async function loadDatasets() {
  const data = await apiFetch('/api/datasets/') || [];
  const html = data.length === 0
    ? '<div class="card-body"><div class="empty">No datasets uploaded yet</div></div>'
    : `<div class="card-hdr"><div class="card-icon green">🗄</div><div class="card-title">Your Datasets</div><div class="card-meta">${data.length} total</div></div>
       <div class="card-body" style="padding:0">
         <div class="tbl-wrap">
           <table>
             <thead><tr><th>File</th><th>Format</th><th>Rows</th><th>Cols</th><th>Size</th><th>Uploaded</th><th></th></tr></thead>
             <tbody>${data.map(d => `
               <tr>
                 <td>${d.original_name || d.name}</td>
                 <td><span class="kpill categorical">${d.file_format}</span></td>
                 <td>${(d.row_count || 0).toLocaleString()}</td>
                 <td>${d.col_count || 0}</td>
                 <td>${formatBytes(d.file_size_bytes || d.file_size || 0)}</td>
                 <td>${new Date(d.uploaded_at).toLocaleString()}</td>
                 <td style="white-space:nowrap">
                   <span onclick="downloadFile('/api/datasets/${d.id}/download','${d.original_name || d.name}')" style="color:var(--green);font-family:var(--mono);font-size:.62rem;cursor:pointer;margin-right:.5rem">⬇ Download</span>
                   <span onclick="deleteDatasetById(${d.id})" style="color:var(--red);font-family:var(--mono);font-size:.62rem;cursor:pointer">✕</span>
                 </td>
               </tr>`).join('')}
             </tbody>
           </table>
         </div>
       </div>`;
  document.getElementById('datasetsTable').innerHTML = html;
}

async function deleteDatasetById(id) {
  if (!confirm('Delete this dataset?')) return;
  await fetch('/api/datasets/' + id, { method: 'DELETE' });
  loadDatasets();
}

/* ── AUTH ───────────────────────────────────────── */
function openModal(mode) { authMode = mode; setAuthMode(mode); document.getElementById('authModal').classList.add('open'); }
function closeModal() { document.getElementById('authModal').classList.remove('open'); }
function setAuthMode(mode) {
  authMode = mode;
  document.getElementById('modalTitle').textContent = mode === 'login' ? 'Sign in' : 'Create account';
  document.getElementById('modalSub').textContent = mode === 'login' ? 'Access your DataForge workspace' : 'Start engineering data';
  document.getElementById('authSubmit').textContent = mode === 'login' ? 'Sign in' : 'Create account';
  document.getElementById('emailField').style.display = mode === 'register' ? 'block' : 'none';
  document.getElementById('modalSwitch').innerHTML = mode === 'login'
    ? 'No account? <a onclick="switchAuthMode()">Register</a>'
    : 'Have an account? <a onclick="switchAuthMode()">Sign in</a>';
  document.getElementById('authErr').textContent = '';
}
function switchAuthMode() { setAuthMode(authMode === 'login' ? 'register' : 'login'); }

async function submitAuth() {
  const username = document.getElementById('authUsername').value.trim();
  const password = document.getElementById('authPassword').value;
  const email = document.getElementById('authEmail').value.trim();
  const errEl = document.getElementById('authErr');

  const url = authMode === 'login' ? '/api/auth/login' : '/api/auth/register';
  const body = authMode === 'login' ? { username, password } : { username, email, password };

  try {
    const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const data = await res.json();
    if (!res.ok) { errEl.textContent = data.error || 'Auth failed'; return; }
    closeModal();
    updateUserUI(data.user);
    updateRunCountBadge();
  } catch (e) { errEl.textContent = 'Network error'; }
}

async function doLogout() {
  await fetch('/api/auth/logout', { method: 'POST' });
  document.getElementById('authArea').style.display = 'flex';
  document.getElementById('userArea').style.display = 'none';
}

async function checkSession() {
  const res = await fetch('/api/auth/me');
  if (res.ok) { const u = await res.json(); updateUserUI(u); updateRunCountBadge(); }
}

function updateUserUI(user) {
  document.getElementById('authArea').style.display = 'none';
  document.getElementById('userArea').style.display = 'flex';
  document.getElementById('userGreet').textContent = `👋 ${user.username}`;
}

/* ── HELPERS ─────────────────────────────────────── */
async function apiFetch(url) {
  try { const r = await fetch(url); return r.ok ? r.json() : null; }
  catch { return null; }
}

function setStatus(state, text) {
  const dot = document.getElementById('statusDot');
  const txt = document.getElementById('statusText');
  dot.className = 'dot ' + state;
  txt.textContent = text;
}



async function updateRunCountBadge() {
  const data = await apiFetch('/api/history/stats');
  if (data) document.getElementById('runCountBadge').textContent = data.total_runs;
}

function resetApp() {
  clearFile();
  chartInstances.forEach(c => c.destroy()); chartInstances = [];
  document.getElementById('resultsSection').style.display = 'none';
  document.getElementById('progressSection').style.display = 'none';
  setStatus('green', 'Ready');
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

function formatBytes(b) {
  if (!b) return '—';
  if (b < 1024) return b + 'B';
  if (b < 1048576) return (b / 1024).toFixed(1) + 'KB';
  return (b / 1048576).toFixed(1) + 'MB';
}

function statusBg(s) {
  return {
    success: 'rgba(0,229,160,.1)', failed: 'rgba(255,77,109,.1)', running: 'rgba(79,142,255,.1)',
    raw: 'rgba(120,120,120,.1)', profiled: 'rgba(79,142,255,.1)', cleaned: 'rgba(0,229,160,.1)', partial: 'rgba(255,148,66,.1)'
  }[s] || 'var(--s3)';
}
function statusFg(s) {
  return {
    success: '#00e5a0', failed: '#ff4d6d', running: '#4f8eff',
    raw: '#7a9ab0', profiled: '#4f8eff', cleaned: '#00e5a0', partial: '#ff9442'
  }[s] || '#7a9ab0';
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function downloadFile(url, filename) {
  try {
    const res = await fetch(url, { credentials: 'same-origin' });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'Download failed' }));
      alert(err.error || 'Download failed');
      return;
    }
    const blob = await res.blob();
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = filename || 'download';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(a.href);
  } catch (e) {
    alert('Download error: ' + e.message);
  }
}
