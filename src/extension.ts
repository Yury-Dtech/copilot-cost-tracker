import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { Worker } from 'node:worker_threads';
import * as vscode from 'vscode';

type UsageEvent = {
  timestamp: number;
  model: string;
  charge: number;
  eventId: string;
  sessionId: string;
  mode: string;
  cachedTokens: number;
  inputTokens: number;
  outputTokens: number;
};
type WorkerResponse = { file: string; size: number; mtimeMs: number; fresh: boolean; events: UsageEvent[] };

/** A scanned file plus the workspace it belongs to, resolved once per scan. */
type ScannedFile = { file: string; workspace: string };

/** Per-model token/cost roll-up, including cache efficiency. */
type ModelDetail = {
  nanoAiu: number;
  events: number;
  inputTokens: number;
  outputTokens: number;
  cachedTokens: number;
};

/** One message thread (conversation) folded into a single row. */
type ThreadUsage = {
  id: string;
  workspace: string;
  nanoAiu: number;
  events: number;
  firstAt: number;
  lastAt: number;
  models: Map<string, number>;
};

type WorkspaceUsage = {
  name: string;
  nanoAiu: number;
  events: number;
  threads: number;
};

type UsageSnapshot = {
  month: string;
  monthUsd: number;
  dayUsd: number;
  sessionUsd: number;
  models: Map<string, ModelDetail>;
  dayModels: Map<string, ModelDetail>;
  sessionModels: Map<string, ModelDetail>;
  threads: ThreadUsage[];
  workspaces: WorkspaceUsage[];
  overall: ModelDetail;
  /** Days of the current month with at least one recorded request, ascending dd. */
  coveredDays: number[];
  /** Days elapsed in the current month including today (local). */
  elapsedDays: number;
  sessionStart: number;
  sourceFound: boolean;
};

const nanoAiuPerCredit = 1_000_000_000;
/** The worker reads only appended bytes, so a short interval stays cheap. */
const refreshIntervalMs = 10 * 1000;
const startupDelayMs = 3000;

/** Session = since this extension activated in this VS Code window. */
let sessionStart = Date.now();

/** Label used for logs that live outside any workspace (global debug logs). */
const GLOBAL_WORKSPACE = 'Global (all windows)';

function monthKey(date: Date): string {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
}

function dayKey(date: Date): string {
  return `${monthKey(date)}-${String(date.getDate()).padStart(2, '0')}`;
}

function findJsonlFiles(): ScannedFile[] {
  const appData = process.env.APPDATA;
  if (!appData) {
    return [];
  }

  const userRoot = path.join(appData, 'Code', 'User');
  const roots: { directory: string; workspace: string }[] = [
    { directory: path.join(userRoot, 'globalStorage', 'github.copilot-chat', 'debug-logs'), workspace: GLOBAL_WORKSPACE },
    { directory: path.join(userRoot, 'globalStorage', 'github.copilot-chat', 'agentHostUsage'), workspace: GLOBAL_WORKSPACE },
    { directory: path.join(userRoot, 'globalStorage', 'agentHostUsage'), workspace: GLOBAL_WORKSPACE }
  ];

  try {
    for (const workspace of fs.readdirSync(path.join(userRoot, 'workspaceStorage'), { withFileTypes: true })) {
      if (workspace.isDirectory()) {
        roots.push({
          directory: path.join(userRoot, 'workspaceStorage', workspace.name, 'GitHub.copilot-chat', 'debug-logs'),
          workspace: workspaceName(path.join(userRoot, 'workspaceStorage', workspace.name))
        });
      }
    }
  } catch {
    // No workspaceStorage (portable install or fresh profile).
  }

  const files: ScannedFile[] = [];
  const visit = (directory: string, workspace: string): void => {
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(directory, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const fullPath = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        visit(fullPath, workspace);
      } else if (entry.isFile() && entry.name.endsWith('.jsonl')) {
        files.push({ file: fullPath, workspace });
      }
    }
  };

  for (const root of roots) {
    visit(root.directory, root.workspace);
  }
  return files;
}

/**
 * Reads a workspaceStorage folder's `workspace.json` and turns the folder or
 * .code-workspace URI into a short human label; falls back to the folder id.
 */
function workspaceName(folder: string): string {
  try {
    const raw = fs.readFileSync(path.join(folder, 'workspace.json'), 'utf8');
    const target = JSON.parse(raw).folder ?? JSON.parse(raw).workspace;
    if (typeof target === 'string') {
      const decoded = decodeURIComponent(target.replace(/^file:\/\//, ''));
      const base = path.basename(decoded).replace(/\.code-workspace$/i, '');
      if (base) {
        return base;
      }
    }
  } catch {
    // Untitled workspace or missing metadata: fall back to the folder id.
  }
  return `Workspace ${path.basename(folder)}`;
}

function toUsd(nanoAiu: number, usdPerCredit: number): number {
  return (nanoAiu / nanoAiuPerCredit) * usdPerCredit;
}

function newDetail(): ModelDetail {
  return { nanoAiu: 0, events: 0, inputTokens: 0, outputTokens: 0, cachedTokens: 0 };
}

function addEvent(detail: ModelDetail, event: UsageEvent): void {
  detail.nanoAiu += event.charge;
  detail.events += 1;
  detail.inputTokens += event.inputTokens;
  detail.outputTokens += event.outputTokens;
  detail.cachedTokens += event.cachedTokens;
}

function bump(target: Map<string, ModelDetail>, model: string, event: UsageEvent): void {
  const detail = target.get(model) ?? newDetail();
  addEvent(detail, event);
  target.set(model, detail);
}

/**
 * Off-main-thread scanner. The worker parses only files whose size/mtime
 * changed; the host aggregates cached events, so refreshes are near-free.
 */
class ScanRunner {
  private worker: Worker | undefined;
  private ready: Promise<void> | undefined;
  private pending: ((responses: WorkerResponse[]) => void) | undefined;

  private ensureWorker(): Promise<void> {
    if (this.ready) {
      return this.ready;
    }

    this.worker = new Worker(path.join(__dirname, 'scannerWorker.js'));
    this.worker.unref();
    this.ready = new Promise((resolve, reject) => {
      this.worker!.on('message', (message: unknown) => {
        if (message === 'ready') {
          resolve();
          return;
        }
        if (Array.isArray(message)) {
          this.pending?.(message as WorkerResponse[]);
          this.pending = undefined;
        }
      });
      this.worker!.on('error', reject);
    });
    return this.ready;
  }

  async scan(files: string[], monthBoundary: number, scanBoundary: number): Promise<WorkerResponse[]> {
    await this.ensureWorker();
    return new Promise<WorkerResponse[]>((resolve, reject) => {
      const worker = this.worker;
      if (!worker) {
        reject(new Error('Scanner worker is not available.'));
        return;
      }

      const onError = (error: Error): void => {
        this.pending = undefined;
        reject(error);
      };
      worker.once('error', onError);
      this.pending = responses => {
        worker.off('error', onError);
        resolve(responses);
      };
      worker.postMessage({ files, monthBoundary, scanBoundary });
    });
  }

  dispose(): void {
    void this.worker?.terminate();
    this.worker = undefined;
    this.ready = undefined;
  }
}

function formatUsd(value: number): string {
  if (value > 0 && value < 0.01) {
    return '<$0.01';
  }
  return value.toLocaleString('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });
}

function modelRows(models: Map<string, ModelDetail>, usdPerCredit: number, empty: string): string {
  if (models.size === 0) {
    return empty;
  }
  return [...models.entries()]
    .sort((left, right) => right[1].nanoAiu - left[1].nanoAiu)
    .map(([model, usage]) => {
      const safe = model.replace(/[|`*_\\]/g, character => `\\${character}`);
      return `- ${safe} · **${formatUsd(toUsd(usage.nanoAiu, usdPerCredit))}**`;
    })
    .join('\n');
}

/** Share of prompt tokens served from cache; `undefined` when no tokens were logged. */
function cacheRate(detail: ModelDetail): number | undefined {
  if (detail.inputTokens <= 0) {
    return undefined;
  }
  return Math.min(1, detail.cachedTokens / detail.inputTokens);
}

function formatPct(value: number | undefined): string {
  if (value === undefined) {
    return '—';
  }
  return `${Math.round(value * 100)}%`;
}

function renderReportHtml(data: UsageSnapshot | undefined, usdPerCredit: number): string {
  const style = `
    :root { color-scheme: light dark; }
    body { font-family: var(--vscode-font-family); padding: 20px 24px; color: var(--vscode-foreground); max-width: 1100px; }
    h1 { font-size: 1.4em; font-weight: 600; margin: 0 0 4px; }
    .sub { color: var(--vscode-descriptionForeground); font-size: 0.85em; margin-bottom: 20px; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 24px; }
    .card { border: 1px solid var(--vscode-panel-border, rgba(128,128,128,.35)); border-radius: 8px; padding: 12px 14px; }
    .card .label { color: var(--vscode-descriptionForeground); font-size: 0.78em; text-transform: uppercase; letter-spacing: .04em; }
    .card .value { font-size: 1.6em; font-weight: 600; margin-top: 4px; }
    section { margin-bottom: 28px; }
    h2 { font-size: 1.05em; font-weight: 600; margin: 0 0 10px; }
    table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
    th { text-align: left; color: var(--vscode-descriptionForeground); font-weight: 500; padding: 6px 8px; border-bottom: 1px solid var(--vscode-panel-border, rgba(128,128,128,.35)); }
    td { padding: 6px 8px; border-bottom: 1px solid var(--vscode-panel-border, rgba(128,128,128,.15)); vertical-align: middle; }
    th.num, td.num { text-align: right; font-variant-numeric: tabular-nums; }
    .bar { height: 6px; border-radius: 3px; background: var(--vscode-progressBar-background, #0e70c0); min-width: 2px; }
    .barwrap { background: rgba(128,128,128,.2); border-radius: 3px; width: 100%; min-width: 60px; }
    .share { display: flex; align-items: center; gap: 8px; }
    .muted { color: var(--vscode-descriptionForeground); }
    .empty { color: var(--vscode-descriptionForeground); font-style: italic; }
    .tabs { display: flex; gap: 6px; margin-bottom: 12px; }
    .tab { padding: 4px 12px; border-radius: 6px; cursor: pointer; border: 1px solid transparent; font-size: 0.85em; }
    .tab.active { background: var(--vscode-button-background); color: var(--vscode-button-foreground); }
    .tab:not(.active):hover { border-color: var(--vscode-panel-border, rgba(128,128,128,.35)); }
    .pill { display: inline-block; padding: 1px 7px; border-radius: 10px; font-size: 0.78em; background: rgba(128,128,128,.2); }
    .mono { font-family: var(--vscode-editor-font-family, monospace); font-size: 0.85em; }
    .coverage { display: flex; flex-wrap: wrap; gap: 4px; }
    .day { display: inline-flex; align-items: center; justify-content: center; width: 26px; height: 26px; border-radius: 5px; font-size: 0.72em; font-variant-numeric: tabular-nums; }
    .day-on { background: var(--vscode-progressBar-background, #0e70c0); color: #fff; }
    .day-off { background: rgba(128,128,128,.18); color: var(--vscode-descriptionForeground); border: 1px dashed rgba(128,128,128,.5); }
  `;

  if (!data || !data.sourceFound) {
    return `<!doctype html><html><head><meta charset="utf-8"><style>${style}</style></head><body>
      <h1>Copilot costs</h1>
      <p class="empty">No recognized Copilot usage logs are available yet. Use Copilot Chat once and press Refresh.</p>
    </body></html>`;
  }

  const usd = (nanoAiu: number): string => formatUsd(toUsd(nanoAiu, usdPerCredit));
  const share = (part: number, whole: number): number => (whole > 0 ? part / whole : 0);

  const modelTable = (models: Map<string, ModelDetail>): string => {
    const rows = [...models.entries()].sort((left, right) => right[1].nanoAiu - left[1].nanoAiu);
    if (rows.length === 0) {
      return '<p class="empty">No usage recorded for this period.</p>';
    }
    const total = rows.reduce((sum, [, detail]) => sum + detail.nanoAiu, 0);
    const body = rows
      .map(([model, detail]) => {
        const pct = share(detail.nanoAiu, total);
        const rate = cacheRate(detail);
        const cacheText = rate === undefined ? '<span class="muted">—</span>' : formatPct(rate);
        return `<tr>
          <td>${escapeHtml(model)}</td>
          <td class="num">${detail.events}</td>
          <td class="num">${usd(detail.nanoAiu)}</td>
          <td>
            <div class="share">
              <div class="barwrap"><div class="bar" style="width:${Math.max(2, Math.round(pct * 100))}%"></div></div>
              <span class="muted">${Math.round(pct * 100)}%</span>
            </div>
          </td>
          <td class="num">${cacheText}</td>
          <td class="num muted">${detail.inputTokens.toLocaleString('en-US')} / ${detail.outputTokens.toLocaleString('en-US')}</td>
        </tr>`;
      })
      .join('');
    return `<table><thead><tr>
      <th>Model</th><th class="num">Requests</th><th class="num">Cost</th><th>Share</th><th class="num">Cache hit</th><th class="num">In / Out tokens</th>
    </tr></thead><tbody>${body}</tbody></table>`;
  };

  const workspaceRows = [...data.workspaces].sort((left, right) => right.nanoAiu - left.nanoAiu);
  const workspaceTable = workspaceRows.length === 0
    ? '<p class="empty">No workspace usage recorded.</p>'
    : `<table><thead><tr><th>Workspace</th><th class="num">Threads</th><th class="num">Requests</th><th class="num">Cost</th><th>Share</th></tr></thead><tbody>${workspaceRows
        .map(usage => {
          const pct = share(usage.nanoAiu, data.overall.nanoAiu);
          return `<tr>
            <td>${escapeHtml(usage.name)}</td>
            <td class="num">${usage.threads}</td>
            <td class="num">${usage.events}</td>
            <td class="num">${usd(usage.nanoAiu)}</td>
            <td><div class="share"><div class="barwrap"><div class="bar" style="width:${Math.max(2, Math.round(pct * 100))}%"></div></div><span class="muted">${Math.round(pct * 100)}%</span></div></td>
          </tr>`;
        })
        .join('')}</tbody></table>`;

  const threadRows = [...data.threads].sort((left, right) => right.nanoAiu - left.nanoAiu).slice(0, 40);
  const threadTable = threadRows.length === 0
    ? '<p class="empty">No conversations recorded.</p>'
    : `<table><thead><tr><th>Conversation</th><th>Workspace</th><th class="num">Top model</th><th class="num">Requests</th><th class="num">Cost</th><th>When</th></tr></thead><tbody>${threadRows
        .map(thread => {
          const topModel = [...thread.models.entries()].sort((left, right) => right[1] - left[1])[0]?.[0] ?? '—';
          const when = new Date(thread.lastAt).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
          return `<tr>
            <td class="mono" title="${escapeHtml(thread.id)}">${escapeHtml(thread.id.slice(0, 8))}</td>
            <td>${escapeHtml(thread.workspace)}</td>
            <td class="num">${escapeHtml(topModel)}</td>
            <td class="num">${thread.events}</td>
            <td class="num"><strong>${usd(thread.nanoAiu)}</strong></td>
            <td class="muted">${when}</td>
          </tr>`;
        })
        .join('')}</tbody></table>`;

  const overallRate = cacheRate(data.overall);
  const since = new Date(data.sessionStart).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });

  // Compact day strip: one cell per elapsed day of the month, filled when logged.
  const covered = new Set(data.coveredDays);
  const missingDays: number[] = [];
  const dayCells: string[] = [];
  for (let d = 1; d <= data.elapsedDays; d++) {
    const has = covered.has(d);
    if (!has) {
      missingDays.push(d);
    }
    dayCells.push(
      `<span class="day ${has ? 'day-on' : 'day-off'}" title="Day ${d}: ${has ? 'data present' : 'no local data'}">${d}</span>`
    );
  }
  const coverageNote = missingDays.length === 0
    ? 'Every elapsed day this month has local data.'
    : `${data.coveredDays.length} of ${data.elapsedDays} elapsed days have local data. Missing: ${missingDays.join(', ')}. These may be usage from another device, the browser, mobile, or logs the Copilot extension already rotated.`;

  return `<!doctype html><html><head><meta charset="utf-8"><style>${style}</style></head><body>
    <h1>Copilot costs</h1>
    <div class="sub">Locally reported AI credits, converted at $${usdPerCredit}/credit. <strong>This reflects this VS Code only; the GitHub profile may show more.</strong></div>

    <div class="cards">
      <div class="card"><div class="label">Month (${escapeHtml(data.month)})</div><div class="value">${formatUsd(data.monthUsd)}</div></div>
      <div class="card"><div class="label">Today</div><div class="value">${formatUsd(data.dayUsd)}</div></div>
      <div class="card"><div class="label">Session (since ${since})</div><div class="value">${formatUsd(data.sessionUsd)}</div></div>
      <div class="card"><div class="label">Prompt cache hit</div><div class="value">${formatPct(overallRate)}</div></div>
      <div class="card"><div class="label">Requests this month</div><div class="value">${data.overall.events.toLocaleString('en-US')}</div></div>
    </div>

    <section>
      <h2>Data coverage</h2>
      <div class="coverage">${dayCells.join('')}</div>
      <p class="muted" style="font-size:.82em;margin-top:8px">${escapeHtml(coverageNote)}</p>
    </section>

    <section>
      <h2>Cost by model</h2>
      <div class="tabs" role="tablist">
        <div class="tab active" data-target="tab-month">Month</div>
        <div class="tab" data-target="tab-day">Today</div>
        <div class="tab" data-target="tab-session">Session</div>
      </div>
      <div id="tab-month">${modelTable(data.models)}</div>
      <div id="tab-day" hidden>${modelTable(data.dayModels)}</div>
      <div id="tab-session" hidden>${modelTable(data.sessionModels)}</div>
      <p class="muted" style="font-size:.8em;margin-top:8px">Cache hit = cached prompt tokens ÷ total prompt tokens for the period.</p>
    </section>

    <section>
      <h2>Cost by workspace</h2>
      ${workspaceTable}
    </section>

    <section>
      <h2>Most expensive conversations</h2>
      ${threadTable}
      ${data.threads.length > threadRows.length ? `<p class="muted">Showing top ${threadRows.length} of ${data.threads.length} conversations.</p>` : ''}
    </section>

    <script>
      const tabs = document.querySelectorAll('.tab');
      for (const tab of tabs) {
        tab.addEventListener('click', () => {
          for (const other of tabs) {
            other.classList.toggle('active', other === tab);
            document.getElementById(other.dataset.target).hidden = other !== tab;
          }
        });
      }
    </script>
  </body></html>`;
}

export function activate(context: vscode.ExtensionContext): void {
  sessionStart = Date.now();

  const statusBar = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
  statusBar.command = 'copilotCostTracker.showReport';
  statusBar.text = '$(calendar) … · $(clock) … · $(pulse) …';
  statusBar.show();
  context.subscriptions.push(statusBar);

  const readUsdPerCredit = (): number =>
    vscode.workspace.getConfiguration('copilotCostTracker').get<number>('usdPerCredit', 0.01);

  const runner = new ScanRunner();
  const allEvents = new Map<string, UsageEvent[]>();
  /** File path -> workspace label, refreshed on every scan. */
  const fileWorkspace = new Map<string, string>();
  let snapshot: UsageSnapshot | undefined;
  let timer: NodeJS.Timeout | undefined;
  let refreshing = false;

  const aggregate = (): UsageSnapshot => {
    const now = new Date();
    const targetMonth = monthKey(now);
    const targetDay = dayKey(now);
    const usdPerCredit = readUsdPerCredit();
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1).getTime();
    const models = new Map<string, ModelDetail>();
    const dayModels = new Map<string, ModelDetail>();
    const sessionModels = new Map<string, ModelDetail>();
    const overall = newDetail();
    const threads = new Map<string, ThreadUsage>();
    const workspaces = new Map<string, WorkspaceUsage>();
    const covered = new Set<number>();
    let monthNanoAiu = 0;
    let dayNanoAiu = 0;
    let sessionNanoAiu = 0;

    for (const [file, events] of allEvents) {
      const workspace = fileWorkspace.get(file) ?? GLOBAL_WORKSPACE;
      for (const event of events) {
        if (event.timestamp < monthStart) {
          continue;
        }

        monthNanoAiu += event.charge;
        covered.add(new Date(event.timestamp).getDate());
        addEvent(overall, event);
        bump(models, event.model, event);
        if (dayKey(new Date(event.timestamp)) === targetDay) {
          dayNanoAiu += event.charge;
          bump(dayModels, event.model, event);
        }
        if (event.timestamp >= sessionStart) {
          sessionNanoAiu += event.charge;
          bump(sessionModels, event.model, event);
        }

        // Workspace roll-up.
        const workspaceUsage = workspaces.get(workspace) ?? { name: workspace, nanoAiu: 0, events: 0, threads: 0 };
        workspaceUsage.nanoAiu += event.charge;
        workspaceUsage.events += 1;
        workspaces.set(workspace, workspaceUsage);

        // Thread (conversation) roll-up; records without a session id are skipped.
        if (event.sessionId) {
          const thread = threads.get(event.sessionId) ?? {
            id: event.sessionId,
            workspace,
            nanoAiu: 0,
            events: 0,
            firstAt: event.timestamp,
            lastAt: event.timestamp,
            models: new Map<string, number>()
          };
          thread.nanoAiu += event.charge;
          thread.events += 1;
          thread.firstAt = Math.min(thread.firstAt, event.timestamp);
          thread.lastAt = Math.max(thread.lastAt, event.timestamp);
          thread.models.set(event.model, (thread.models.get(event.model) ?? 0) + event.charge);
          threads.set(event.sessionId, thread);
        }
      }
    }

    // Distinguish workspace threads from total charges for the threads column.
    for (const thread of threads.values()) {
      const workspaceUsage = workspaces.get(thread.workspace);
      if (workspaceUsage) {
        workspaceUsage.threads += 1;
      }
    }

    return {
      month: targetMonth,
      monthUsd: toUsd(monthNanoAiu, usdPerCredit),
      dayUsd: toUsd(dayNanoAiu, usdPerCredit),
      sessionUsd: toUsd(sessionNanoAiu, usdPerCredit),
      models,
      dayModels,
      sessionModels,
      threads: [...threads.values()],
      workspaces: [...workspaces.values()],
      overall,
      coveredDays: [...covered].sort((left, right) => left - right),
      elapsedDays: now.getDate(),
      sessionStart,
      sourceFound: allEvents.size > 0
    };
  };

  const render = (): void => {
    const data = snapshot;
    if (!data) {
      statusBar.text = '$(info) Copilot costs: no logs';
      return;
    }

    statusBar.text = data.sourceFound
      ? `$(calendar) ${formatUsd(data.monthUsd)} · $(clock) ${formatUsd(data.dayUsd)} · $(pulse) ${formatUsd(data.sessionUsd)}`
      : '$(info) Copilot costs: no logs';

    const usdPerCredit = readUsdPerCredit();
    const since = new Date(data.sessionStart).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
    const tooltip = new vscode.MarkdownString(
      [
        '### Copilot costs',
        '',
        '| Period | USD |',
        '| :--- | ---: |',
        `| Month (${data.month}) | **${formatUsd(data.monthUsd)}** |`,
        `| Today | **${formatUsd(data.dayUsd)}** |`,
        `| Session (since ${since}) | **${formatUsd(data.sessionUsd)}** |`,
        '',
        '---',
        '',
        '**Month by model**',
        '',
        modelRows(data.models, usdPerCredit, '_No usage recorded_'),
        '',
        '**Today by model**',
        '',
        modelRows(data.dayModels, usdPerCredit, '_No usage recorded_'),
        '',
        '**Session by model**',
        '',
        modelRows(data.sessionModels, usdPerCredit, '_No usage recorded_')
      ].join('\n'),
      true
    );
    tooltip.supportHtml = false;
    statusBar.tooltip = tooltip;
    statusBar.show();
  };

  const refresh = async (): Promise<void> => {
    if (refreshing) {
      return;
    }
    refreshing = true;
    const started = Date.now();
    try {
      const files = findJsonlFiles();
      const now = new Date();
      const monthStart = new Date(now.getFullYear(), now.getMonth(), 1).getTime();
      const scanBoundary = new Date(now.getFullYear(), now.getMonth() - 1, 1).getTime();
      const responses = await runner.scan(files.map(entry => entry.file), monthStart, scanBoundary);
      const live = new Set(files.map(entry => entry.file));
      for (const file of allEvents.keys()) {
        if (!live.has(file)) {
          allEvents.delete(file);
        }
      }
      fileWorkspace.clear();
      for (const entry of files) {
        fileWorkspace.set(entry.file, entry.workspace);
      }
      for (const response of responses) {
        allEvents.set(response.file, response.events);
      }
      snapshot = aggregate();
    } catch (error) {
      console.error('Copilot Cost Tracker scan failed', error);
    } finally {
      refreshing = false;
      const elapsed = Date.now() - started;
      if (elapsed > 1500) {
        console.warn(`Copilot Cost Tracker: scan took ${elapsed} ms`);
      }
      render();
    }
  };

  const startTimer = (): void => {
    if (timer) {
      clearInterval(timer);
    }
    timer = setInterval(() => void refresh(), refreshIntervalMs);
  };

  const showReport = (): void => {
    const data = snapshot;
    const usdPerCredit = readUsdPerCredit();
    const panel = vscode.window.createWebviewPanel('copilotCostReport', 'Copilot Credit Cost', vscode.ViewColumn.One, {
      enableScripts: true
    });

    panel.webview.html = renderReportHtml(data, usdPerCredit);
  };

  context.subscriptions.push(
    vscode.commands.registerCommand('copilotCostTracker.showReport', showReport),
    vscode.commands.registerCommand('copilotCostTracker.refresh', () => {
      void refresh();
      startTimer();
    }),
    vscode.commands.registerCommand('copilotCostTracker.exportCsv', async () => {
      const data = snapshot;
      if (!data) {
        return;
      }
      const usdPerCredit = readUsdPerCredit();
      const uri = await vscode.window.showSaveDialog({
        defaultUri: vscode.Uri.file(path.join(os.homedir(), `copilot-cost-${data.month}.csv`)),
        filters: { CSV: ['csv'] }
      });
      if (!uri) {
        return;
      }
      const lines = ['period,model,events,usd,input_tokens,output_tokens,cached_tokens'];
      for (const [period, models] of [
        ['month', data.models],
        ['day', data.dayModels],
        ['session', data.sessionModels]
      ] as const) {
        for (const [model, usage] of models) {
          lines.push(
            `${period},${csv(model)},${usage.events},${toUsd(usage.nanoAiu, usdPerCredit).toFixed(6)},${usage.inputTokens},${usage.outputTokens},${usage.cachedTokens}`
          );
        }
      }
      await fs.promises.writeFile(uri.fsPath, lines.join('\r\n'), 'utf8');
    }),
    vscode.workspace.onDidChangeConfiguration(event => {
      if (event.affectsConfiguration('copilotCostTracker.usdPerCredit')) {
        void refresh();
      }
    }),
    new vscode.Disposable(() => {
      if (timer) {
        clearInterval(timer);
      }
      runner.dispose();
    })
  );

  // Never block activation: the first scan runs after the window has settled.
  setTimeout(() => void refresh(), startupDelayMs);
  startTimer();
}

function escapeHtml(value: string): string {
  return value.replace(/[&<>"']/g, character => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' })[character] ?? character);
}

function csv(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}