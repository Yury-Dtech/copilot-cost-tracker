import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { Worker } from 'node:worker_threads';
import * as vscode from 'vscode';

type ModelUsage = { nanoAiu: number; events: number };
type UsageEvent = { timestamp: number; model: string; charge: number; eventId: string };
type WorkerResponse = { file: string; size: number; mtimeMs: number; fresh: boolean; events: UsageEvent[] };

type UsageSnapshot = {
  month: string;
  monthUsd: number;
  dayUsd: number;
  sessionUsd: number;
  models: Map<string, ModelUsage>;
  dayModels: Map<string, ModelUsage>;
  sessionModels: Map<string, ModelUsage>;
  sessionStart: number;
  sourceFound: boolean;
};

const nanoAiuPerCredit = 1_000_000_000;
/** The worker reads only appended bytes, so a short interval stays cheap. */
const refreshIntervalMs = 10 * 1000;
const startupDelayMs = 3000;

/** Session = since this extension activated in this VS Code window. */
let sessionStart = Date.now();

function monthKey(date: Date): string {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
}

function dayKey(date: Date): string {
  return `${monthKey(date)}-${String(date.getDate()).padStart(2, '0')}`;
}

function findJsonlFiles(): string[] {
  const appData = process.env.APPDATA;
  if (!appData) {
    return [];
  }

  const userRoot = path.join(appData, 'Code', 'User');
  const roots = [
    path.join(userRoot, 'globalStorage', 'github.copilot-chat', 'debug-logs'),
    path.join(userRoot, 'globalStorage', 'github.copilot-chat', 'agentHostUsage'),
    path.join(userRoot, 'globalStorage', 'agentHostUsage')
  ];

  try {
    for (const workspace of fs.readdirSync(path.join(userRoot, 'workspaceStorage'), { withFileTypes: true })) {
      if (workspace.isDirectory()) {
        roots.push(path.join(userRoot, 'workspaceStorage', workspace.name, 'GitHub.copilot-chat', 'debug-logs'));
      }
    }
  } catch {
    // No workspaceStorage (portable install or fresh profile).
  }

  const files: string[] = [];
  const visit = (directory: string): void => {
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(directory, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const fullPath = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        visit(fullPath);
      } else if (entry.isFile() && entry.name.endsWith('.jsonl')) {
        files.push(fullPath);
      }
    }
  };

  for (const root of roots) {
    visit(root);
  }
  return files;
}

function toUsd(nanoAiu: number, usdPerCredit: number): number {
  return (nanoAiu / nanoAiuPerCredit) * usdPerCredit;
}

function bump(target: Map<string, ModelUsage>, model: string, charge: number): void {
  const usage = target.get(model) ?? { nanoAiu: 0, events: 0 };
  usage.nanoAiu += charge;
  usage.events += 1;
  target.set(model, usage);
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

function modelRows(models: Map<string, ModelUsage>, usdPerCredit: number, empty: string): string {
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
  let snapshot: UsageSnapshot | undefined;
  let timer: NodeJS.Timeout | undefined;
  let refreshing = false;

  const aggregate = (): UsageSnapshot => {
    const now = new Date();
    const targetMonth = monthKey(now);
    const targetDay = dayKey(now);
    const usdPerCredit = readUsdPerCredit();
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1).getTime();
    const models = new Map<string, ModelUsage>();
    const dayModels = new Map<string, ModelUsage>();
    const sessionModels = new Map<string, ModelUsage>();
    let monthNanoAiu = 0;
    let dayNanoAiu = 0;
    let sessionNanoAiu = 0;

    for (const events of allEvents.values()) {
      for (const event of events) {
        if (event.timestamp < monthStart) {
          continue;
        }

        monthNanoAiu += event.charge;
        bump(models, event.model, event.charge);
        if (dayKey(new Date(event.timestamp)) === targetDay) {
          dayNanoAiu += event.charge;
          bump(dayModels, event.model, event.charge);
        }
        if (event.timestamp >= sessionStart) {
          sessionNanoAiu += event.charge;
          bump(sessionModels, event.model, event.charge);
        }
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
      const responses = await runner.scan(files, monthStart, scanBoundary);
      const live = new Set(files);
      for (const file of allEvents.keys()) {
        if (!live.has(file)) {
          allEvents.delete(file);
        }
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
    const panel = vscode.window.createWebviewPanel('copilotCostReport', 'Copilot Credit Cost', vscode.ViewColumn.One, {});
    const table = (models: Map<string, ModelUsage>): string => {
      const rows = [...models.entries()].sort((left, right) => right[1].nanoAiu - left[1].nanoAiu);
      if (rows.length === 0) {
        return '<p>No usage recorded.</p>';
      }
      return `<table><thead><tr><th>Model</th><th>Events</th><th>USD</th></tr></thead><tbody>${rows
        .map(([model, usage]) => `<tr><td>${escapeHtml(model)}</td><td>${usage.events}</td><td>${formatUsd(toUsd(usage.nanoAiu, usdPerCredit))}</td></tr>`)
        .join('')}</tbody></table>`;
    };

    panel.webview.html = `<!doctype html><html><body style="font-family:var(--vscode-font-family);padding:16px">
      <h2>Copilot costs</h2>
      <p>Month <strong>${data ? formatUsd(data.monthUsd) : '—'}</strong> · Today <strong>${data ? formatUsd(data.dayUsd) : '—'}</strong> · Session <strong>${data ? formatUsd(data.sessionUsd) : '—'}</strong></p>
      ${data && data.sourceFound
        ? `<h3>Month by model</h3>${table(data.models)}<h3>Today by model</h3>${table(data.dayModels)}<h3>Session by model</h3>${table(data.sessionModels)}`
        : '<p>No recognized Copilot usage logs are available yet.</p>'}
    </body></html>`;
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
      const lines = ['period,model,events,usd'];
      for (const [period, models] of [
        ['month', data.models],
        ['day', data.dayModels],
        ['session', data.sessionModels]
      ] as const) {
        for (const [model, usage] of models) {
          lines.push(`${period},${csv(model)},${usage.events},${toUsd(usage.nanoAiu, usdPerCredit).toFixed(6)}`);
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