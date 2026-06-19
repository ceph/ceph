import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import {
  LOKI_CLUSTER_LOGS_JOB,
  LokiLogLine,
  LokiLogsViewState,
  LokiQueryRangeResponse,
  LokiQueryResponse
} from '../models/loki.interface';

export interface LokiQueryOptions {
  since?: string;
  start?: string;
  end?: string;
  limit?: number;
  direction?: 'forward' | 'backward';
  step?: number | string;
  interval?: number | string;
  time?: string;
}

@Injectable({
  providedIn: 'root'
})
export class LokiService {
  private readonly baseUrl = 'api/loki';
  private readonly viewStateStorageKey = 'dashboard_loki_logs_view';

  constructor(private http: HttpClient) {}

  getLabels(streamSelector?: string, options: LokiQueryOptions = {}): Observable<string[]> {
    return this.http.get<string[]>(`${this.baseUrl}/labels`, {
      params: this.buildHttpParams(streamSelector, options)
    });
  }

  getLabelValues(
    label: string,
    streamSelector?: string,
    options: LokiQueryOptions = {}
  ): Observable<string[]> {
    return this.http.get<string[]>(`${this.baseUrl}/label/${label}/values`, {
      params: this.buildHttpParams(streamSelector, options)
    });
  }

  getLogFilenames(since = '7d'): Observable<string[]> {
    return this.getLabelValues('filename', `{job="${LOKI_CLUSTER_LOGS_JOB}"}`, { since });
  }

  query(logql: string, options: LokiQueryOptions = {}): Observable<LokiQueryResponse> {
    return this.http.get<LokiQueryResponse>(`${this.baseUrl}/query`, {
      params: this.buildHttpParams(logql, options)
    });
  }

  queryRange(logql: string, options: LokiQueryOptions = {}): Observable<LokiQueryRangeResponse> {
    return this.http.get<LokiQueryRangeResponse>(`${this.baseUrl}/query_range`, {
      params: this.buildHttpParams(logql, options)
    });
  }

  // getLogs(filename: string): Observable<LokiLogLine[]> {
  //   return this.getLogLinesForFilename(filename, { since: '24h', limit: 5000 });
  // }

  getLogLinesForFilename(
    filename: string,
    options: LokiQueryOptions = {}
  ): Observable<LokiLogLine[]> {
    const logql = this.buildFilenameQuery(filename);
    const queryOptions: LokiQueryOptions = {
      since: '24h',
      limit: 1000,
      direction: 'backward',
      ...options
    };
    return this.queryRange(logql, queryOptions).pipe(
      map((response) => this.parseLogLines(response, filename))
    );
  }

  getViewState(): LokiLogsViewState | null {
    try {
      const raw = localStorage.getItem(this.viewStateStorageKey);
      if (!raw) {
        return null;
      }
      return JSON.parse(raw) as LokiLogsViewState;
    } catch {
      return null;
    }
  }

  saveViewState(state: LokiLogsViewState): void {
    localStorage.setItem(this.viewStateStorageKey, JSON.stringify(state));
  }

  clearViewState(): void {
    localStorage.removeItem(this.viewStateStorageKey);
  }

  logLinesToText(lines: LokiLogLine[]): string {
    return lines.map((line) => `${line.stamp}\t${line.message}`).join('\n');
  }

  downloadFileNameFromPath(filename: string): string {
    if (!filename) {
      return 'loki_log';
    }
    const base = filename.split('/').pop() ?? 'loki_log';
    return base.replace(/\.log$/, '') || 'loki_log';
  }

  buildFilenameQuery(filename: string): string {
    const escaped = filename.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
    return `{job="${LOKI_CLUSTER_LOGS_JOB}", filename="${escaped}"}`;
  }

  parseLogLines(response: LokiQueryRangeResponse, defaultFilename = ''): LokiLogLine[] {
    if (!response?.result?.length) {
      return [];
    }

    const lines: LokiLogLine[] = [];
    response.result.forEach((stream) => {
      const filename = stream.stream?.filename ?? defaultFilename;
      stream.values?.forEach(([timestampNs, message]) => {
        lines.push({
          stamp: this.timestampNsToIso(timestampNs),
          message,
          filename
        });
      });
    });

    return lines.sort((a, b) => b.stamp.localeCompare(a.stamp));
  }

  private buildHttpParams(logql: string | undefined, options: LokiQueryOptions): HttpParams {
    let params = new HttpParams();
    if (logql) {
      params = params.set('params', logql);
    }
    if (options.since) {
      params = params.set('since', options.since);
    }
    if (options.start) {
      params = params.set('start', options.start);
    }
    if (options.end) {
      params = params.set('end', options.end);
    }
    if (options.limit !== undefined) {
      params = params.set('limit', String(options.limit));
    }
    if (options.direction) {
      params = params.set('direction', options.direction);
    }
    if (options.step !== undefined) {
      params = params.set('step', String(options.step));
    }
    if (options.interval !== undefined) {
      params = params.set('interval', String(options.interval));
    }
    if (options.time) {
      params = params.set('time', options.time);
    }
    return params;
  }

  private timestampNsToIso(timestampNs: string): string {
    const ms = Math.floor(Number(timestampNs) / 1_000_000);
    if (Number.isNaN(ms)) {
      return timestampNs;
    }
    return new Date(ms).toISOString();
  }
}
