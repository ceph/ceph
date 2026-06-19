export interface LokiStreamValue {
  stream: Record<string, string>;
  values: [string, string][];
}

export interface LokiQueryRangeResponse {
  resultType: 'streams' | 'matrix' | 'vector';
  result: LokiStreamValue[];
  stats?: Record<string, unknown>;
}

export interface LokiQueryResponse {
  resultType: 'streams' | 'matrix' | 'vector';
  result: LokiStreamValue[] | LokiInstantVector[];
  stats?: Record<string, unknown>;
}

export interface LokiInstantVector {
  metric: Record<string, string>;
  value: [number, string];
}

export interface LokiLogLine {
  stamp: string;
  message: string;
  filename: string;
}

export interface LokiLogsViewState {
  selectedFilename: string;
  hasRun: boolean;
}

export const LOKI_CLUSTER_LOGS_JOB = 'Cluster Logs';
