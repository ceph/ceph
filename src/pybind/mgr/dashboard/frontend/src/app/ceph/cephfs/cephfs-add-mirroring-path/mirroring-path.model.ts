export interface PathLevel {
  options: string[];
  selected: string;
}

export interface PathEntry {
  fullPath: string;
  levels: PathLevel[];
  expanded: boolean;
}

export function createPathEntry(expanded = true): PathEntry {
  return {
    fullPath: '',
    levels: [{ options: [], selected: '' }],
    expanded
  };
}

export interface ExistingScheduleEntry {
  id: string;
  path: string;
  existingSchedule: string;
  filesReplicating: string;
  rawPath: string;
  schedule: string;
  start: string;
  retention: Record<string, number>;
  subvol: string;
  group: string;
  fs: string;
  action: 'keep' | 'replace';
}

export interface PathSubmitFailure {
  path: string;
  detail: string;
}

export interface PathSubmitOutput {
  failed: PathSubmitFailure[];
  scheduleFailed: PathSubmitFailure[];
  alreadyMirrored: string[];
  skippedByServer: string[];
  succeeded: string[];
}
