import { Observable, of } from 'rxjs';

export interface PathLevel {
  options$: Observable<string[]>;
  options: string[];
  loading: boolean;
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
    levels: [{ options$: of([]), options: [], loading: false, selected: '' }],
    expanded
  };
}

export interface PathSubmitFailure {
  path: string;
  detail: string;
}

export interface PathSubmitOutput {
  failed: PathSubmitFailure[];
  alreadyMirrored: string[];
  skippedByServer: string[];
  succeeded: string[];
}
