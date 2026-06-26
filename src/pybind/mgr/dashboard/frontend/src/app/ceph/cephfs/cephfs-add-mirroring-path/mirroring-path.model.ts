export interface DirTreeEntry {
  name: string;
  parent: string;
}

export interface DirLevel {
  options: string[];
  selected: string;
  kind: 'group' | 'subvolume' | 'dir';
}

export interface PathEntry {
  fullPath: string;
  levels: DirLevel[];
  expanded: boolean;
  subvolumePath?: string;
  resolvedSubvolumeRoot?: string;
}

export function createPathEntry(groupOptions: string[], expanded = true): PathEntry {
  return {
    fullPath: '',
    levels: [{ options: groupOptions, selected: '', kind: 'group' }],
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
