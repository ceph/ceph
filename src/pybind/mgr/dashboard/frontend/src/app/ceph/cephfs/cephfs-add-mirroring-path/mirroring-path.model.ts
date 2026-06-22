export interface MirroringPathSelection {
  path: string;
  subvol?: string;
  group?: string;
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
  filePickerPath?: string;
  subvol?: string;
  group?: string;
}

export function createPathEntry(groupOptions: string[], expanded = true): PathEntry {
  return {
    fullPath: '',
    levels: [{ options: groupOptions, selected: '', kind: 'group' }],
    expanded
  };
}
