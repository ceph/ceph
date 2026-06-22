import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { Observable, of } from 'rxjs';
import { catchError, map, switchMap, tap } from 'rxjs/operators';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';
import {
  buildGroupPath,
  buildSubvolumePath,
  folderPathFromFileInput,
  getMirrorPath,
  normalizeMirroringPath,
  splitResolvedSubvolumePath,
  toGroupParam,
  toMirroringPathSelections,
  withResolvedDisplayPath
} from '../mirroring-path-utils';
import {
  createPathEntry,
  DirLevel,
  MirroringPathSelection,
  PathEntry
} from '../mirroring-path.model';

@Component({
  selector: 'cd-mirroring-paths-step',
  templateUrl: './mirroring-paths-step.component.html',
  styleUrls: ['./mirroring-paths-step.component.scss'],
  standalone: false
})
export class MirroringPathsStepComponent implements OnInit, TearsheetStep {
  @Input() fsName: string;
  @Input() fsId: number;
  @Output() pathsChanged = new EventEmitter<MirroringPathSelection[]>();

  formGroup!: CdFormGroup;
  paths: PathEntry[] = [];

  constructor(
    private cephfsService: CephfsService,
    private subvolumeGroupService: CephfsSubvolumeGroupService,
    private subvolumeService: CephfsSubvolumeService
  ) {}

  ngOnInit(): void {
    this.createForm();
    this.paths = [createPathEntry([])];
    this.resolveFsId();
    this.loadSubvolumeGroups();
  }

  createForm(): void {
    this.formGroup = new CdFormGroup({
      pathsControl: new UntypedFormControl([], Validators.required)
    });
  }

  addPath(): void {
    const groupOptions = this.paths[0]?.levels[0]?.options ?? [];
    this.paths.push(createPathEntry([...groupOptions]));
  }

  removePath(index: number): void {
    this.paths.splice(index, 1);
    this.syncFormValue();
  }

  toggleExpand(index: number): void {
    this.paths[index].expanded = !this.paths[index].expanded;
  }

  onLevelChange(pathIndex: number, levelIndex: number, selected: string): void {
    const entry = this.paths[pathIndex];
    if (!entry) {
      return;
    }

    const levels = entry.levels.map((level, index) =>
      index === levelIndex ? { ...level, selected } : level
    );
    levels.splice(levelIndex + 1);

    const updatedEntry: PathEntry = { ...entry, levels };

    if (!selected) {
      this.updatePath(pathIndex, withResolvedDisplayPath(updatedEntry));
      return;
    }

    const kind = levels[levelIndex].kind;
    if (kind === 'group') {
      this.handleGroupSelection(pathIndex, updatedEntry, selected);
    } else if (kind === 'subvolume') {
      this.handleSubvolumeSelection(pathIndex, updatedEntry, selected);
    } else {
      this.handleDirSelection(pathIndex, withResolvedDisplayPath(updatedEntry));
    }
  }

  onFilePickerSelected(event: Event): void {
    const file = (event.target as HTMLInputElement)?.files?.[0];
    if (!file) {
      return;
    }
    const folderPath = folderPathFromFileInput(file);
    this.paths.push({
      fullPath: folderPath,
      levels: [],
      expanded: false,
      filePickerPath: folderPath
    });
    this.syncFormValue();
    (event.target as HTMLInputElement).value = '';
  }

  getPathSelections(): MirroringPathSelection[] {
    return toMirroringPathSelections(this.paths);
  }

  getValidPaths(): string[] {
    return this.paths.map((entry) => getMirrorPath(entry)).filter(Boolean);
  }

  getDisplayPath(entry: PathEntry): string {
    return getMirrorPath(entry) || '—';
  }

  private handleGroupSelection(pathIndex: number, entry: PathEntry, groupName: string): void {
    const updatedEntry = withResolvedDisplayPath({
      ...entry,
      subvol: undefined,
      subvolumePath: undefined,
      resolvedSubvolumeRoot: undefined,
      group: toGroupParam(groupName),
      fullPath: buildGroupPath(groupName)
    });

    this.updatePath(pathIndex, updatedEntry);

    this.subvolumeService
      .get(this.fsName, toGroupParam(groupName), false)
      .pipe(catchError(() => of([])))
      .subscribe((subvols: { name: string }[]) => {
        const current = this.paths[pathIndex];
        if (!current) {
          return;
        }
        const subvolNames = subvols.map((subvol) => subvol.name).filter(Boolean);
        this.updatePath(pathIndex, {
          ...current,
          levels: [
            ...current.levels,
            { options: subvolNames, selected: '', kind: 'subvolume' as const }
          ]
        });
      });
  }

  private handleSubvolumeSelection(pathIndex: number, entry: PathEntry, subvolName: string): void {
    const groupName = entry.levels[0].selected;
    const groupParam = toGroupParam(groupName);

    this.updatePath(
      pathIndex,
      withResolvedDisplayPath({
        ...entry,
        subvol: subvolName,
        group: groupParam,
        fullPath: buildSubvolumePath(groupName, subvolName)
      })
    );

    this.subvolumeService
      .info(this.fsName, subvolName, groupParam)
      .pipe(
        catchError(() => of({ path: null })),
        switchMap((info: { path?: string }) => {
          const resolvedPath = info?.path ?? buildSubvolumePath(groupName, subvolName);
          const { parentPath, lastSegment } = splitResolvedSubvolumePath(resolvedPath);
          return this.listChildDirNames(parentPath).pipe(
            map((childNames) => ({ resolvedPath, parentPath, lastSegment, childNames }))
          );
        })
      )
      .subscribe(({ resolvedPath, parentPath, lastSegment, childNames }) => {
        const current = this.paths[pathIndex];
        if (!current) {
          return;
        }

        const dirOptions = childNames.length > 0 ? childNames : lastSegment ? [lastSegment] : [];
        const levels: DirLevel[] =
          dirOptions.length > 0
            ? [...current.levels, { options: dirOptions, selected: '', kind: 'dir' as const }]
            : current.levels;

        this.updatePath(
          pathIndex,
          withResolvedDisplayPath({
            ...current,
            subvol: subvolName,
            group: groupParam,
            subvolumePath: parentPath,
            resolvedSubvolumeRoot: resolvedPath,
            fullPath: buildSubvolumePath(groupName, subvolName),
            levels
          })
        );
      });
  }

  private handleDirSelection(pathIndex: number, entry: PathEntry): void {
    this.updatePath(pathIndex, entry);

    this.listChildDirNames(entry.fullPath).subscribe((childNames) => {
      const current = this.paths[pathIndex];
      if (!current || childNames.length === 0) {
        return;
      }
      this.updatePath(pathIndex, {
        ...current,
        levels: [...current.levels, { options: childNames, selected: '', kind: 'dir' as const }]
      });
    });
  }

  private updatePath(pathIndex: number, entry: PathEntry): void {
    if (!this.paths[pathIndex]) {
      return;
    }
    this.paths = this.paths.map((current, index) => (index === pathIndex ? entry : current));
    this.syncFormValue();
  }

  private syncFormValue(): void {
    const validPaths = this.getValidPaths();
    this.formGroup.get('pathsControl')?.setValue(validPaths);
    this.formGroup.get('pathsControl')?.updateValueAndValidity();
    this.pathsChanged.emit(this.getPathSelections());
  }

  private resolveFsId(): void {
    if (!this.fsName) {
      return;
    }
    this.ensureFsId().subscribe();
  }

  private ensureFsId(): Observable<number> {
    if (!this.fsName) {
      return of(this.fsId ?? 0);
    }
    return this.cephfsService.list().pipe(
      map((filesystems: { id?: number; mdsmap?: { fs_name?: string } }[]) => {
        const match = filesystems.find((fs) => fs.mdsmap?.fs_name === this.fsName);
        return match?.id ?? this.fsId ?? 0;
      }),
      tap((id) => {
        if (id) {
          this.fsId = id;
        }
      })
    );
  }

  private listChildDirNames(parentPath: string): Observable<string[]> {
    const parent = normalizeMirroringPath(parentPath);
    return this.ensureFsId().pipe(
      switchMap((fsId) => {
        if (!fsId) {
          return of([] as string[]);
        }
        return this.cephfsService.lsDir(fsId, parent, 2).pipe(
          catchError(() => of([])),
          map((dirs) =>
            dirs
              .filter((dir) => normalizeMirroringPath(dir.parent) === parent)
              .map((dir) => dir.name)
              .filter(Boolean)
              .sort()
          )
        );
      })
    );
  }

  private loadSubvolumeGroups(): void {
    this.subvolumeGroupService
      .get(this.fsName, false)
      .pipe(catchError(() => of([])))
      .subscribe((groups: { name: string }[]) => {
        const groupNames = groups.map((group) => group.name).filter(Boolean);
        this.paths = this.paths.map((entry) => ({
          ...entry,
          levels: entry.levels.map((level, index) =>
            index === 0 ? { ...level, options: groupNames } : level
          )
        }));
      });
  }
}
