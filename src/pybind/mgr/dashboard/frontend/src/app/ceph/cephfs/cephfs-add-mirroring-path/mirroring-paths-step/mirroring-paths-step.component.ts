import { Component, DestroyRef, inject, Input, OnInit } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { FormControl } from '@angular/forms';
import { forkJoin, Observable, of } from 'rxjs';
import { catchError, map, switchMap, take } from 'rxjs/operators';

import { DEFAULT_SUBVOLUME_GROUP } from '~/app/shared/constants/cephfs.constant';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephfsSubvolumeGroupService } from '~/app/shared/api/cephfs-subvolume-group.service';
import { CephfsDir } from '~/app/shared/models/cephfs-directory-models';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';
import { MirroringPathUtils } from '../mirroring-path-utils';
import { createPathEntry, DirTreeEntry, PathEntry } from '../mirroring-path.model';

@Component({
  selector: 'cd-mirroring-paths-step',
  templateUrl: './mirroring-paths-step.component.html',
  styleUrls: ['./mirroring-paths-step.component.scss'],
  standalone: false
})
export class MirroringPathsStepComponent implements OnInit, TearsheetStep {
  @Input() fsName: string;
  @Input() fsId: number;

  formGroup!: CdFormGroup;
  paths: PathEntry[] = [];
  private trackedPaths = new Set<string>();
  private cachedGroupNames: string[] = [];
  private dirTree: DirTreeEntry[] = [];
  private destroyRef = inject(DestroyRef);

  constructor(
    private cephfsService: CephfsService,
    private subvolumeGroupService: CephfsSubvolumeGroupService
  ) {}

  ngOnInit(): void {
    this.formGroup = new CdFormGroup({
      pathsControl: new FormControl<string[]>([], { nonNullable: true })
    });
    this.paths = [createPathEntry([])];
    this.syncFormValue();
    this.loadInitialData();
  }

  get pathsControl(): FormControl<string[]> {
    return this.formGroup.get('pathsControl') as FormControl<string[]>;
  }

  get pathsError(): string {
    const control = this.pathsControl;
    if (!control.invalid || !(control.touched || control.dirty)) {
      return '';
    }
    if (control.hasError('alreadyMirrored')) {
      return $localize`Selected path(s) are already mirrored. Select a path that is not already mirrored.`;
    }
    return $localize`Select at least one path to continue.`;
  }

  addPath(): void {
    this.paths.push(createPathEntry([...this.filterAvailableGroupNames()]));
  }

  removePath(index: number): void {
    this.paths.splice(index, 1);
    this.refreshGroupOptions();
    this.syncFormValue();
  }

  toggleExpand(index: number): void {
    this.paths[index].expanded = !this.paths[index].expanded;
  }

  onLevelChange(pathIndex: number, levelIndex: number, selected: string): void {
    const entry = this.paths[pathIndex];
    if (!entry) return;

    const levels = entry.levels.map((level, i) =>
      i === levelIndex ? { ...level, selected } : level
    );
    levels.splice(levelIndex + 1);

    const updatedEntry: PathEntry = { ...entry, levels };

    if (!selected) {
      this.updatePath(pathIndex, MirroringPathUtils.withResolvedDisplayPath(updatedEntry));
      return;
    }

    const kind = levels[levelIndex].kind;
    if (kind === 'group') {
      this.handleGroupSelection(pathIndex, updatedEntry, selected);
    } else if (kind === 'subvolume') {
      this.handleSubvolumeSelection(pathIndex, updatedEntry, selected);
    } else {
      this.handleDirSelection(pathIndex, MirroringPathUtils.withResolvedDisplayPath(updatedEntry));
    }
  }

  getSubmitPaths(): { toAdd: string[]; alreadyMirrored: string[] } {
    const toAdd: string[] = [];
    const alreadyMirrored: string[] = [];
    this.paths.forEach((entry, i) => {
      const rawPath = MirroringPathUtils.getMirrorPath(entry);
      if (!rawPath) return;
      const path = MirroringPathUtils.normalizeMirroringPath(rawPath);
      if (this.isPathAvailable(path, i)) toAdd.push(path);
      else if (MirroringPathUtils.isMirroringPathTracked(path, this.trackedPaths))
        alreadyMirrored.push(path);
    });
    return { toAdd, alreadyMirrored };
  }

  addTrackedPath(path: string): void {
    const normalized = MirroringPathUtils.normalizeMirroringPath(path);
    if (normalized) {
      this.trackedPaths.add(normalized);
      this.syncFormValue();
    }
  }

  refreshTrackedPaths(): Observable<void> {
    if (!this.fsName) return of(undefined);
    return this.cephfsService.listMirrorDirectories(this.fsName).pipe(
      map((response) => {
        this.trackedPaths = MirroringPathUtils.toTrackedPathSet(
          MirroringPathUtils.parseMirrorDirectoryList(response)
        );
        this.syncFormValue();
      }),
      catchError(() => of(undefined)),
      take(1)
    );
  }

  private handleGroupSelection(pathIndex: number, entry: PathEntry, groupName: string): void {
    const groupPath = MirroringPathUtils.buildGroupPath(groupName);
    const updated = MirroringPathUtils.withResolvedDisplayPath({
      ...entry,
      subvolumePath: undefined,
      resolvedSubvolumeRoot: undefined,
      fullPath: groupPath
    });

    const usedSubvols = this.getUsedSubvolumes(groupName, pathIndex);
    const subvolNames = this.getChildNamesFromTree(groupPath).filter(
      (name) =>
        !usedSubvols.has(name) &&
        !MirroringPathUtils.isMirroringPathTracked(
          MirroringPathUtils.buildSubvolumePath(groupName, name),
          this.trackedPaths
        )
    );

    this.updatePath(pathIndex, {
      ...updated,
      levels: [
        ...updated.levels,
        { options: subvolNames, selected: '', kind: 'subvolume' as const }
      ]
    });
  }

  private handleSubvolumeSelection(pathIndex: number, entry: PathEntry, subvolName: string): void {
    const groupName = entry.levels[0].selected;
    const subvolPath = MirroringPathUtils.buildSubvolumePath(groupName, subvolName);
    const dirChildren = this.getChildNamesFromTree(subvolPath).filter((name) =>
      this.isPathAvailable(MirroringPathUtils.joinMirroringPath(subvolPath, name), pathIndex)
    );

    this.updatePath(
      pathIndex,
      MirroringPathUtils.withResolvedDisplayPath({
        ...entry,
        fullPath: subvolPath,
        subvolumePath: subvolPath,
        resolvedSubvolumeRoot: subvolPath,
        levels: [
          ...entry.levels.slice(0, 2),
          ...(dirChildren.length
            ? [{ options: dirChildren, selected: '', kind: 'dir' as const }]
            : [])
        ]
      })
    );
  }

  private handleDirSelection(pathIndex: number, entry: PathEntry): void {
    const parentPath = MirroringPathUtils.normalizeMirroringPath(
      MirroringPathUtils.getMirrorPath(entry) || entry.fullPath
    );
    const childNames = this.getChildNamesFromTree(parentPath).filter((name) =>
      this.isPathAvailable(MirroringPathUtils.joinMirroringPath(parentPath, name), pathIndex)
    );
    this.updatePath(
      pathIndex,
      childNames.length
        ? {
            ...entry,
            levels: [...entry.levels, { options: childNames, selected: '', kind: 'dir' as const }]
          }
        : entry
    );
  }

  private updatePath(pathIndex: number, entry: PathEntry): void {
    if (!this.paths[pathIndex]) return;
    this.paths = this.paths.map((c, i) => (i === pathIndex ? entry : c));
    this.syncFormValue();
  }

  private syncFormValue(): void {
    const { toAdd, alreadyMirrored } = this.getSubmitPaths();
    const control = this.pathsControl;
    control.setValue(toAdd, { emitEvent: false });
    if (toAdd.length) {
      control.setErrors(null);
    } else if (alreadyMirrored.length) {
      control.setErrors({ alreadyMirrored: true });
    } else {
      control.setErrors({ required: true });
    }
  }

  private loadInitialData(): void {
    if (!this.fsName) return;

    this.resolveFs()
      .pipe(
        switchMap((fsId) =>
          forkJoin([
            this.subvolumeGroupService.get(this.fsName, false).pipe(catchError(() => of([]))),
            fsId
              ? this.cephfsService.lsDir(fsId, '/volumes', 3).pipe(catchError(() => of([])))
              : of([]),
            this.cephfsService.listMirrorDirectories(this.fsName).pipe(
              map((r) => MirroringPathUtils.parseMirrorDirectoryList(r)),
              catchError(() => of([] as string[]))
            )
          ])
        ),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe(([groups, dirs, trackedList]: [{ name: string }[], CephfsDir[], string[]]) => {
        this.dirTree = dirs.map(({ name, parent }) => ({ name, parent }));
        this.trackedPaths = MirroringPathUtils.toTrackedPathSet(trackedList);

        const groupNames = groups.map((g) => MirroringPathUtils.normalizeGroupOptionName(g.name));
        if (!groupNames.includes(DEFAULT_SUBVOLUME_GROUP))
          groupNames.unshift(DEFAULT_SUBVOLUME_GROUP);
        this.cachedGroupNames = groupNames;

        const available = this.filterAvailableGroupNames();
        this.paths = this.paths.map((entry) => ({
          ...entry,
          levels: entry.levels.map((level, i) =>
            i === 0 ? { ...level, options: available } : level
          )
        }));
        this.syncFormValue();
      });
  }

  private resolveFs(): Observable<number> {
    return this.fsId
      ? of(this.fsId)
      : this.cephfsService.list().pipe(
          map(
            (fs: { id?: number; mdsmap?: { fs_name?: string } }[]) =>
              fs.find((f) => f.mdsmap?.fs_name === this.fsName)?.id ?? 0
          ),
          map((id) => {
            this.fsId = id;
            return id;
          }),
          catchError(() => of(0))
        );
  }

  private refreshGroupOptions(): void {
    const available = this.filterAvailableGroupNames();
    this.paths = this.paths.map((entry) => ({
      ...entry,
      levels: entry.levels.map((level, i) => (i === 0 ? { ...level, options: available } : level))
    }));
  }

  private filterAvailableGroupNames(): string[] {
    return this.cachedGroupNames.filter(
      (name) =>
        !MirroringPathUtils.isGroupPathMirrored(
          MirroringPathUtils.buildGroupPath(name),
          this.trackedPaths
        )
    );
  }

  private getUsedSubvolumes(groupName: string, excludeIndex: number): Set<string> {
    const used = new Set<string>();
    this.paths.forEach((entry, i) => {
      if (i === excludeIndex) return;
      const group = entry.levels.find((l) => l.kind === 'group');
      const subvol = entry.levels.find((l) => l.kind === 'subvolume');
      if (group?.selected === groupName && subvol?.selected) used.add(subvol.selected);
    });
    return used;
  }

  private getChildNamesFromTree(parentPath: string): string[] {
    const normalized = MirroringPathUtils.normalizeMirroringPath(parentPath);
    return this.dirTree
      .filter((d) => MirroringPathUtils.normalizeMirroringPath(d.parent) === normalized)
      .map((d) => d.name)
      .filter(Boolean)
      .sort();
  }

  private isPathAvailable(path: string, pathIndex: number): boolean {
    const normalized = MirroringPathUtils.normalizeMirroringPath(path);
    return (
      !!normalized &&
      !MirroringPathUtils.isMirroringPathTracked(normalized, this.trackedPaths) &&
      !this.paths.some((entry, i) => {
        if (i === pathIndex) return false;
        const selected = MirroringPathUtils.normalizeMirroringPath(
          MirroringPathUtils.getMirrorPath(entry)
        );
        return selected && MirroringPathUtils.mirroringPathsOverlap(normalized, selected);
      })
    );
  }
}
