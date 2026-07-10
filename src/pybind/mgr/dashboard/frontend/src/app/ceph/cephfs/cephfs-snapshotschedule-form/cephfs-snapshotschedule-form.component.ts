import {
  ChangeDetectorRef,
  Component,
  DestroyRef,
  Inject,
  Input,
  OnChanges,
  OnInit,
  Optional,
  SimpleChanges,
  inject
} from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { AbstractControl, FormArray, FormControl, FormGroup, ValidationErrors, Validators } from '@angular/forms';
import { NgbDateStruct, NgbTimeStruct } from '@ng-bootstrap/ng-bootstrap';
import { padStart } from 'lodash';
import moment from 'moment';
import { Observable, OperatorFunction, Subject, of, timer } from 'rxjs';
import {
  catchError,
  debounceTime,
  distinctUntilChanged,
  filter,
  map,
  mergeMap,
  pluck,
  switchMap,
  tap
} from 'rxjs/operators';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { CephfsSubvolumeService } from '~/app/shared/api/cephfs-subvolume.service';
import { DirectoryStoreService } from '~/app/shared/api/directory-store.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { DEFAULT_SUBVOLUME_GROUP } from '~/app/shared/constants/cephfs.constant';
import { Icons } from '~/app/shared/enum/icons.enum';
import { RepeatFrequency } from '~/app/shared/enum/repeat-frequency.enum';
import { RetentionFrequency, RetentionFrequencyCopy } from '~/app/shared/enum/retention-frequency.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { FinishedTask } from '~/app/shared/models/finished-task';
import {
  RetentionPolicy,
  SnapshotSchedule,
  SnapshotScheduleFormValue
} from '~/app/shared/models/snapshot-schedule';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';

const VALIDATON_TIMER = 300;
const DEBOUNCE_TIMER = 300;

@Component({
  selector: 'cd-cephfs-snapshotschedule-form',
  templateUrl: './cephfs-snapshotschedule-form.component.html',
  styleUrls: ['./cephfs-snapshotschedule-form.component.scss'],
  standalone: false
})
export class CephfsSnapshotscheduleFormComponent
  extends CdForm
  implements OnInit, OnChanges, TearsheetStep {
  @Input() embedded = false;
  @Input() hideDirectory = false;

  @Input()
  set fsIdInput(value: number) {
    if (value) {
      this.id = value;
    }
  }

  @Input()
  set fsNameInput(value: string) {
    if (value) {
      this.fsName = value;
    }
  }

  @Input()
  set schedulePath(value: string) {
    if (value) {
      this.path = value;
      if (this.snapScheduleForm && this.hideDirectory) {
        this.applyDirectoryPath(value);
      }
    }
  }

  subvol!: string;
  group!: string;
  icons = Icons;
  repeatFrequencies = Object.entries(RepeatFrequency);
  retentionFrequencies = Object.entries(RetentionFrequency);
  retentionPoliciesToRemove: RetentionPolicy[] = [];
  isDefaultSubvolumeGroup = false;
  subvolumeGroup!: string;
  subvolume!: string;
  isSubvolume = false;

  minDate!: string;

  snapScheduleForm!: CdFormGroup;

  private readonly destroyRef = inject(DestroyRef);
  private readonly scheduleUniquenessCheck$ = new Subject<void>();
  private readonly retentionUniquenessCheck$ = new Subject<void>();

  get formGroup(): CdFormGroup {
    return this.snapScheduleForm;
  }

  action!: string;
  resource!: string;

  columns!: CdTableColumn[];

  constructor(
    private actionLabels: ActionLabelsI18n,
    private snapScheduleService: CephfsSnapshotScheduleService,
    private taskWrapper: TaskWrapperService,
    private cd: ChangeDetectorRef,
    public directoryStore: DirectoryStoreService,
    private subvolumeService: CephfsSubvolumeService,

    @Optional() @Inject('fsName') public fsName: string,
    @Optional() @Inject('id') public id: number,
    @Optional() @Inject('path') public path: string,
    @Optional() @Inject('schedule') public schedule: string,
    @Optional() @Inject('retention') public retention: string,
    @Optional() @Inject('start') public start: string,
    @Optional() @Inject('status') public status: string,
    @Optional() @Inject('isEdit') public isEdit = false
  ) {
    super();
    this.resource = $localize`Snapshot schedule`;

    const currentDatetime = new Date();
    this.minDate = `${currentDatetime.getUTCFullYear()}-${
      currentDatetime.getUTCMonth() + 1
    }-${currentDatetime.getUTCDate()}`;
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.schedulePath && this.snapScheduleForm && this.hideDirectory && this.path) {
      this.applyDirectoryPath(this.path);
    }
  }

  ngOnInit(): void {
    this.action = this.actionLabels.CREATE;
    if (this.id) {
      this.directoryStore.loadDirectories(this.id, '/', 3);
    }
    this.createForm();
    if (this.hideDirectory && this.path) {
      this.applyDirectoryPath(this.path);
    }
    this.setupScheduleUniquenessValidation();
    this.setupRetentionUniquenessValidation();
    this.isEdit ? this.populateForm() : this.loadingReady();

    this.snapScheduleForm
      .get('directory')
      .valueChanges.pipe(
        filter(() => !this.isEdit && !this.hideDirectory),
        debounceTime(DEBOUNCE_TIMER),
        tap(() => {
          this.isSubvolume = false;
        }),
        tap((value: string) => {
          this.subvolumeGroup = value?.split?.('/')?.[2];
          this.subvolume = value?.split?.('/')?.[3];
        }),
        filter(() => !!this.subvolume && !!this.subvolumeGroup),
        tap(() => {
          this.isSubvolume = !!this.subvolume && !!this.subvolumeGroup;
          this.snapScheduleForm.get('repeatFrequency').setErrors(null);
        }),
        mergeMap(() =>
          this.subvolumeService
            .exists(
              this.subvolume,
              this.fsName,
              this.subvolumeGroup === DEFAULT_SUBVOLUME_GROUP ? '' : this.subvolumeGroup
            )
            .pipe(
              tap((exists: boolean) => (this.isSubvolume = exists)),
              tap(
                (exists: boolean) =>
                  (this.isDefaultSubvolumeGroup =
                    exists && this.subvolumeGroup === DEFAULT_SUBVOLUME_GROUP)
              )
            )
        ),
        filter((exists: boolean) => exists),
        mergeMap(() =>
          this.subvolumeService
            .info(
              this.fsName,
              this.subvolume,
              this.subvolumeGroup === DEFAULT_SUBVOLUME_GROUP ? '' : this.subvolumeGroup
            )
            .pipe(pluck('path'))
        ),
        filter((path: string) => path !== this.snapScheduleForm.get('directory').value)
      )
      .subscribe({
        next: (path: string) => this.snapScheduleForm.get('directory').setValue(path)
      });
  }

  get retentionPolicies() {
    return this.snapScheduleForm.get('retentionPolicies') as FormArray;
  }

  search: OperatorFunction<string, readonly string[]> = (input: Observable<string>) =>
    input.pipe(
      debounceTime(DEBOUNCE_TIMER),
      distinctUntilChanged(),
      switchMap((term) =>
        this.directoryStore.search(term, this.id).pipe(
          catchError(() => {
            return of([]);
          })
        )
      )
    );

  populateForm() {
    this.action = this.actionLabels.EDIT;
    this.snapScheduleService.getSnapshotSchedule(this.path, this.fsName, false).subscribe({
      next: (response: SnapshotSchedule[]) => {
        const schedule = response.find((x) => x.path === this.path);
        const offset = moment().utcOffset();
        const startDate = moment
          .parseZone(schedule.start)
          .utc()
          .utcOffset(offset)
          .local()
          .format('YYYY-MM-DD HH:mm:ss');
        this.snapScheduleForm.get('directory').disable();
        this.snapScheduleForm.get('directory').setValue(schedule.path);
        this.snapScheduleForm.get('startDate').disable();
        this.snapScheduleForm.get('startDate').setValue(startDate);
        this.snapScheduleForm.get('repeatInterval').disable();
        this.snapScheduleForm.get('repeatInterval').setValue(schedule.schedule.split('')?.[0]);
        this.snapScheduleForm.get('repeatFrequency').disable();
        this.snapScheduleForm.get('repeatFrequency').setValue(schedule.schedule.split('')?.[1]);

        // retention policies
        schedule.retention &&
          Object.entries(schedule.retention).forEach(([frequency, interval], idx) => {
            const freqKey = Object.keys(RetentionFrequency)[
              Object.values(RetentionFrequency).indexOf(frequency as any)
            ];
            this.retentionPolicies.push(
              new FormGroup({
                retentionInterval: new FormControl(interval),
                retentionFrequency: new FormControl(RetentionFrequency[freqKey])
              })
            );
            this.retentionPolicies.controls[idx].get('retentionInterval').disable();
            this.retentionPolicies.controls[idx].get('retentionFrequency').disable();
          });
        this.loadingReady();
      }
    });
  }

  createForm() {
    this.snapScheduleForm = new CdFormGroup(
      {
        directory: new FormControl(undefined, {
          updateOn: 'blur',
          validators: [Validators.required]
        }),
        startDate: new FormControl(this.minDate, {
          validators: [Validators.required]
        }),
        repeatInterval: new FormControl(1, {
          validators: [Validators.required, Validators.min(1)]
        }),
        repeatFrequency: new FormControl(RepeatFrequency.Daily, {
          validators: [Validators.required]
        }),
        retentionPolicies: new FormArray([])
      },
      {
        asyncValidators: [this.validateRetention()]
      }
    );
  }

  onScheduleIntervalChange(value: number | string): void {
    const control = this.snapScheduleForm.get('repeatInterval');
    const interval = Number(value);
    if (!control || !Number.isFinite(interval)) {
      return;
    }
    if (control.value !== interval) {
      control.setValue(interval, { emitEvent: false });
    }
    control.markAsDirty();
    control.markAsTouched();
    this.queueScheduleUniquenessCheck();
  }

  onScheduleFrequencyChange(value: RepeatFrequency): void {
    const control = this.snapScheduleForm.get('repeatFrequency');
    if (!control || !value) {
      return;
    }
    if (control.value !== value) {
      control.setValue(value, { emitEvent: false });
    }
    control.markAsDirty();
    control.markAsTouched();
    this.queueScheduleUniquenessCheck();
  }

  onRetentionIntervalChange(index: number, value: number | string): void {
    const control = this.retentionPolicies.at(index)?.get('retentionInterval');
    const interval = Number(value);
    if (!control || !Number.isFinite(interval)) {
      return;
    }
    if (control.value !== interval) {
      control.setValue(interval, { emitEvent: false });
    }
    control.markAsDirty();
    control.markAsTouched();
    this.queueRetentionUniquenessCheck();
  }

  onRetentionFrequencyChange(index: number, value: string): void {
    const control = this.retentionPolicies.at(index)?.get('retentionFrequency');
    if (!control || !value) {
      return;
    }
    if (control.value !== value) {
      control.setValue(value, { emitEvent: false });
    }
    control.markAsDirty();
    control.markAsTouched();
    this.queueRetentionUniquenessCheck();
  }

  hasRetentionUniquenessError(index: number): boolean {
    const policy = this.retentionPolicies.at(index);
    if (!policy) {
      return false;
    }
    const frequency = policy.get('retentionFrequency');
    const interval = policy.get('retentionInterval');
    const hasError = policy.hasError('notUnique') || frequency?.hasError('notUnique');
    if (!hasError) {
      return false;
    }
    return (
      policy.dirty ||
      policy.touched ||
      frequency?.dirty ||
      frequency?.touched ||
      interval?.dirty ||
      interval?.touched
    );
  }

  getRetentionConflictMessage(index: number): string {
    const frequencyControl = this.retentionPolicies.at(index)?.get('retentionFrequency');
    const existingValue = frequencyControl?.getError('notUnique')?.existingValue;
    const frequency = frequencyControl?.value;
    const frequencyLabel = frequency
      ? (RetentionFrequencyCopy[frequency] || frequency).toLowerCase()
      : $localize`retention`;

    if (existingValue) {
      return $localize`A ${frequencyLabel} retention policy (${existingValue}) already exists for this path. Remove it first or choose a different frequency.`;
    }

    return $localize`This retention policy already exists for the selected directory.`;
  }

  validateRetentionForPaths(paths: string[]): Observable<boolean> {
    const retentionList = this.retentionPolicies.controls?.map(
      (ctrl) => ctrl.get('retentionFrequency').value
    );

    if (!retentionList?.length) {
      return of(true);
    }

    const duplicateIndex = this.findDuplicateRetentionFrequencyIndex(retentionList);
    if (duplicateIndex >= 0) {
      this.applyRetentionUniquenessResult(this.snapScheduleForm, true, duplicateIndex);
      return of(false);
    }

    const uniquePaths = [...new Set(paths.filter(Boolean))];
    if (!uniquePaths.length) {
      return of(true);
    }

    return this.snapScheduleService
      .checkRetentionPolicyExistsForPaths(
        uniquePaths,
        this.fsName,
        retentionList,
        this.retentionPoliciesToRemove?.map?.((rp) => rp.retentionFrequency) || [],
        uniquePaths.some((path) => this.resolveIsSubvolumeForPath(path))
      )
      .pipe(
        map(({ exists, errorIndex, existingValue }) => {
          this.applyRetentionUniquenessResult(
            this.snapScheduleForm,
            exists,
            errorIndex,
            existingValue
          );
          return !exists;
        }),
        catchError(() => of(true))
      );
  }

  queueScheduleUniquenessCheck(): void {
    this.scheduleUniquenessCheck$.next();
  }

  queueRetentionUniquenessCheck(): void {
    this.retentionUniquenessCheck$.next();
  }

  addRetentionPolicy() {
    this.retentionPolicies.push(
      new FormGroup({
        retentionInterval: new FormControl(1),
        retentionFrequency: new FormControl(RetentionFrequency.Daily)
      })
    );
    this.queueRetentionUniquenessCheck();
    this.cd.detectChanges();
  }

  removeRetentionPolicy(idx: number) {
    if (this.isEdit && this.retentionPolicies.at(idx).disabled) {
      const values = this.retentionPolicies.at(idx).value as RetentionPolicy;
      this.retentionPoliciesToRemove.push(values);
    }
    this.retentionPolicies.removeAt(idx);
    this.retentionPolicies.controls.forEach((x) =>
      x.get('retentionFrequency').updateValueAndValidity()
    );
    this.queueRetentionUniquenessCheck();
    this.cd.detectChanges();
  }

  convertNumberToString(input: number, length = 2, format = '0'): string {
    return padStart(input.toString(), length, format);
  }

  parseDatetime(date: NgbDateStruct, time?: NgbTimeStruct): string {
    if (!date || !time) return null;
    return `${date.year}-${this.convertNumberToString(date.month)}-${this.convertNumberToString(
      date.day
    )}T${this.convertNumberToString(time.hour)}:${this.convertNumberToString(
      time.minute
    )}:${this.convertNumberToString(time.second)}`;
  }
  parseSchedule(interval: number, frequency: string): string {
    return `${interval}${frequency}`;
  }

  parseRetentionPolicies(retentionPolicies: RetentionPolicy[]) {
    return retentionPolicies
      ?.filter((r) => r?.retentionInterval !== null && r?.retentionFrequency !== null)
      ?.map?.((r) => `${r.retentionInterval}-${r.retentionFrequency}`)
      .join('|');
  }

  getScheduleSummary(): string {
    if (!this.snapScheduleForm) {
      return '';
    }

    const values = this.snapScheduleForm.getRawValue() as SnapshotScheduleFormValue;
    const schedule = this.parseSchedule(values?.repeatInterval, values?.repeatFrequency);
    const retention = this.parseRetentionPolicies(values?.retentionPolicies);
    const parts = [$localize`Every ${schedule}`];

    if (retention) {
      parts.push($localize`Retention: ${retention}`);
    }

    return parts.join(' · ');
  }

  buildCreatePayload(targetPath?: string): Record<string, unknown> {
    const values = this.snapScheduleForm.getRawValue() as SnapshotScheduleFormValue;
    const path = targetPath ?? values.directory;
    const subvolInfo = this.parseSubvolumePath(path);
    const useSubvolume = this.hideDirectory ? subvolInfo.isSubvolumePath : this.isSubvolume;
    const subvolume = subvolInfo.isSubvolumePath ? subvolInfo.subvolume : this.subvolume;
    const subvolumeGroup = subvolInfo.isSubvolumePath
      ? subvolInfo.subvolumeGroup
      : this.subvolumeGroup;
    const isDefaultSubvolumeGroup =
      subvolInfo.isSubvolumePath && subvolumeGroup === DEFAULT_SUBVOLUME_GROUP
        ? true
        : this.isDefaultSubvolumeGroup;

    const snapScheduleObj: Record<string, unknown> = {
      fs: this.fsName,
      path,
      snap_schedule: this.parseSchedule(values?.repeatInterval, values?.repeatFrequency),
      start: new Date(values?.startDate.replace(/\//g, '-').replace(' ', 'T'))
        .toISOString()
        .slice(0, 19)
    };

    const retentionPoliciesValues = this.parseRetentionPolicies(values?.retentionPolicies);
    if (retentionPoliciesValues) {
      snapScheduleObj['retention_policy'] = retentionPoliciesValues;
    }

    if (useSubvolume && subvolume) {
      snapScheduleObj['subvol'] = subvolume;
    }

    if (useSubvolume && subvolume && !isDefaultSubvolumeGroup && subvolumeGroup) {
      snapScheduleObj['group'] = subvolumeGroup;
    }

    return snapScheduleObj;
  }

  private parseSubvolumePath(path: string): {
    subvolumeGroup: string;
    subvolume: string;
    isSubvolumePath: boolean;
  } {
    const parts = (path ?? '')
      .trim()
      .replace(/\/+$/, '')
      .split('/')
      .filter(Boolean);

    if (parts[0] !== 'volumes' || parts.length < 3) {
      return { subvolumeGroup: '', subvolume: '', isSubvolumePath: false };
    }

    return {
      subvolumeGroup: parts[1],
      subvolume: parts[2],
      isSubvolumePath: true
    };
  }

  submit() {
    this.evaluateScheduleUniqueness()
      .pipe(switchMap(() => this.evaluateRetentionUniqueness()))
      .subscribe({
      next: () => {
        if (this.snapScheduleForm.invalid) {
          this.snapScheduleForm.setErrors({ cdSubmitButton: true });
          return;
        }

        if (this.isEdit) {
          const retentionPoliciesToAdd = (this.snapScheduleForm.get(
            'retentionPolicies'
          ) as FormArray).controls
            ?.filter(
              (ctrl) =>
                !ctrl.get('retentionInterval').disabled && !ctrl.get('retentionFrequency').disabled
            )
            .map((ctrl) => ({
              retentionInterval: ctrl.get('retentionInterval').value,
              retentionFrequency: ctrl.get('retentionFrequency').value
            }));

          const updateObj = {
            fs: this.fsName,
            path: this.path,
            subvol: this.subvol,
            group: this.group,
            retention_to_add: this.parseRetentionPolicies(retentionPoliciesToAdd) || null,
            retention_to_remove: this.parseRetentionPolicies(this.retentionPoliciesToRemove) || null
          };

          this.taskWrapper
            .wrapTaskAroundCall({
              task: new FinishedTask('cephfs/snapshot/schedule/' + URLVerbs.EDIT, {
                path: this.path
              }),
              call: this.snapScheduleService.update(updateObj)
            })
            .subscribe({
              error: (resp) => {
                this.applyRetentionConflictError(resp);
                this.snapScheduleForm.setErrors({ cdSubmitButton: true });
              },
              complete: () => {
                this.closeModal();
              }
            });
        } else {
          const snapScheduleObj = this.buildCreatePayload();
          this.taskWrapper
            .wrapTaskAroundCall({
              task: new FinishedTask('cephfs/snapshot/schedule/' + URLVerbs.CREATE, {
                path: snapScheduleObj.path
              }),
              call: this.snapScheduleService.create(snapScheduleObj)
            })
            .subscribe({
              error: (resp) => {
                this.applyRetentionConflictError(resp);
                this.snapScheduleForm.setErrors({ cdSubmitButton: true });
              },
              complete: () => {
                this.closeModal();
              }
            });
        }
      }
    });
  }

  private setupScheduleUniquenessValidation(): void {
    this.scheduleUniquenessCheck$
      .pipe(
        debounceTime(DEBOUNCE_TIMER),
        switchMap(() => this.evaluateScheduleUniqueness()),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe();

    ['repeatInterval', 'repeatFrequency'].forEach((fieldName) => {
      this.snapScheduleForm
        .get(fieldName)
        ?.valueChanges.pipe(
          distinctUntilChanged(),
          takeUntilDestroyed(this.destroyRef)
        )
        .subscribe(() => this.queueScheduleUniquenessCheck());
    });
  }

  private setupRetentionUniquenessValidation(): void {
    this.retentionUniquenessCheck$
      .pipe(
        debounceTime(DEBOUNCE_TIMER),
        switchMap(() => this.evaluateRetentionUniqueness()),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe();

    this.snapScheduleForm
      .get('directory')
      ?.valueChanges.pipe(
        debounceTime(DEBOUNCE_TIMER),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe(() => this.queueRetentionUniquenessCheck());

    this.retentionPolicies.valueChanges
      .pipe(
        debounceTime(DEBOUNCE_TIMER),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe(() => this.queueRetentionUniquenessCheck());
  }

  private evaluateRetentionUniqueness(): Observable<ValidationErrors | null> {
    const frm = this.snapScheduleForm;
    const directoryPath = this.getScheduleDirectoryPath(frm);
    const retentionPolicies = (frm.get('retentionPolicies') as FormArray).controls;
    const retentionList = retentionPolicies?.map((ctrl) => ctrl.get('retentionFrequency').value);

    if (!retentionList?.length) {
      this.clearRetentionUniquenessErrors(frm);
      this.cd.markForCheck();
      return of(null);
    }

    const duplicateIndex = this.findDuplicateRetentionFrequencyIndex(retentionList);
    if (duplicateIndex >= 0) {
      this.applyRetentionUniquenessResult(frm, true, duplicateIndex);
      return of({ retentionConflict: true });
    }

    if (!directoryPath) {
      this.clearRetentionUniquenessErrors(frm);
      this.cd.markForCheck();
      return of(null);
    }

    return this.snapScheduleService
      .checkRetentionPolicyExists(
        directoryPath,
        this.fsName,
        retentionList,
        this.retentionPoliciesToRemove?.map?.((rp) => rp.retentionFrequency) || [],
        this.resolveIsSubvolumeForPath(directoryPath)
      )
      .pipe(
        map(({ exists, errorIndex, existingValue }) => {
          this.applyRetentionUniquenessResult(frm, exists, errorIndex, existingValue);
          return exists && errorIndex >= 0 ? { retentionConflict: true } : null;
        }),
        catchError(() => of(null))
      );
  }

  private applyRetentionUniquenessResult(
    frm: CdFormGroup,
    exists: boolean,
    errorIndex: number,
    existingValue?: number
  ): void {
    if (exists && errorIndex >= 0) {
      const frequencyControl = this.getFormArrayItem(
        frm,
        'retentionPolicies',
        'retentionFrequency',
        errorIndex
      );
      const intervalControl = this.getFormArrayItem(
        frm,
        'retentionPolicies',
        'retentionInterval',
        errorIndex
      );
      const retentionPolicyControl = (frm.get('retentionPolicies') as FormArray).at(errorIndex);
      this.setControlError(frequencyControl, 'notUnique', true, { existingValue });
      frequencyControl?.markAsDirty();
      frequencyControl?.markAsTouched();
      intervalControl?.markAsDirty();
      intervalControl?.markAsTouched();
      this.setControlError(retentionPolicyControl, 'notUnique', true, { existingValue });
      retentionPolicyControl?.markAsDirty();
      retentionPolicyControl?.markAsTouched();
      this.setControlError(frm.get('retentionPolicies'), 'notUnique', true);
    } else {
      this.clearRetentionUniquenessErrors(frm);
    }

    frm.updateValueAndValidity({ emitEvent: false });
    this.cd.markForCheck();
  }

  private applyRetentionConflictError(resp: { error?: { detail?: string } }): void {
    this.applyRetentionConflictFromDetail(resp?.error?.detail || '');
  }

  applyRetentionConflictFromDetail(detail: string): void {
    const conflict = this.snapScheduleService.parseRetentionConflict(detail);
    if (!conflict) {
      return;
    }

    this.retentionPolicies.controls.forEach((ctrl, idx) => {
      if (ctrl.get('retentionFrequency')?.value === conflict.frequency) {
        this.applyRetentionUniquenessResult(
          this.snapScheduleForm,
          true,
          idx,
          conflict.existingValue
        );
      }
    });
  }

  private evaluateScheduleUniqueness(): Observable<void> {
    const frm = this.snapScheduleForm;

    if (this.isEdit) {
      return of(undefined);
    }

    const directoryPath = this.getScheduleDirectoryPath(frm);
    const interval = Number(frm.get('repeatInterval')?.value);
    const frequency = frm.get('repeatFrequency')?.value as RepeatFrequency;

    if (!directoryPath || !interval || !frequency) {
      this.clearScheduleUniquenessErrors(frm);
      this.cd.markForCheck();
      return of(undefined);
    }

    return this.snapScheduleService
      .checkScheduleExists(
        directoryPath,
        this.fsName,
        interval,
        frequency,
        this.resolveIsSubvolumeForPath(directoryPath)
      )
      .pipe(
        tap((exists) => this.applyScheduleUniquenessResult(frm, exists)),
        map(() => undefined),
        catchError(() => {
          this.clearScheduleUniquenessErrors(frm);
          this.cd.markForCheck();
          return of(undefined);
        })
      );
  }

  private applyScheduleUniquenessResult(frm: CdFormGroup, exists: boolean): void {
    const repeatFrequency = frm.get('repeatFrequency');
    const repeatInterval = frm.get('repeatInterval');

    if (exists) {
      repeatFrequency?.markAsDirty();
      repeatInterval?.markAsDirty();
      this.setControlError(repeatFrequency, 'notUnique', true);
      this.setControlError(repeatInterval, 'notUnique', true);
    } else {
      this.clearScheduleUniquenessErrors(frm);
    }

    frm.updateValueAndValidity({ emitEvent: false });
    this.cd.markForCheck();
  }

  private getScheduleDirectoryPath(frm: CdFormGroup): string {
    const directory = frm.get('directory');
    return this.hideDirectory
      ? directory?.getRawValue?.() ?? directory?.value
      : directory?.value;
  }

  private resolveIsSubvolumeForPath(path: string): boolean {
    const subvolInfo = this.parseSubvolumePath(path);
    return this.hideDirectory ? subvolInfo.isSubvolumePath : this.isSubvolume;
  }

  private setControlError(
    control: AbstractControl | null,
    errorKey: string,
    present: boolean,
    errorValue: Record<string, unknown> = { value: true }
  ): void {
    if (!control) {
      return;
    }
    const errors = { ...(control.errors ?? {}) };
    if (present) {
      errors[errorKey] = errorValue;
      control.setErrors(errors);
      return;
    }
    delete errors[errorKey];
    control.setErrors(Object.keys(errors).length ? errors : null);
  }

  private clearScheduleUniquenessErrors(frm: AbstractControl): void {
    this.setControlError(frm.get('repeatFrequency'), 'notUnique', false);
    this.setControlError(frm.get('repeatInterval'), 'notUnique', false);
  }

  private clearRetentionUniquenessErrors(frm: CdFormGroup): void {
    (frm.get('retentionPolicies') as FormArray).controls?.forEach?.((ctrl, i) => {
      const frequencyControl = this.getFormArrayItem(
        frm,
        'retentionPolicies',
        'retentionFrequency',
        i
      );
      const intervalControl = this.getFormArrayItem(frm, 'retentionPolicies', 'retentionInterval', i);
      this.setControlError(frequencyControl, 'notUnique', false);
      this.setControlError(intervalControl, 'notUnique', false);
      this.setControlError(ctrl, 'notUnique', false);
    });
    this.setControlError(frm.get('retentionPolicies'), 'notUnique', false);
  }

  private findDuplicateRetentionFrequencyIndex(frequencies: string[]): number {
    const seen = new Set<string>();
    for (let i = 0; i < frequencies.length; i++) {
      const frequency = frequencies[i];
      if (!frequency) {
        continue;
      }
      if (seen.has(frequency)) {
        return i;
      }
      seen.add(frequency);
    }
    return -1;
  }

  getFormArrayItem(frm: FormGroup, frmArrayName: string, ctrl: string, idx: number) {
    return (frm.get(frmArrayName) as FormArray)?.controls?.[idx]?.get?.(ctrl);
  }

  private applyDirectoryPath(path: string): void {
    const directoryControl = this.snapScheduleForm.get('directory');
    directoryControl.setValue(path);
    directoryControl.disable();
    const subvolInfo = this.parseSubvolumePath(path);
    this.subvolumeGroup = subvolInfo.subvolumeGroup || path?.split?.('/')?.[2];
    this.subvolume = subvolInfo.subvolume || path?.split?.('/')?.[3];

    if (this.hideDirectory && subvolInfo.isSubvolumePath) {
      this.isSubvolume = true;
      this.isDefaultSubvolumeGroup = subvolInfo.subvolumeGroup === DEFAULT_SUBVOLUME_GROUP;
      this.queueScheduleUniquenessCheck();
      this.queueRetentionUniquenessCheck();
      return;
    }

    if (!this.subvolume || !this.subvolumeGroup || !this.fsName) {
      return;
    }

    this.subvolumeService
      .exists(
        this.subvolume,
        this.fsName,
        this.subvolumeGroup === DEFAULT_SUBVOLUME_GROUP ? '' : this.subvolumeGroup
      )
      .subscribe((exists: boolean) => {
        this.isSubvolume = exists;
        this.isDefaultSubvolumeGroup = exists && this.subvolumeGroup === DEFAULT_SUBVOLUME_GROUP;
        this.queueScheduleUniquenessCheck();
      });
  }

  validateRetention() {
    return () => {
      return timer(VALIDATON_TIMER).pipe(switchMap(() => this.evaluateRetentionUniqueness()));
    };
  }
}
