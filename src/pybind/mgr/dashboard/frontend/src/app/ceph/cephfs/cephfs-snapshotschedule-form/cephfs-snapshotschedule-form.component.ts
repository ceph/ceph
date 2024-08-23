import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { AbstractControl, FormArray, FormControl, FormGroup, Validators } from '@angular/forms';
import { NgbActiveModal, NgbDateStruct, NgbTimeStruct } from '@ng-bootstrap/ng-bootstrap';
import { padStart, uniq } from 'lodash';
import { Observable, OperatorFunction, of, timer } from 'rxjs';
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
import { Icons } from '~/app/shared/enum/icons.enum';
import { RepeatFrequency } from '~/app/shared/enum/repeat-frequency.enum';
import { RetentionFrequency } from '~/app/shared/enum/retention-frequency.enum';
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

const VALIDATON_TIMER = 300;
const DEBOUNCE_TIMER = 300;
const DEFAULT_SUBVOLUME_GROUP = '_nogroup';

@Component({
  selector: 'cd-cephfs-snapshotschedule-form',
  templateUrl: './cephfs-snapshotschedule-form.component.html',
  styleUrls: ['./cephfs-snapshotschedule-form.component.scss']
})
export class CephfsSnapshotscheduleFormComponent extends CdForm implements OnInit {
  fsName!: string;
  path!: string;
  schedule!: string;
  retention!: string;
  start!: string;
  status!: string;
  subvol!: string;
  group!: string;
  id!: number;
  isEdit = false;
  icons = Icons;
  repeatFrequencies = Object.entries(RepeatFrequency);
  retentionFrequencies = Object.entries(RetentionFrequency);
  retentionPoliciesToRemove: RetentionPolicy[] = [];
  isDefaultSubvolumeGroup = false;
  subvolumeGroup!: string;
  subvolume!: string;
  isSubvolume = false;

  currentTime!: NgbTimeStruct;
  minDate!: NgbDateStruct;

  snapScheduleForm!: CdFormGroup;

  action!: string;
  resource!: string;

  columns!: CdTableColumn[];

  constructor(
    public activeModal: NgbActiveModal,
    private actionLabels: ActionLabelsI18n,
    private snapScheduleService: CephfsSnapshotScheduleService,
    private taskWrapper: TaskWrapperService,
    private cd: ChangeDetectorRef,
    public directoryStore: DirectoryStoreService,
    private subvolumeService: CephfsSubvolumeService
  ) {
    super();
    this.resource = $localize`Snapshot schedule`;

    const currentDatetime = new Date();
    this.minDate = {
      year: currentDatetime.getUTCFullYear(),
      month: currentDatetime.getUTCMonth() + 1,
      day: currentDatetime.getUTCDate()
    };
    this.currentTime = {
      hour: currentDatetime.getUTCHours(),
      minute: currentDatetime.getUTCMinutes(),
      second: currentDatetime.getUTCSeconds()
    };
  }

  ngOnInit(): void {
    this.action = this.actionLabels.CREATE;
    this.directoryStore.loadDirectories(this.id, '/', 3);
    this.createForm();
    this.isEdit ? this.populateForm() : this.loadingReady();

    this.snapScheduleForm
      .get('directory')
      .valueChanges.pipe(
        filter(() => !this.isEdit),
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
        const first = response.find((x) => x.path === this.path);
        this.snapScheduleForm.get('directory').disable();
        this.snapScheduleForm.get('directory').setValue(first.path);
        this.snapScheduleForm.get('startDate').disable();
        this.snapScheduleForm.get('startDate').setValue({
          year: new Date(first.start).getUTCFullYear(),
          month: new Date(first.start).getUTCMonth() + 1,
          day: new Date(first.start).getUTCDate()
        });
        this.snapScheduleForm.get('startTime').disable();
        this.snapScheduleForm.get('startTime').setValue({
          hour: new Date(first.start).getUTCHours(),
          minute: new Date(first.start).getUTCMinutes(),
          second: new Date(first.start).getUTCSeconds()
        });
        this.snapScheduleForm.get('repeatInterval').disable();
        this.snapScheduleForm.get('repeatInterval').setValue(first.schedule.split('')?.[0]);
        this.snapScheduleForm.get('repeatFrequency').disable();
        this.snapScheduleForm.get('repeatFrequency').setValue(first.schedule.split('')?.[1]);

        // retention policies
        first.retention &&
          Object.entries(first.retention).forEach(([frequency, interval], idx) => {
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
        startTime: new FormControl(this.currentTime, {
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
        asyncValidators: [this.validateSchedule(), this.validateRetention()]
      }
    );
  }

  addRetentionPolicy() {
    this.retentionPolicies.push(
      new FormGroup({
        retentionInterval: new FormControl(1),
        retentionFrequency: new FormControl(RetentionFrequency.Daily)
      })
    );
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

  submit() {
    this.validateSchedule()(this.snapScheduleForm).subscribe({
      next: () => {
        if (this.snapScheduleForm.invalid) {
          this.snapScheduleForm.setErrors({ cdSubmitButton: true });
          return;
        }

        const values = this.snapScheduleForm.value as SnapshotScheduleFormValue;

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
              error: () => {
                this.snapScheduleForm.setErrors({ cdSubmitButton: true });
              },
              complete: () => {
                this.activeModal.close();
              }
            });
        } else {
          const snapScheduleObj = {
            fs: this.fsName,
            path: values.directory,
            snap_schedule: this.parseSchedule(values?.repeatInterval, values?.repeatFrequency),
            start: this.parseDatetime(values?.startDate, values?.startTime)
          };

          const retentionPoliciesValues = this.parseRetentionPolicies(values?.retentionPolicies);

          if (retentionPoliciesValues) {
            snapScheduleObj['retention_policy'] = retentionPoliciesValues;
          }

          if (this.isSubvolume) {
            snapScheduleObj['subvol'] = this.subvolume;
          }

          if (this.isSubvolume && !this.isDefaultSubvolumeGroup) {
            snapScheduleObj['group'] = this.subvolumeGroup;
          }

          this.taskWrapper
            .wrapTaskAroundCall({
              task: new FinishedTask('cephfs/snapshot/schedule/' + URLVerbs.CREATE, {
                path: snapScheduleObj.path
              }),
              call: this.snapScheduleService.create(snapScheduleObj)
            })
            .subscribe({
              error: () => {
                this.snapScheduleForm.setErrors({ cdSubmitButton: true });
              },
              complete: () => {
                this.activeModal.close();
              }
            });
        }
      }
    });
  }

  validateSchedule() {
    return (frm: AbstractControl) => {
      const directory = frm.get('directory');
      const repeatFrequency = frm.get('repeatFrequency');
      const repeatInterval = frm.get('repeatInterval');

      if (this.isEdit) {
        return of(null);
      }

      return timer(VALIDATON_TIMER).pipe(
        switchMap(() =>
          this.snapScheduleService
            .checkScheduleExists(
              directory?.value,
              this.fsName,
              repeatInterval?.value,
              repeatFrequency?.value,
              this.isSubvolume
            )
            .pipe(
              map((exists: boolean) => {
                if (exists) {
                  repeatFrequency?.markAsDirty();
                  repeatFrequency?.setErrors({ notUnique: true }, { emitEvent: true });
                } else {
                  repeatFrequency?.setErrors(null);
                }
                return null;
              })
            )
        )
      );
    };
  }

  getFormArrayItem(frm: FormGroup, frmArrayName: string, ctrl: string, idx: number) {
    return (frm.get(frmArrayName) as FormArray)?.controls?.[idx]?.get?.(ctrl);
  }

  validateRetention() {
    return (frm: FormGroup) => {
      return timer(VALIDATON_TIMER).pipe(
        switchMap(() => {
          const retentionList = (frm.get('retentionPolicies') as FormArray).controls?.map(
            (ctrl) => {
              return ctrl.get('retentionFrequency').value;
            }
          );
          if (uniq(retentionList)?.length !== retentionList?.length) {
            this.getFormArrayItem(
              frm,
              'retentionPolicies',
              'retentionFrequency',
              retentionList.length - 1
            )?.setErrors?.({
              notUnique: true
            });
            return null;
          }
          return this.snapScheduleService
            .checkRetentionPolicyExists(
              frm.get('directory').value,
              this.fsName,
              retentionList,
              this.retentionPoliciesToRemove?.map?.((rp) => rp.retentionFrequency) || [],
              !!this.subvolume
            )
            .pipe(
              map(({ exists, errorIndex }) => {
                if (exists) {
                  this.getFormArrayItem(
                    frm,
                    'retentionPolicies',
                    'retentionFrequency',
                    errorIndex
                  )?.setErrors?.({ notUnique: true });
                } else {
                  (frm.get('retentionPolicies') as FormArray).controls?.forEach?.((_, i) => {
                    this.getFormArrayItem(
                      frm,
                      'retentionPolicies',
                      'retentionFrequency',
                      i
                    )?.setErrors?.(null);
                  });
                }
                return null;
              })
            );
        })
      );
    };
  }
}
