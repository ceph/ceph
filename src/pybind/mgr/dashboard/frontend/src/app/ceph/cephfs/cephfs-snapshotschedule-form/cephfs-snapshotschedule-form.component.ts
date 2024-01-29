import { ChangeDetectorRef, Component, OnInit } from '@angular/core';
import { AbstractControl, FormArray, FormControl, FormGroup, Validators } from '@angular/forms';
import { NgbActiveModal, NgbDateStruct, NgbTimeStruct } from '@ng-bootstrap/ng-bootstrap';
import { uniq } from 'lodash';
import { Observable, OperatorFunction, of, timer } from 'rxjs';
import { catchError, debounceTime, distinctUntilChanged, map, switchMap } from 'rxjs/operators';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { DirectoryStoreService } from '~/app/shared/api/directory-store.service';
import { ActionLabelsI18n, URLVerbs } from '~/app/shared/constants/app.constants';
import { Icons } from '~/app/shared/enum/icons.enum';
import { RepeatFrequency } from '~/app/shared/enum/repeat-frequency.enum';
import { RetentionFrequency } from '~/app/shared/enum/retention-frequency.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { RetentionPolicy, SnapshotScheduleFormValue } from '~/app/shared/models/snapshot-schedule';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';

const VALIDATON_TIMER = 300;
const DEBOUNCE_TIMER = 300;

@Component({
  selector: 'cd-cephfs-snapshotschedule-form',
  templateUrl: './cephfs-snapshotschedule-form.component.html',
  styleUrls: ['./cephfs-snapshotschedule-form.component.scss']
})
export class CephfsSnapshotscheduleFormComponent extends CdForm implements OnInit {
  fsName!: string;
  id!: number;
  isEdit = false;
  icons = Icons;
  repeatFrequencies = Object.entries(RepeatFrequency);
  retentionFrequencies = Object.entries(RetentionFrequency);

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
    public directoryStore: DirectoryStoreService
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
    this.loadingReady();
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

  createForm() {
    this.snapScheduleForm = new CdFormGroup(
      {
        directory: new FormControl(undefined, {
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
    this.retentionPolicies.removeAt(idx);
    this.cd.detectChanges();
  }

  parseDatetime(date: NgbDateStruct, time?: NgbTimeStruct): string {
    return `${date.year}-${date.month}-${date.day}T${time.hour || '00'}:${time.minute || '00'}:${
      time.second || '00'
    }`;
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
    if (this.snapScheduleForm.invalid) {
      this.snapScheduleForm.setErrors({ cdSubmitButton: true });
      return;
    }

    const values = this.snapScheduleForm.value as SnapshotScheduleFormValue;

    const snapScheduleObj = {
      fs: this.fsName,
      path: values.directory,
      snap_schedule: this.parseSchedule(values.repeatInterval, values.repeatFrequency),
      start: this.parseDatetime(values.startDate, values.startTime)
    };

    const retentionPoliciesValues = this.parseRetentionPolicies(values?.retentionPolicies);
    if (retentionPoliciesValues) {
      snapScheduleObj['retention_policy'] = retentionPoliciesValues;
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

  validateSchedule() {
    return (frm: AbstractControl) => {
      const directory = frm.get('directory');
      const repeatFrequency = frm.get('repeatFrequency');
      const repeatInterval = frm.get('repeatInterval');
      return timer(VALIDATON_TIMER).pipe(
        switchMap(() =>
          this.snapScheduleService
            .checkScheduleExists(
              directory?.value,
              this.fsName,
              repeatInterval?.value,
              repeatFrequency?.value
            )
            .pipe(
              map((exists: boolean) => {
                if (exists) {
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
            .checkRetentionPolicyExists(frm.get('directory').value, this.fsName, retentionList)
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
