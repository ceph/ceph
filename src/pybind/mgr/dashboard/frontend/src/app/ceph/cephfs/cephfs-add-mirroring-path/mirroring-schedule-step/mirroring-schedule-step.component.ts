import {
  ChangeDetectorRef,
  Component,
  Input,
  OnChanges,
  OnInit,
  SimpleChanges,
  TemplateRef,
  ViewChild
} from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { of } from 'rxjs';
import { catchError, finalize } from 'rxjs/operators';

import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { RepeatFrequency } from '~/app/shared/enum/repeat-frequency.enum';
import { RetentionFrequency } from '~/app/shared/enum/retention-frequency.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';
import { SnapshotSchedule } from '~/app/shared/models/snapshot-schedule';
import { MirroringPathSelection } from '../mirroring-path.model';
import {
  EXISTING_SCHEDULE_ACTION,
  EXISTING_SCHEDULE_HANDLING,
  ExistingScheduleAction,
  ExistingScheduleHandling,
  MIRRORING_SCHEDULE_TIMEZONES,
  PathScheduleConflictRow,
  SCHEDULE_TYPE,
  START_AM_PM,
  StartAmPm
} from './mirroring-schedule-step.model';
import {
  buildPathScheduleRows,
  createConflictTableColumns,
  hasExistingSchedules,
  toGlobalConflictAction
} from './mirroring-schedule-step.utils';

@Component({
  selector: 'cd-mirroring-schedule-step',
  templateUrl: './mirroring-schedule-step.component.html',
  styleUrls: ['./mirroring-schedule-step.component.scss'],
  standalone: false
})
export class MirroringScheduleStepComponent implements OnInit, OnChanges, TearsheetStep {
  @Input() fsName: string;
  @Input() selectedPaths: MirroringPathSelection[] = [];

  @ViewChild('actionTpl', { static: true })
  actionTpl: TemplateRef<unknown>;

  readonly SCHEDULE_TYPE = SCHEDULE_TYPE;
  readonly START_AM_PM = START_AM_PM;
  readonly EXISTING_SCHEDULE_HANDLING = EXISTING_SCHEDULE_HANDLING;
  readonly EXISTING_SCHEDULE_ACTION = EXISTING_SCHEDULE_ACTION;
  readonly timezones = MIRRORING_SCHEDULE_TIMEZONES;

  formGroup!: CdFormGroup;
  startDatePickerValue: string[] = [];
  startTimeValue = '';
  startAmPmValue: StartAmPm = START_AM_PM.AM;
  conflictRows: PathScheduleConflictRow[] = [];

  repeatFrequencies = Object.entries(RepeatFrequency);
  retentionFrequencies = Object.entries(RetentionFrequency);

  constructor(
    private snapScheduleService: CephfsSnapshotScheduleService,
    private cdRef: ChangeDetectorRef
  ) {}

  get hasSelectedPaths(): boolean {
    return this.conflictRows.length > 0;
  }

  get hasScheduleConflicts(): boolean {
    return hasExistingSchedules(this.conflictRows);
  }

  get showActionColumn(): boolean {
    return (
      this.formGroup?.getValue('existingScheduleHandling') ===
      EXISTING_SCHEDULE_HANDLING.CONFIGURE_INDIVIDUALLY
    );
  }

  get conflictColumns(): CdTableColumn[] {
    return createConflictTableColumns(this.showActionColumn ? this.actionTpl : undefined);
  }

  ngOnInit(): void {
    this.formGroup = this.buildForm();
    this.refreshPathSchedules();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (!this.formGroup) {
      return;
    }
    if (changes['fsName'] || changes['selectedPaths']) {
      this.refreshPathSchedules();
    }
  }

  onStartDateChange(dates: string[]): void {
    this.startDatePickerValue = dates ?? [];
    this.formGroup.controls.startDate.setValue(dates?.length ? dates[0] : '');
  }

  onStartTimeChange(event?: string): void {
    if (typeof event === 'string') {
      if (Object.values(START_AM_PM).includes(event as StartAmPm)) {
        this.startAmPmValue = event as StartAmPm;
      } else {
        this.startTimeValue = event;
      }
    }
    this.formGroup.controls.startTime.setValue(this.startTimeValue);
    this.formGroup.controls.startAmPm.setValue(this.startAmPmValue);
  }

  loadScheduleConflicts(paths?: MirroringPathSelection[]): void {
    if (paths) {
      this.selectedPaths = paths;
    }
    this.refreshPathSchedules();
  }

  onConflictActionChange(row: PathScheduleConflictRow, action: ExistingScheduleAction): void {
    row.action = action;
  }

  private buildForm(): CdFormGroup {
    const formGroup = new CdFormGroup({
      scheduleName: new UntypedFormControl(''),
      scheduleType: new UntypedFormControl(SCHEDULE_TYPE.INTERVAL),
      repeatInterval: new UntypedFormControl(1, [Validators.min(1)]),
      repeatFrequency: new UntypedFormControl(RepeatFrequency.Daily),
      startDate: new UntypedFormControl(''),
      startTime: new UntypedFormControl(''),
      startAmPm: new UntypedFormControl(START_AM_PM.AM),
      startTimezone: new UntypedFormControl(''),
      retentionInterval: new UntypedFormControl(7, [Validators.min(1)]),
      retentionFrequency: new UntypedFormControl(RetentionFrequency.Daily),
      retentionCount: new UntypedFormControl(5, [Validators.min(1)]),
      existingScheduleHandling: new UntypedFormControl(EXISTING_SCHEDULE_HANDLING.KEEP_AND_ADD)
    });

    formGroup.get('existingScheduleHandling')?.valueChanges.subscribe((handling) => {
      this.applyGlobalConflictAction(handling);
    });

    return formGroup;
  }

  private refreshPathSchedules(): void {
    if (!this.fsName || !this.selectedPaths.length) {
      this.conflictRows = [];
      return;
    }

    this.snapScheduleService
      .getSnapshotScheduleList('/', this.fsName)
      .pipe(
        catchError(() => of([] as SnapshotSchedule[])),
        finalize(() => this.cdRef.markForCheck())
      )
      .subscribe((allSchedules) => {
        this.conflictRows = buildPathScheduleRows(
          this.selectedPaths,
          allSchedules,
          toGlobalConflictAction(this.formGroup?.getValue('existingScheduleHandling')),
          (schedule) => this.snapScheduleService.parseScheduleCopy(schedule)
        );
      });
  }

  private applyGlobalConflictAction(handling: ExistingScheduleHandling): void {
    if (handling === EXISTING_SCHEDULE_HANDLING.CONFIGURE_INDIVIDUALLY) {
      return;
    }

    const action = toGlobalConflictAction(handling);
    this.conflictRows.forEach((row) => {
      row.action = action;
    });
  }
}
