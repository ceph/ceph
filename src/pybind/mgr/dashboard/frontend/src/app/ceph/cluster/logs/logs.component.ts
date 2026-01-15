import { DatePipe } from '@angular/common';
import { AfterViewInit, ChangeDetectorRef, Component, Input, NgZone, OnDestroy, OnInit, ViewEncapsulation } from '@angular/core';

import moment from 'moment';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
 
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { LogsService } from '~/app/shared/api/logs.service';
import { Icons } from '~/app/shared/enum/icons.enum';
import { CdForm } from '~/app/shared/forms/cd-form';
import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { Prioity } from '~/app/shared/models/logs.interface';
 
@Component({
  selector: 'cd-logs',
  templateUrl: './logs.component.html',
  styleUrls: ['./logs.component.scss'],
  standalone: false,
  encapsulation:ViewEncapsulation.None
})
export class LogsComponent extends CdForm implements OnInit, OnDestroy, AfterViewInit {
  @Input()
  showClusterLogs = true;
  @Input()
  showAuditLogs = true;
  @Input()
  showDaemonLogs = true;
  @Input()
  showNavLinks = true;
  @Input()
  showFilterTools = true;
  @Input()
  showDownloadCopyButton = true;
  @Input()
  defaultTab = '';
  @Input()
  scrollable = false;

  contentData: any;
  clog: Array<any>;
  audit_log: Array<any>;
  icons = Icons;
  clogText: string;
  auditLogText: string;
  lokiServiceStatus$: Observable<boolean>;
  alloyServiceStatus$: Observable<boolean>;
  codeSnippetExpanded = false;
 
  interval: number;
  priorities: Array<{ name: string; value: string }> = [
    { name: Prioity.Debug, value: '[DBG]' },
    { name: Prioity.Info, value: '[INF]' },
    { name: Prioity.Warning, value: '[WRN]' },
    { name: Prioity.Error, value: '[ERR]' },
    { name: Prioity.All, value: 'All' }
  ];
  // combobox items derived from priorities (Carbon expects `content` property)
  priorityItems = this.priorities.map((p) => ({ content: p.name, value: p.value }));
 
  priority = Prioity.All;
  search = '';
  selectedDate: string[] | string = [];
  startTimeStr = '12:00';
  startAmPm = 'AM';
  endTimeStr = '11:59';
  endAmPm = 'PM';

  filterLogsForm: CdFormGroup;
 
  constructor(
    private logsService: LogsService,
    private cephService: CephServiceService,
    private datePipe: DatePipe,
    private ngZone: NgZone,
    private formBuilder: CdFormBuilder,
    private cdr: ChangeDetectorRef
  ) {
    super();
  }
 
  ngOnInit() {
    this.initializeForm();
    this.getInfo();
    this.ngZone.runOutsideAngular(() => {
      this.getDaemonDetails();
      this.interval = window.setInterval(() => {
        this.ngZone.run(() => {
          this.getInfo();
        });
      }, 5000);
    });
  }

  ngAfterViewInit() {
    // Set expanded state after view initialization to avoid ExpressionChangedAfterItHasBeenCheckedError
    setTimeout(() => {
      this.codeSnippetExpanded = true;
      this.cdr.detectChanges();
    }, 1);
  }

  initializeForm() {
    this.filterLogsForm = this.formBuilder.group({
      priority: [Prioity.All],
      search: [''],
      selectedDate:[this.selectedDate],
      startTime:[this.startTimeStr],
      startAmPm:[this.startAmPm],
      endTime:[this.endTimeStr],
      endAmPm:[this.endAmPm]
    });

    this.filterLogsForm.valueChanges.subscribe(() => {
      this.filterLogs();
    });
  }
 
  onDateChange(event: string[]) {
    const dateFinal  = event;
    this.selectedDate = moment(dateFinal[0]).format('YYYY-MM-DD');
    this.filterLogs();
  }
 
  onPrioritySelection(event: any) {
    this.priority = event.value;
    this.filterLogs();
  }

  onKeywordChange(event: string) {
    this.search = event;
    this.filterLogs();
  }
 
  onPriorityClear() {
    this.priority = Prioity.All;
    this.filterLogs();
  }
 
  onStartTimeChange(event: string) {
    this.startTimeStr = event;
    this.filterLogs();
  }
 
  onStartAmPmChange(event: string) {
    this.startAmPm = event;
    this.filterLogs();
  }
 
  onEndTimeChange(event: string) {
    this.endTimeStr = event;
    this.filterLogs();
  }
 
  onEndAmPmChange(event: string) {
    this.endAmPm = event;
    this.filterLogs();
  }
 
  private convertTo24Hour(timeStr: string, ampm: string): { hour: number; minute: number } {
    const [hourStr, minuteStr] = timeStr.split(':');
    let hour = parseInt(hourStr, 10);
    const minute = parseInt(minuteStr, 10) || 0;
 
    if (ampm === 'AM') {
      if (hour === 12) hour = 0;
    } else {
      if (hour !== 12) hour += 12;
    }
 
    return { hour, minute };
  }
 
  ngOnDestroy() {
    clearInterval(this.interval);
  }
 
  getDaemonDetails() {
    this.lokiServiceStatus$ = this.cephService.getDaemons('loki').pipe(
      map((data: any) => {
        return data.length > 0 && data[0].status === 1;
      })
    );
    this.alloyServiceStatus$ = this.cephService.getDaemons('alloy').pipe(
      map((data: any) => {
        return data.length > 0 && data[0].status === 1;
      })
    );
  }
 
  getInfo() {
    this.logsService.getLogs().subscribe((data: any) => {
      this.contentData = data;
      this.clogText = this.logToText(this.contentData.clog);
      this.auditLogText = this.logToText(this.contentData.audit_log);
      this.filterLogs();
    });
  }
 
  abstractFilters(): any {
    const priority = this.priority;
    const key = this.search.toLowerCase();
 
    let startMoment = null;
    let endMoment = null;
 
    if (this.selectedDate && this.selectedDate.length > 0) {
      const dateStr = typeof this.selectedDate == 'string'  ? this.selectedDate :this.selectedDate[0];
      const startTime = this.convertTo24Hour(this.startTimeStr, this.startAmPm);
      const endTime = this.convertTo24Hour(this.endTimeStr, this.endAmPm);
      const startHour = `${startTime.hour.toString().padStart(2, '0')}:${startTime.minute.toString().padStart(2, '0')}`;
      const endHour = `${endTime.hour.toString().padStart(2, '0')}:${endTime.minute.toString().padStart(2, '0')}`;
      startMoment = moment(`${dateStr} ${startHour}`, 'YYYY-MM-DD HH:mm');
      endMoment = moment(`${dateStr} ${endHour}`, 'YYYY-MM-DD HH:mm');
    }
 
    return { priority, key, startMoment, endMoment };
  }
 
  filterExecutor(logs: Array<any>, filters: any): Array<any> {
    return logs.filter((line) => {
      const logMoment = moment(line.stamp);
      let prio: string;
 
      prio = filters.priority === 'All' ? line.priority : filters.priority;
 
      const matchesPriority = line.priority === prio;
      const matchesKeyword = line.message.toLowerCase().indexOf(filters.key) !== -1;
 
      let matchesDateRange = true;
      if (filters.startMoment && logMoment.isBefore(filters.startMoment)) {
        matchesDateRange = false;
      }
      if (filters.endMoment && logMoment.isAfter(filters.endMoment)) {
        matchesDateRange = false;
      }
 
      return matchesPriority && matchesKeyword && matchesDateRange;
    });
  }
 
  filterLogs() {
    const filters = this.abstractFilters();
    this.clog = this.filterExecutor(this.contentData.clog, filters);
    this.audit_log = this.filterExecutor(this.contentData.audit_log, filters);
  }
 
  clearSearchKey() {
    this.search = '';
    this.filterLogs();
  }
  clearDate() {
    this.selectedDate = [];
    this.startTimeStr = '12:00';
    this.startAmPm = 'AM';
    this.endTimeStr = '11:59';
    this.endAmPm = 'PM';
    this.filterLogs();
  }
 
  resetFilter() {
    this.priority = Prioity.All;
    this.search = '';
    this.selectedDate = [];
    this.startTimeStr = '12:00';
    this.startAmPm = 'AM';
    this.endTimeStr = '11:59';
    this.endAmPm = 'PM';
    this.filterLogs();
 
    return false;
  }
 
  logToText(log: object) {
    let logText = '';
    for (const line of Object.keys(log)) {
      logText =
        logText +
        this.datePipe.transform(log[line].stamp, 'medium') +
        '\t' +
        log[line].priority +
        '\t' +
        log[line].message +
        '\n';
    }
    return logText;
  }
}