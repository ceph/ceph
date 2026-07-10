import { Component, NgZone, OnDestroy, OnInit } from '@angular/core';
import { forkJoin } from 'rxjs';
import { finalize } from 'rxjs/operators';

import { ListItem } from 'carbon-components-angular';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { LokiService } from '~/app/shared/api/loki.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { LokiLogLine } from '~/app/shared/models/loki.interface';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-loki-logs',
  templateUrl: './loki-logs.component.html',
  styleUrls: ['./loki-logs.component.scss', '../logs/logs.component.scss'],
  standalone: false
})
export class LokiLogsComponent implements OnInit, OnDestroy {
  scrollable = true;
  showDownloadCopyButton = true;

  loadingServices = true;
  lokiAvailable = false;
  alloyAvailable = false;

  loadingFilenames = false;
  loadingLogs = false;

  filenameItems: ListItem[] = [];
  selectedFilename = '';
  downloadFileName = 'loki_log';

  logLines: LokiLogLine[] = [];
  logText = '';
  hasRun = false;

  private readonly refreshIntervalMs = 5000;
  private refreshInterval?: number;

  constructor(
    private cephService: CephServiceService,
    private lokiService: LokiService,
    private notificationService: NotificationService,
    private ngZone: NgZone
  ) {}

  ngOnInit(): void {
    this.restoreViewState();
    this.checkServices();
  }

  ngOnDestroy(): void {
    this.stopLogRefresh();
  }

  checkServices(): void {
    this.loadingServices = true;
    forkJoin({
      loki: this.cephService.getDaemons('loki'),
      alloy: this.cephService.getDaemons('alloy')
    })
      .pipe(finalize(() => (this.loadingServices = false)))
      .subscribe({
        next: ({ loki, alloy }) => {
          this.lokiAvailable = loki.some((d) => d.status === 1);
          this.alloyAvailable = alloy.some((d) => d.status === 1);
          if (this.lokiAvailable && this.alloyAvailable) {
            this.loadFilenames();
          }
        },
        error: () => {
          this.lokiAvailable = false;
          this.alloyAvailable = false;
        }
      });
  }

  loadFilenames(): void {
    this.loadingFilenames = true;
    this.lokiService
      .getLogFilenames('7d')
      .pipe(finalize(() => (this.loadingFilenames = false)))
      .subscribe({
        next: (filenames) => {
          const selected = this.selectedFilename;
          this.filenameItems = filenames.sort().map((filename) => ({
            content: filename,
            selected: filename === selected
          }));
          if (!selected && this.filenameItems.length > 0) {
            this.selectedFilename = this.filenameItems[0].content;
            this.filenameItems[0].selected = true;
            this.updateDownloadFileName();
            this.persistViewState();
          }
          if (this.hasRun && this.selectedFilename) {
            this.getInfo(true);
            this.startLogRefresh();
          }
        },
        error: (resp) => {
          this.notificationService.show(
            NotificationType.error,
            resp,
            $localize`Failed to load log files`
          );
        }
      });
  }

  onFilenameSelected(event: ListItem | { item: ListItem }): void {
    const item = 'item' in event ? event.item : event;
    const previousFilename = this.selectedFilename;
    this.selectedFilename = item.content;
    this.updateDownloadFileName();
    this.persistViewState();

    if (previousFilename !== this.selectedFilename) {
      this.stopLogRefresh();
    }
  }

  runQuery(): void {
    if (!this.selectedFilename) {
      return;
    }

    this.hasRun = true;
    this.persistViewState();
    this.getInfo(true);
    this.startLogRefresh();
  }

  getInfo(showLoading = false): void {
    if (!this.selectedFilename) {
      return;
    }

    if (showLoading) {
      this.loadingLogs = true;
    }

    this.lokiService
      .getLogLinesForFilename(this.selectedFilename, { since: '24h', limit: 5000 })
      .pipe(
        finalize(() => {
          if (showLoading) {
            this.loadingLogs = false;
          }
        })
      )
      .subscribe({
        next: (lines) => {
          this.logLines = lines;
          this.logText = this.lokiService.logLinesToText(lines);
        },
        error: (resp) => {
          if (showLoading) {
            this.notificationService.show(
              NotificationType.error,
              resp,
              $localize`Failed to query logs`
            );
          }
        }
      });
  }

  trackByLogEntry(index: number, entry: LokiLogLine): string {
    return `${entry.stamp}-${index}`;
  }

  private restoreViewState(): void {
    const saved = this.lokiService.getViewState();
    if (!saved) {
      return;
    }
    this.selectedFilename = saved.selectedFilename;
    this.hasRun = saved.hasRun;
    this.updateDownloadFileName();
  }

  private persistViewState(): void {
    this.lokiService.saveViewState({
      selectedFilename: this.selectedFilename,
      hasRun: this.hasRun
    });
  }

  private startLogRefresh(): void {
    this.stopLogRefresh();
    this.ngZone.runOutsideAngular(() => {
      this.refreshInterval = window.setInterval(() => {
        this.ngZone.run(() => {
          if (this.hasRun && this.selectedFilename) {
            this.getInfo();
          }
        });
      }, this.refreshIntervalMs);
    });
  }

  private stopLogRefresh(): void {
    if (this.refreshInterval !== undefined) {
      clearInterval(this.refreshInterval);
      this.refreshInterval = undefined;
    }
  }

  private updateDownloadFileName(): void {
    this.downloadFileName = this.lokiService.downloadFileNameFromPath(this.selectedFilename);
  }
}
