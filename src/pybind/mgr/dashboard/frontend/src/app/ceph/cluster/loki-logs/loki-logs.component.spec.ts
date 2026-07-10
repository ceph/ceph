import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { of, throwError } from 'rxjs';

import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { LokiService } from '~/app/shared/api/loki.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { LokiLogsComponent } from './loki-logs.component';

describe('LokiLogsComponent', () => {
  let component: LokiLogsComponent;
  let fixture: ComponentFixture<LokiLogsComponent>;
  let lokiService: {
    getLogFilenames: jest.Mock;
    getLogs: jest.Mock;
    getViewState: jest.Mock;
    saveViewState: jest.Mock;
    logLinesToText: jest.Mock;
    downloadFileNameFromPath: jest.Mock;
  };
  let cephService: { getDaemons: jest.Mock };
  let notificationService: { show: jest.Mock };

  configureTestBed({
    imports: [HttpClientTestingModule, SharedModule, FormsModule],
    declarations: [LokiLogsComponent],
    providers: [
      {
        provide: LokiService,
        useValue: {
          getLogFilenames: jest.fn(),
          getLogs: jest.fn(),
          getViewState: jest.fn(),
          saveViewState: jest.fn(),
          logLinesToText: jest.fn(),
          downloadFileNameFromPath: jest.fn()
        }
      },
      {
        provide: CephServiceService,
        useValue: { getDaemons: jest.fn() }
      },
      {
        provide: NotificationService,
        useValue: { show: jest.fn() }
      }
    ]
  });

  beforeEach(() => {
    lokiService = TestBed.inject(LokiService) as typeof lokiService;
    cephService = TestBed.inject(CephServiceService) as typeof cephService;
    notificationService = TestBed.inject(NotificationService) as typeof notificationService;

    lokiService.getViewState.mockReturnValue(null);
    lokiService.logLinesToText.mockImplementation((lines) =>
      lines.map((line) => `${line.stamp}\t${line.message}`).join('\n')
    );
    lokiService.downloadFileNameFromPath.mockImplementation((filename: string) => {
      if (!filename) {
        return 'loki_log';
      }
      const base = filename.split('/').pop() ?? 'loki_log';
      return base.replace(/\.log$/, '') || 'loki_log';
    });
    lokiService.getLogFilenames.mockReturnValue(
      of(['/var/log/ceph/cephadm.log', '/var/log/ceph/ceph.log'])
    );
    lokiService.getLogs.mockReturnValue(
      of([
        {
          stamp: '2026-06-15T10:00:00.000Z',
          message: 'test log line',
          filename: '/var/log/ceph/cephadm.log'
        }
      ])
    );
    cephService.getDaemons.mockImplementation((service: string) => {
      if (service === 'loki') {
        return of([{ status: 1 }]);
      }
      return of([{ status: 1 }]);
    });

    fixture = TestBed.createComponent(LokiLogsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    component.ngOnDestroy();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load filenames when services are available', () => {
    expect(lokiService.getLogFilenames).toHaveBeenCalledWith('7d');
    expect(component.filenameItems.length).toBe(2);
  });

  it('should restore persisted view state and fetch fresh logs on init', () => {
    lokiService.getViewState.mockReturnValue({
      selectedFilename: '/var/log/ceph/cephadm.log',
      hasRun: true
    });

    const restoredFixture = TestBed.createComponent(LokiLogsComponent);
    const restored = restoredFixture.componentInstance;
    restoredFixture.detectChanges();

    expect(restored.hasRun).toBe(true);
    expect(restored.selectedFilename).toBe('/var/log/ceph/cephadm.log');
    expect(restored.logLines.length).toBe(1);
    expect(lokiService.getLogs).toHaveBeenCalledWith('/var/log/ceph/cephadm.log');
    expect(lokiService.saveViewState).not.toHaveBeenCalledWith(
      expect.objectContaining({ logLines: expect.anything() })
    );

    restored.ngOnDestroy();
  });

  it('should run query, persist state without log lines, and start refresh', () => {
    jest.spyOn(window, 'setInterval').mockReturnValue(42 as unknown as number);

    component.selectedFilename = '/var/log/ceph/cephadm.log';
    component.runQuery();

    expect(lokiService.getLogs).toHaveBeenCalledWith('/var/log/ceph/cephadm.log');
    expect(component.hasRun).toBe(true);
    expect(component.logLines.length).toBe(1);
    expect(lokiService.saveViewState).toHaveBeenCalledWith({
      selectedFilename: '/var/log/ceph/cephadm.log',
      hasRun: true
    });
    expect(window.setInterval).toHaveBeenCalled();
  });

  it('should not fetch logs when only the filename changes', () => {
    component.hasRun = true;
    component.selectedFilename = '/var/log/ceph/cephadm.log';
    lokiService.getLogs.mockClear();

    component.onFilenameSelected({ content: '/var/log/ceph/ceph.log', selected: true });

    expect(component.selectedFilename).toBe('/var/log/ceph/ceph.log');
    expect(lokiService.getLogs).not.toHaveBeenCalled();
    expect(lokiService.saveViewState).toHaveBeenCalledWith({
      selectedFilename: '/var/log/ceph/ceph.log',
      hasRun: true
    });
  });

  it('should show notification when filename load fails', () => {
    lokiService.getLogFilenames.mockReturnValue(throwError(() => 'error'));

    component.loadFilenames();

    expect(notificationService.show).toHaveBeenCalled();
  });
});
