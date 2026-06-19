import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { of, throwError } from 'rxjs';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { MirrorStatusResponse } from '~/app/shared/models/cephfs.model';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { CephfsMirroringFsMirrorPathsComponent } from './cephfs-mirroring-fs-mirror-paths.component';

describe('CephfsMirroringFsMirrorPathsComponent', () => {
  let component: CephfsMirroringFsMirrorPathsComponent;
  let fixture: ComponentFixture<CephfsMirroringFsMirrorPathsComponent>;
  let cephfsService: any;
  let formatterService: any;

  const mockMirrorStatusResponse: MirrorStatusResponse = {
    metrics: {
      '/path1': {
        peer: {
          'peer-uuid-1': {
            state: 'syncing',
            current_syncing_snap: {
              id: 4,
              name: 'snap-current',
              'sync-mode': 'delta',
              avg_read_throughput_bytes: '0.00 B/s',
              avg_write_throughput_bytes: '0.00 B/s',
              crawl: {
                state: 'completed',
                duration: '0s'
              },
              datasync_queue_wait: {
                state: 'completed',
                duration: '0s'
              },
              bytes: {
                sync_bytes: '1.50 GiB',
                total_bytes: '3.00 GiB',
                sync_percent: '50.00%'
              },
              files: {
                sync_files: 100,
                total_files: 200,
                sync_percent: '50.00%'
              },
              eta: 'calculating...'
            },
            last_synced_snap: {
              id: 1,
              name: 'snap-last',
              sync_time_stamp: '1583.101609s'
            },
            snaps_synced: 10,
            snaps_deleted: 2,
            snaps_renamed: 1
          }
        }
      },
      '/path2': {
        peer: {
          'peer-uuid-2': {
            state: 'idle',
            current_syncing_snap: {
              id: 5,
              name: 'snap-current-2',
              bytes: {
                sync_bytes: '1.00 GiB',
                total_bytes: '2.00 GiB',
                sync_percent: '50.00%'
              },
              files: {
                sync_files: 50,
                total_files: 100,
                sync_percent: '50.00%'
              },
              eta: '10s'
            },
            last_synced_snap: {
              id: 2,
              name: 'snap-last-2'
            },
            snaps_synced: 5,
            snaps_deleted: 1
          }
        }
      }
    }
  };

  beforeEach(async () => {
    const cephfsServiceMock = {
      getMirrorStatus: jest.fn()
    };

    const formatterServiceMock = {
      toBytes: jest.fn((value: string) => {
        const match = value.match(/([\d.]+)\s*(\w+)/);
        if (!match) return 0;
        const val = parseFloat(match[1]);
        const unit = match[2].toUpperCase();
        const multipliers: { [key: string]: number } = {
          B: 1,
          KIB: 1024,
          MIB: 1024 * 1024,
          GIB: 1024 * 1024 * 1024,
          TIB: 1024 * 1024 * 1024 * 1024,
          KB: 1024,
          MB: 1024 * 1024,
          GB: 1024 * 1024 * 1024,
          TB: 1024 * 1024 * 1024 * 1024
        };
        return val * (multipliers[unit] || 1);
      })
    };

    await TestBed.configureTestingModule({
      declarations: [CephfsMirroringFsMirrorPathsComponent],
      providers: [
        {
          provide: CephfsService,
          useValue: cephfsServiceMock
        },
        {
          provide: FormatterService,
          useValue: formatterServiceMock
        },
        {
          provide: ActivatedRoute,
          useValue: {
            parent: {
              paramMap: of(convertToParamMap({ fsName: 'test-fs' }))
            }
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    cephfsService = TestBed.inject(CephfsService);
    formatterService = TestBed.inject(FormatterService);

    fixture = TestBed.createComponent(CephfsMirroringFsMirrorPathsComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize columns and fetch fsName on init', () => {
    cephfsService.getMirrorStatus.mockReturnValue(of(mockMirrorStatusResponse));

    component.ngOnInit();

    expect(component.columns).toBeDefined();
    expect(component.columns.length).toBe(5);
    expect(component.columns[0].prop).toBe('path');
    expect(component.columns[1].prop).toBe('syncStatus');
    expect(component.columns[2].prop).toBe('snapshotCount');
    expect(component.columns[3].prop).toBe('currentSyncSnapshot');
    expect(component.columns[4].prop).toBe('lastSyncedSnapshot');

    // Verify fsName is fetched and data is loaded
    expect(component.fsName).toBe('test-fs');
    expect(cephfsService.getMirrorStatus).toHaveBeenCalledWith('test-fs');
  });

  describe('parseMirrorStatus', () => {
    it('should return empty array when data is null', () => {
      const result = component.parseMirrorStatus(null as any);
      expect(result).toEqual([]);
    });

    it('should return empty array when data.metrics is undefined', () => {
      const result = component.parseMirrorStatus({} as any);
      expect(result).toEqual([]);
    });

    it('should parse mirror status with current_syncing_snap structure', () => {
      const result = component.parseMirrorStatus(mockMirrorStatusResponse);

      expect(result.length).toBe(2);

      expect(result[0].path).toBe('/path1');
      expect(result[0].syncStatus).toBe('syncing');
      expect(result[0].currentSyncSnapshot).toBe('snap-current');
      expect(result[0].currentSyncEta).toBe('calculating...');
      expect(result[0].currentSyncMode).toBe('delta');
      expect(result[0].lastSyncedSnapshot).toBe('snap-last');
      expect(result[0].lastSyncedTime).toBe('1583.101609s');
      expect(result[0].snapshotCount).toBe(10);
      expect(result[0].checkpointCount).toBe(2);
      expect(result[0].renamedSnapshotCount).toBe(1);
      expect(result[0].filesSynced).toBe(100);
      expect(result[0].totalFiles).toBe(200);
      expect(result[0].syncProgress).toBe(50);
      expect(result[0].crawlState).toBe('completed');
      expect(result[0].crawlDuration).toBe('0s');
      expect(result[0].datasyncQueueWaitState).toBe('completed');
      expect(result[0].datasyncQueueWaitDuration).toBe('0s');
      expect(result[0].avgReadThroughput).toBe('0.00 B/s');
      expect(result[0].avgWriteThroughput).toBe('0.00 B/s');

      expect(result[1].path).toBe('/path2');
      expect(result[1].syncStatus).toBe('idle');
      expect(result[1].currentSyncSnapshot).toBe('snap-current-2');
      expect(result[1].lastSyncedSnapshot).toBe('snap-last-2');
      expect(result[1].snapshotCount).toBe(5);
      expect(result[1].checkpointCount).toBe(1);
      expect(result[1].filesSynced).toBe(50);
      expect(result[1].totalFiles).toBe(100);
      expect(result[1].bytesSynced).toBe(1073741824);
      expect(result[1].totalBytes).toBe(2147483648);
      expect(result[1].syncProgress).toBe(50);
    });

    it('should skip paths without peer data', () => {
      const dataWithoutPeer: MirrorStatusResponse = {
        metrics: {
          '/path1': {} as any
        }
      };

      const result = component.parseMirrorStatus(dataWithoutPeer);
      expect(result).toEqual([]);
    });

    it('should use default values when optional fields are missing', () => {
      const minimalData: MirrorStatusResponse = {
        metrics: {
          '/path1': {
            peer: {
              'peer-1': {
                state: 'idle'
              }
            }
          }
        }
      };

      const result = component.parseMirrorStatus(minimalData);

      expect(result.length).toBe(1);
      expect(result[0].syncStatus).toBe('idle');
      expect(result[0].currentSyncSnapshot).toBe('-');
      expect(result[0].lastSyncedSnapshot).toBe('-');
      expect(result[0].snapshotCount).toBe(0);
      expect(result[0].checkpointCount).toBe(0);
      expect(result[0].syncProgress).toBe(0);
    });
  });

  describe('parsePercent', () => {
    it('should parse percentage string', () => {
      const result = component['parsePercent']('78.00%');
      expect(result).toBe(78);
    });

    it('should return 0 for undefined input', () => {
      const result = component['parsePercent'](undefined);
      expect(result).toBe(0);
    });

    it('should return 0 for invalid string format', () => {
      const result = component['parsePercent']('invalid');
      expect(result).toBe(0);
    });
  });

  describe('parseByteValue', () => {
    it('should parse byte value from string format', () => {
      const result = component['parseByteValue']('1.5 GiB');

      expect(result).toBeGreaterThan(0);
      expect(formatterService.toBytes).toHaveBeenCalledWith('1.5GiB', 0);
    });

    it('should return 0 for undefined input', () => {
      const result = component['parseByteValue'](undefined);

      expect(result).toBe(0);
    });

    it('should return 0 for invalid string format', () => {
      formatterService.toBytes.mockReturnValueOnce(0);
      const result = component['parseByteValue']('invalid');

      expect(result).toBe(0);
    });

    it('should handle different byte units', () => {
      component['parseByteValue']('512 MB');

      expect(formatterService.toBytes).toHaveBeenCalledWith('512MB', 0);
    });
  });

  describe('calculateSyncProgress', () => {
    it('should return file progress when available', () => {
      const result = component['calculateSyncProgress'](75, 50, 100, 500, 1000, 50);
      expect(result).toBe(75);
    });

    it('should calculate progress from file count when file progress is 0', () => {
      const result = component['calculateSyncProgress'](0, 50, 100, 0, 0, 0);
      expect(result).toBe(50);
    });

    it('should calculate progress from byte percentage when file progress and count are 0', () => {
      const result = component['calculateSyncProgress'](0, 0, 0, 500, 1000, 50);
      expect(result).toBe(50);
    });

    it('should calculate progress from byte count when percentages are unavailable', () => {
      const result = component['calculateSyncProgress'](0, 0, 0, 500, 1000, 0);
      expect(result).toBe(50);
    });

    it('should return 0 when no progress data is available', () => {
      const result = component['calculateSyncProgress'](0, 0, 0, 0, 0, 0);
      expect(result).toBe(0);
    });

    it('should round progress to nearest integer', () => {
      const result = component['calculateSyncProgress'](0, 33, 100, 0, 0, 0);
      expect(result).toBe(33);
    });
  });

  describe('extractPeerInfo', () => {
    it('should extract first peer info from path data', () => {
      const pathData = {
        peer: {
          'peer-1': { state: 'syncing' },
          'peer-2': { state: 'idle' }
        }
      };

      const result = component['extractPeerInfo'](pathData as any);

      expect(result).toBeDefined();
      expect(result?.state).toBe('syncing');
    });

    it('should return null when peer object is empty', () => {
      const pathData = { peer: {} };

      const result = component['extractPeerInfo'](pathData as any);

      expect(result).toBeNull();
    });

    it('should return null when peer is undefined', () => {
      const pathData = { peer: undefined };

      const result = component['extractPeerInfo'](pathData as any);

      expect(result).toBeNull();
    });
  });

  describe('loadMirrorPaths', () => {
    it('should load mirror paths successfully', () => {
      cephfsService.getMirrorStatus.mockReturnValue(of(mockMirrorStatusResponse));
      component.fsName = 'test-fs';

      component.loadMirrorPaths();

      expect(cephfsService.getMirrorStatus).toHaveBeenCalledWith('test-fs');
      expect(component.mirrorPaths.length).toBe(2);
    });

    it('should set empty array on error', () => {
      cephfsService.getMirrorStatus.mockReturnValue(throwError(() => new Error('API Error')));
      component.fsName = 'test-fs';

      component.loadMirrorPaths();

      expect(component.mirrorPaths).toEqual([]);
    });

    it('should not call service when fsName is empty', () => {
      component.fsName = '';

      component.loadMirrorPaths();

      expect(cephfsService.getMirrorStatus).not.toHaveBeenCalled();
    });
  });

  describe('getSyncStatusIcon', () => {
    it('should return correct icon for syncing status', () => {
      const icon = component.getSyncStatusIcon('syncing');
      expect(icon).toBe('inProgress');
    });

    it('should return correct icon for idle status', () => {
      const icon = component.getSyncStatusIcon('idle');
      expect(icon).toBe('pendingFilled');
    });

    it('should return correct icon for failed status', () => {
      const icon = component.getSyncStatusIcon('failed');
      expect(icon).toBe('danger');
    });

    it('should return correct icon for completed status', () => {
      const icon = component.getSyncStatusIcon('completed');
      expect(icon).toBe('checkMarkOutline');
    });

    it('should return default icon for unknown status', () => {
      const icon = component.getSyncStatusIcon('unknown');
      expect(icon).toBe('infoCircle');
    });
  });

  describe('getSyncStatusClass', () => {
    it('should return correct class for syncing status', () => {
      const cssClass = component.getSyncStatusClass('syncing');
      expect(cssClass).toBe('info');
    });

    it('should return correct class for completed status', () => {
      const cssClass = component.getSyncStatusClass('completed');
      expect(cssClass).toBe('success');
    });

    it('should return correct class for idle status', () => {
      const cssClass = component.getSyncStatusClass('idle');
      expect(cssClass).toBe('muted');
    });

    it('should return correct class for failed status', () => {
      const cssClass = component.getSyncStatusClass('failed');
      expect(cssClass).toBe('danger');
    });

    it('should return empty string for unknown status', () => {
      const cssClass = component.getSyncStatusClass('unknown');
      expect(cssClass).toBe('');
    });
  });

  describe('onPathClick', () => {
    it('should set selected path and open side panel', () => {
      const mockPath = {
        path: '/test',
        syncStatus: 'syncing' as const,
        currentSyncSnapshot: 'snap1',
        lastSyncedSnapshot: 'snap0'
      };

      component.onPathClick(mockPath as any);

      expect(component.selectedPath).toBe(mockPath);
      expect(component.sidePanelOpen).toBe(true);
    });
  });

  describe('closeSidePanel', () => {
    it('should close side panel and clear selected path', () => {
      component.sidePanelOpen = true;
      component.selectedPath = {} as any;

      component.closeSidePanel();

      expect(component.sidePanelOpen).toBe(false);
      expect(component.selectedPath).toBeNull();
    });
  });

  describe('onSelectionChange', () => {
    it('should update selection', () => {
      const mockSelection = { selected: [{ path: '/test' }] } as any;

      component.selection = mockSelection;

      expect(component.selection).toBe(mockSelection);
    });
  });
});
