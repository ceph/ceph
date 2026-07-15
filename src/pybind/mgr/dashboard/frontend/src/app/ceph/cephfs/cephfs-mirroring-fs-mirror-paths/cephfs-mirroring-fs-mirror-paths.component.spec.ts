declare const jest: any;

import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { of, throwError } from 'rxjs';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { DeletionImpact } from '~/app/shared/enum/delete-confirmation-modal-impact.enum';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { MirrorStatusResponse } from '~/app/shared/models/cephfs.model';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { CephfsMirroringFsMirrorPathsComponent } from './cephfs-mirroring-fs-mirror-paths.component';
import { HttpClientModule } from '@angular/common/http';
import { RouterTestingModule } from '@angular/router/testing';
import { SharedModule } from '~/app/shared/shared.module';

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

  const mockSchedulePolicies = [
    {
      path: '/path1',
      schedule: '1h',
      start: '2024-01-01T00:00:00Z',
      retention: { h: 24, d: 7 },
      active: true,
      fs: 'test-fs',
      last: '2024-01-01T01:00:00Z'
    },
    {
      path: '/path1',
      schedule: '1d',
      start: '2024-01-01T00:00:00Z',
      retention: { d: 30 },
      active: false,
      fs: 'test-fs'
    }
  ];

  beforeEach(async () => {
    const cephfsServiceMock = {
      getMirrorStatus: jest.fn(),
      listMirrorCheckpoints: jest.fn().mockReturnValue(
        of({
          dir_root: '',
          checkpoints: []
        })
      ),
      removeMirrorDirectory: jest.fn().mockReturnValue(of({}))
    };

    const snapshotScheduleServiceMock = {
      getSnapshotSchedule: jest.fn(),
      parseScheduleCopy: jest.fn((schedule: string) => schedule),
      delete: jest.fn()
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
          provide: CephfsSnapshotScheduleService,
          useValue: snapshotScheduleServiceMock
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
        },
        {
          provide: AuthStorageService,
          useValue: {
            getPermissions: () => ({
              cephfsMirror: { read: true, create: true, update: true, delete: true }
            })
          }
        },
        {
          provide: ModalCdsService,
          useValue: { show: jest.fn() }
        },
        {
          provide: TaskWrapperService,
          useValue: { wrapTaskAroundCall: jest.fn((args) => args.call) }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA],
      imports: [HttpClientModule, RouterTestingModule, SharedModule]
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
    expect(component.columns.length).toBe(6);
    expect(component.columns[0].prop).toBe('path');
    expect(component.columns[1].prop).toBe('syncStatus');
    expect(component.columns[2].prop).toBe('snapshotCount');
    expect(component.columns[3].prop).toBe('checkpointCount');
    expect(component.columns[4].prop).toBe('currentSyncSnapshot');
    expect(component.columns[5].prop).toBe('lastSyncedSnapshot');

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
      expect(result[0].pendingSnapshotCount).toBe(1);
      expect(result[0].syncStatusIcon).toBe('inProgress');
      expect(result[0].syncStatusClass).toBe('info');
      expect(result[0].snapshots).toEqual([
        {
          name: 'snap-current',
          status: 'in-progress',
          eta: 'calculating...',
          icon: 'inProgress',
          iconClass: 'info',
          statusLabel: 'replication in-progress'
        },
        {
          name: 'snap-last',
          status: 'replicated',
          icon: 'checkMarkOutline',
          iconClass: 'success',
          statusLabel: 'replicated.'
        }
      ]);
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
      expect(result[1].pendingSnapshotCount).toBe(1);
      expect(result[1].snapshots).toEqual([
        {
          name: 'snap-current-2',
          status: 'pending',
          icon: 'pendingFilled',
          iconClass: 'muted',
          statusLabel: 'replication pending'
        },
        {
          name: 'snap-last-2',
          status: 'replicated',
          icon: 'checkMarkOutline',
          iconClass: 'success',
          statusLabel: 'replicated.'
        }
      ]);
      expect(result[1].filesSynced).toBe(50);
      expect(result[1].totalFiles).toBe(100);
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
      expect(result[0].pendingSnapshotCount).toBe(0);
      expect(result[0].snapshots).toEqual([]);
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
      cephfsService.listMirrorCheckpoints.mockImplementation((_, path: string) =>
        of({
          dir_root: path,
          checkpoints:
            path === '/path1'
              ? [{ snap_id: 1, snap_name: 'snap1' }, { snap_id: 2, snap_name: 'snap2' }]
              : [{ snap_id: 3, snap_name: 'snap3' }]
        })
      );
      component.fsName = 'test-fs';

      component.loadMirrorPaths();

      expect(cephfsService.getMirrorStatus).toHaveBeenCalledWith('test-fs');
      expect(cephfsService.listMirrorCheckpoints).toHaveBeenCalledTimes(2);
      expect(component.mirrorPaths.length).toBe(2);
      expect(component.mirrorPaths[0].checkpointCount).toBe(2);
      expect(component.mirrorPaths[1].checkpointCount).toBe(1);
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

  describe('selectedPathSyncStatusIcon', () => {
    it('should return sync status icon from selected path', () => {
      component.selectedPath = {
        syncStatusIcon: 'inProgress',
        syncStatusClass: 'info'
      } as any;

      expect(component.selectedPathSyncStatusIcon).toBe('inProgress');
      expect(component.selectedPathSyncStatusClass).toBe('info');
    });

    it('should return defaults when no path is selected', () => {
      component.selectedPath = null;

      expect(component.selectedPathSyncStatusIcon).toBe('infoCircle');
      expect(component.selectedPathSyncStatusClass).toBe('');
    });
  });

  describe('onPathClick', () => {
    it('should set selected path and open side panel', fakeAsync(() => {
      const mockPath = {
        path: '/test',
        syncStatus: 'syncing' as const,
        currentSyncSnapshot: 'snap1',
        lastSyncedSnapshot: 'snap0'
      };

      component.onPathClick(mockPath as any);
      tick();

      expect(component.selectedPath).toBe(mockPath);
      expect(component.sidePanelOpen).toBe(true);
    }));
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
      const mockSelection = new CdTableSelection([{ path: '/test' }]);

      component.updateSelection(mockSelection);

      expect(component.selection).toBe(mockSelection);
    });
  });

  describe('Schedule Policy Tests', () => {
    let snapshotScheduleService: any;

    beforeEach(() => {
      snapshotScheduleService = TestBed.inject(CephfsSnapshotScheduleService);
    });

    describe('loadSchedulePolicies', () => {
      it('should load schedule policies for a path', () => {
        snapshotScheduleService.getSnapshotSchedule.mockReturnValue(of(mockSchedulePolicies));
        component.fsName = 'test-fs';
        component.selectedPath = { path: '/path1' } as any;

        component.loadSchedulePolicies('/path1');

        expect(snapshotScheduleService.getSnapshotSchedule).toHaveBeenCalledWith(
          '/path1',
          'test-fs',
          false
        );
        expect(component.schedulePoliciesLoading).toBe(false);
        expect(component.schedulePolicies.length).toBeGreaterThan(0);
      });

      it('should set empty array when fsName is empty', () => {
        component.fsName = '';

        component.loadSchedulePolicies('/path1');

        expect(snapshotScheduleService.getSnapshotSchedule).not.toHaveBeenCalled();
        expect(component.schedulePolicies).toEqual([]);
      });

      it('should set empty array when path is empty', () => {
        component.fsName = 'test-fs';

        component.loadSchedulePolicies('');

        expect(snapshotScheduleService.getSnapshotSchedule).not.toHaveBeenCalled();
        expect(component.schedulePolicies).toEqual([]);
      });

      it('should handle error when loading schedule policies', () => {
        snapshotScheduleService.getSnapshotSchedule.mockReturnValue(
          throwError(() => new Error('API Error'))
        );
        component.fsName = 'test-fs';
        component.selectedPath = { path: '/path1' } as any;

        component.loadSchedulePolicies('/path1');

        expect(component.schedulePoliciesLoading).toBe(false);
        expect(component.schedulePolicies).toEqual([]);
      });

      it('should filter policies by path', () => {
        const policies = [
          ...mockSchedulePolicies,
          {
            path: '/path2',
            schedule: '1h',
            start: '2024-01-01T00:00:00Z',
            retention: {},
            active: true,
            fs: 'test-fs'
          }
        ];
        snapshotScheduleService.getSnapshotSchedule.mockReturnValue(of(policies));
        component.fsName = 'test-fs';
        component.selectedPath = { path: '/path1' } as any;

        component.loadSchedulePolicies('/path1');

        expect(component.schedulePolicies.length).toBe(2);
        expect(component.schedulePolicies.every((p) => p.path === '/path1')).toBe(true);
      });

      it('should remove duplicate policies', () => {
        const duplicatePolicies = [
          ...mockSchedulePolicies,
          mockSchedulePolicies[0] // duplicate
        ];
        snapshotScheduleService.getSnapshotSchedule.mockReturnValue(of(duplicatePolicies));
        component.fsName = 'test-fs';
        component.selectedPath = { path: '/path1' } as any;

        component.loadSchedulePolicies('/path1');

        expect(component.schedulePolicies.length).toBe(2);
      });

      it('should not update policies if selected path changed', () => {
        snapshotScheduleService.getSnapshotSchedule.mockReturnValue(of(mockSchedulePolicies));
        component.fsName = 'test-fs';
        component.selectedPath = { path: '/path2' } as any;

        component.loadSchedulePolicies('/path1');

        expect(component.schedulePoliciesLoading).toBe(false);
      });
    });

    describe('removeSchedulePolicy', () => {
      it('should remove a schedule policy', () => {
        snapshotScheduleService.delete.mockReturnValue(of({}));
        snapshotScheduleService.getSnapshotSchedule.mockReturnValue(of([]));
        component.fsName = 'test-fs';

        const policy = {
          path: '/path1',
          schedule: '1h',
          start: '2024-01-01T00:00:00Z',
          fs: 'test-fs'
        };

        component.removeSchedulePolicy(policy as any);

        expect(snapshotScheduleService.delete).toHaveBeenCalledWith({
          path: '/path1',
          schedule: '1h',
          start: '2024-01-01T00:00:00Z',
          fs: 'test-fs'
        });
        expect(component.removingSchedule).toBe('');
      });

      it('should handle error when removing schedule policy', () => {
        snapshotScheduleService.delete.mockReturnValue(throwError(() => new Error('API Error')));
        component.fsName = 'test-fs';

        const policy = {
          path: '/path1',
          schedule: '1h',
          start: '2024-01-01T00:00:00Z',
          fs: 'test-fs'
        };

        component.removeSchedulePolicy(policy as any);

        expect(component.removingSchedule).toBe('');
      });

      it('should not remove policy when path is missing', () => {
        component.fsName = 'test-fs';

        const policy = {
          path: '',
          schedule: '1h',
          start: '2024-01-01T00:00:00Z'
        };

        component.removeSchedulePolicy(policy as any);

        expect(snapshotScheduleService.delete).not.toHaveBeenCalled();
      });

      it('should not remove policy when schedule is missing', () => {
        component.fsName = 'test-fs';

        const policy = {
          path: '/path1',
          schedule: '',
          start: '2024-01-01T00:00:00Z'
        };

        component.removeSchedulePolicy(policy as any);

        expect(snapshotScheduleService.delete).not.toHaveBeenCalled();
      });

      it('should not remove policy when fsName is empty', () => {
        component.fsName = '';

        const policy = {
          path: '/path1',
          schedule: '1h',
          start: '2024-01-01T00:00:00Z'
        };

        component.removeSchedulePolicy(policy as any);

        expect(snapshotScheduleService.delete).not.toHaveBeenCalled();
      });
    });

    describe('formatScheduleDate', () => {
      it('should format valid date string', () => {
        const result = component.formatScheduleDate('2024-01-01T00:00:00Z');
        expect(result).not.toBe('-');
      });

      it('should return dash for null value', () => {
        const result = component.formatScheduleDate(null);
        expect(result).toBe('-');
      });

      it('should return dash for undefined value', () => {
        const result = component.formatScheduleDate(undefined);
        expect(result).toBe('-');
      });

      it('should return dash for invalid date string', () => {
        const result = component.formatScheduleDate('invalid-date');
        expect(result).toBe('-');
      });

      it('should format Date object', () => {
        const date = new Date('2024-01-01T00:00:00Z');
        const result = component.formatScheduleDate(date);
        expect(result).not.toBe('-');
      });
    });

    describe('getScheduleStatusIcon', () => {
      it('should return success icon for active schedule', () => {
        const icon = component.getScheduleStatusIcon(true);
        expect(icon).toBe('success');
      });

      it('should return warning icon for inactive schedule', () => {
        const icon = component.getScheduleStatusIcon(false);
        expect(icon).toBe('warning');
      });
    });

    describe('buildSchedulePolicyViewModel', () => {
      it('should build view model with all properties', () => {
        snapshotScheduleService.parseScheduleCopy.mockReturnValue('Every hour');
        const policy = mockSchedulePolicies[0];

        const result = component['buildSchedulePolicyViewModel'](policy as any);

        expect(result.scheduleCopy).toBe('Every hour');
        expect(result.retentionCopy).toBeDefined();
        expect(result.nextSync).toBeDefined();
        expect(result.scheduleText).toBe('1h');
        expect(result.retentionText).toBeDefined();
        expect(result.statusLabel).toBeDefined();
        expect(result.statusIcon).toBeDefined();
        expect(result.removeId).toBe('/path1@1h');
      });

      it('should handle policy with string retention', () => {
        snapshotScheduleService.parseScheduleCopy.mockReturnValue('Every hour');
        const policy = {
          ...mockSchedulePolicies[0],
          retention: 'invalid'
        };

        const result = component['buildSchedulePolicyViewModel'](policy as any);

        expect(result.retention).toEqual({});
      });

      it('should handle policy without retention', () => {
        snapshotScheduleService.parseScheduleCopy.mockReturnValue('Every hour');
        const policy = {
          path: '/path1',
          schedule: '1h',
          start: '2024-01-01T00:00:00Z',
          active: true,
          fs: 'test-fs'
        };

        const result = component['buildSchedulePolicyViewModel'](policy as any);

        expect(result.retentionText).toBe('-');
      });
    });

    describe('calculateNextSync', () => {
      it('should calculate next sync for minutely schedule', () => {
        const policy = {
          schedule: '30m',
          start: '2024-01-01T00:00:00Z'
        };

        const result = component['calculateNextSync'](policy as any);

        expect(result).not.toBe('-');
      });

      it('should calculate next sync for hourly schedule', () => {
        const policy = {
          schedule: '2h',
          start: '2024-01-01T00:00:00Z'
        };

        const result = component['calculateNextSync'](policy as any);

        expect(result).not.toBe('-');
      });

      it('should calculate next sync for daily schedule', () => {
        const policy = {
          schedule: '1d',
          start: '2024-01-01T00:00:00Z'
        };

        const result = component['calculateNextSync'](policy as any);

        expect(result).not.toBe('-');
      });

      it('should calculate next sync for weekly schedule', () => {
        const policy = {
          schedule: '1w',
          start: '2024-01-01T00:00:00Z'
        };

        const result = component['calculateNextSync'](policy as any);

        expect(result).not.toBe('-');
      });

      it('should calculate next sync for monthly schedule', () => {
        const policy = {
          schedule: '1M',
          start: '2024-01-01T00:00:00Z'
        };

        const result = component['calculateNextSync'](policy as any);

        expect(result).not.toBe('-');
      });

      it('should calculate next sync for yearly schedule', () => {
        const policy = {
          schedule: '1y',
          start: '2024-01-01T00:00:00Z'
        };

        const result = component['calculateNextSync'](policy as any);

        expect(result).not.toBe('-');
      });

      it('should use last time if available', () => {
        const policy = {
          schedule: '1h',
          start: '2024-01-01T00:00:00Z',
          last: '2024-01-01T01:00:00Z'
        };

        const result = component['calculateNextSync'](policy as any);

        expect(result).not.toBe('-');
      });

      it('should return dash for missing schedule', () => {
        const policy = {
          start: '2024-01-01T00:00:00Z'
        };

        const result = component['calculateNextSync'](policy as any);

        expect(result).toBe('-');
      });

      it('should return dash for missing start time', () => {
        const policy = {
          schedule: '1h'
        };

        const result = component['calculateNextSync'](policy as any);

        expect(result).toBe('-');
      });

      it('should return dash for invalid date', () => {
        const policy = {
          schedule: '1h',
          start: 'invalid-date'
        };

        const result = component['calculateNextSync'](policy as any);

        expect(result).toBe('-');
      });

      it('should return dash for invalid schedule format', () => {
        const policy = {
          schedule: 'invalid',
          start: '2024-01-01T00:00:00Z'
        };

        const result = component['calculateNextSync'](policy as any);

        expect(result).toBe('-');
      });

      it('should return dash for unknown schedule unit', () => {
        const policy = {
          schedule: '1x',
          start: '2024-01-01T00:00:00Z'
        };

        const result = component['calculateNextSync'](policy as any);

        expect(result).toBe('-');
      });
    });

    describe('buildRetentionCopy', () => {
      it('should build retention copy for hourly retention', () => {
        const retention = { h: 24 };

        const result = component['buildRetentionCopy'](retention);

        expect(result).toContain('24 hourly');
      });

      it('should build retention copy for daily retention', () => {
        const retention = { d: 7 };

        const result = component['buildRetentionCopy'](retention);

        expect(result).toContain('7 daily');
      });

      it('should build retention copy for weekly retention', () => {
        const retention = { w: 4 };

        const result = component['buildRetentionCopy'](retention);

        expect(result).toContain('4 weekly');
      });

      it('should build retention copy for monthly retention', () => {
        const retention = { M: 12 };

        const result = component['buildRetentionCopy'](retention);

        expect(result).toContain('12 monthly');
      });

      it('should build retention copy for multiple retention periods', () => {
        const retention = { h: 24, d: 7, w: 4 };

        const result = component['buildRetentionCopy'](retention);

        expect(result.length).toBe(3);
      });

      it('should return empty array for empty retention', () => {
        const retention = {};

        const result = component['buildRetentionCopy'](retention);

        expect(result).toEqual([]);
      });

      it('should return empty array for undefined retention', () => {
        const result = component['buildRetentionCopy'](undefined);

        expect(result).toEqual([]);
      });

      it('should filter out null retention values', () => {
        const retention = { h: 24, d: null as any };

        const result = component['buildRetentionCopy'](retention);

        expect(result.length).toBe(1);
      });
    });

    describe('closeSidePanel with schedule policies', () => {
      it('should clear schedule policies when closing side panel', () => {
        component.sidePanelOpen = true;
        component.selectedPath = {} as any;
        component.schedulePolicies = mockSchedulePolicies as any;
        component.schedulePoliciesLoading = true;
        component.removingSchedule = 'test';

        component.closeSidePanel();

        expect(component.sidePanelOpen).toBe(false);
        expect(component.selectedPath).toBeNull();
        expect(component.schedulePolicies).toEqual([]);
        expect(component.schedulePoliciesLoading).toBe(false);
        expect(component.removingSchedule).toBe('');
      });
    });

    describe('onPathClick with schedule policies', () => {
      it('should load schedule policies when path is clicked', fakeAsync(() => {
        snapshotScheduleService.getSnapshotSchedule.mockReturnValue(of(mockSchedulePolicies));
        cephfsService.getMirrorStatus.mockReturnValue(of(mockMirrorStatusResponse));
        component.fsName = 'test-fs';

        const mockPath = {
          path: '/path1',
          syncStatus: 'syncing' as const,
          currentSyncSnapshot: 'snap1',
          lastSyncedSnapshot: 'snap0'
        };

        component.onPathClick(mockPath as any);
        tick();

        expect(snapshotScheduleService.getSnapshotSchedule).toHaveBeenCalledWith(
          '/path1',
          'test-fs',
          false
        );
      }));
    });
  });
  describe('removePathModal', () => {
    it('should open high-impact deletion modal for selected path', () => {
      const modalService = TestBed.inject(ModalCdsService);
      component.selection = new CdTableSelection([{ path: '/path1' }]);
      component.fsName = 'test-fs';

      component.removePathModal();

      expect(modalService.show).toHaveBeenCalledWith(
        DeleteConfirmationModalComponent,
        expect.objectContaining({
          impact: DeletionImpact.high,
          itemNames: ['/path1'],
          actionDescription: 'remove'
        })
      );
    });
  });
});
