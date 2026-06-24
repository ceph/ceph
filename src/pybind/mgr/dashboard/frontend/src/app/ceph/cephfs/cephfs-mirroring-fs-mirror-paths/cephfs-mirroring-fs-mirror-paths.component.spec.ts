import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { of, throwError } from 'rxjs';

import { CephfsService, SnapshotMirrorStatusResponse } from '~/app/shared/api/cephfs.service';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { CephfsMirroringFsMirrorPathsComponent } from './cephfs-mirroring-fs-mirror-paths.component';

describe('CephfsMirroringFsMirrorPathsComponent', () => {
  let component: CephfsMirroringFsMirrorPathsComponent;
  let fixture: ComponentFixture<CephfsMirroringFsMirrorPathsComponent>;
  let cephfsService: any;
  let formatterService: any;

  const mockSnapshotMirrorStatusResponse: SnapshotMirrorStatusResponse = {
    metrics: {
      '/path1': {
        peer: {
          'peer-uuid-1': {
            state: 'syncing',
            current_sync_snap: {
              name: 'snap-current',
              files: '100/200 (50%)',
              bytes: '1.5 GB/3.0 GB',
              eta_completion: '2026-06-24T10:00:00Z'
            },
            last_synced_snap: {
              id: 1,
              name: 'snap-last'
            },
            snaps_synced: 10,
            snaps_deleted: 2
          }
        }
      },
      '/path2': {
        peer: {
          'peer-uuid-2': {
            state: 'idle',
            current_sync_snap: {
              name: 'snap-current-2',
              files_synced: 50,
              total_files: 100,
              bytes_synced: 1073741824,
              total_bytes: 2147483648
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
      getSnapshotMirrorStatus: jest.fn()
    };

    const formatterServiceMock = {
      toBytes: jest.fn((value: string) => {
        const match = value.match(/([\d.]+)\s*(\w+)/);
        if (!match) return 0;
        const val = parseFloat(match[1]);
        const unit = match[2].toUpperCase();
        const multipliers: { [key: string]: number } = {
          B: 1,
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

  it('should initialize columns on init', () => {
    component.ngOnInit();
    expect(component.columns).toBeDefined();
    expect(component.columns.length).toBe(4);
    expect(component.columns[0].prop).toBe('path');
    expect(component.columns[1].prop).toBe('syncStatus');
    expect(component.columns[2].prop).toBe('currentSyncSnapshot');
    expect(component.columns[3].prop).toBe('lastSyncedSnapshot');
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

    it('should parse mirror status with string format files and bytes', () => {
      const result = component.parseMirrorStatus(mockSnapshotMirrorStatusResponse);

      expect(result.length).toBe(2);

      // Check first path
      expect(result[0].path).toBe('/path1');
      expect(result[0].syncStatus).toBe('syncing');
      expect(result[0].currentSyncSnapshot).toBe('snap-current');
      expect(result[0].currentSyncEta).toBe('2026-06-24T10:00:00Z');
      expect(result[0].lastSyncedSnapshot).toBe('snap-last');
      expect(result[0].snapshotCount).toBe(10);
      expect(result[0].checkpointCount).toBe(2);
      expect(result[0].filesSynced).toBe(100);
      expect(result[0].totalFiles).toBe(200);
      expect(result[0].syncProgress).toBe(50);
    });

    it('should parse mirror status with numeric format files and bytes', () => {
      const result = component.parseMirrorStatus(mockSnapshotMirrorStatusResponse);

      expect(result.length).toBe(2);

      // Check second path
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
      const dataWithoutPeer: SnapshotMirrorStatusResponse = {
        metrics: {
          '/path1': {} as any
        }
      };

      const result = component.parseMirrorStatus(dataWithoutPeer);
      expect(result).toEqual([]);
    });

    it('should use default values when optional fields are missing', () => {
      const minimalData: SnapshotMirrorStatusResponse = {
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

  describe('parseFileProgress', () => {
    it('should parse file progress from string format', () => {
      const result = component['parseFileProgress']('123/456 (78%)');

      expect(result.synced).toBe(123);
      expect(result.total).toBe(456);
      expect(result.progress).toBe(78);
    });

    it('should return zeros for non-string input', () => {
      const result = component['parseFileProgress'](undefined);

      expect(result.synced).toBe(0);
      expect(result.total).toBe(0);
      expect(result.progress).toBe(0);
    });

    it('should return zeros for invalid string format', () => {
      const result = component['parseFileProgress']('invalid');

      expect(result.synced).toBe(0);
      expect(result.total).toBe(0);
      expect(result.progress).toBe(0);
    });

    it('should parse file progress without percentage', () => {
      const result = component['parseFileProgress']('50/100');

      expect(result.synced).toBe(50);
      expect(result.total).toBe(100);
      expect(result.progress).toBe(0);
    });
  });

  describe('parseByteProgress', () => {
    it('should parse byte progress from string format', () => {
      const result = component['parseByteProgress']('1.5 GB/3.0 GB');

      expect(result.synced).toBeGreaterThan(0);
      expect(result.total).toBeGreaterThan(0);
      expect(formatterService.toBytes).toHaveBeenCalledWith('1.5GB', 0);
      expect(formatterService.toBytes).toHaveBeenCalledWith('3.0GB', 0);
    });

    it('should return zeros for non-string input', () => {
      const result = component['parseByteProgress'](undefined);

      expect(result.synced).toBe(0);
      expect(result.total).toBe(0);
    });

    it('should return zeros for invalid string format', () => {
      const result = component['parseByteProgress']('invalid');

      expect(result.synced).toBe(0);
      expect(result.total).toBe(0);
    });

    it('should handle different byte units', () => {
      component['parseByteProgress']('512 MB/1 GB');

      expect(formatterService.toBytes).toHaveBeenCalledWith('512MB', 0);
      expect(formatterService.toBytes).toHaveBeenCalledWith('1GB', 0);
    });
  });

  describe('calculateSyncProgress', () => {
    it('should return file progress when available', () => {
      const result = component['calculateSyncProgress'](75, 50, 100, 500, 1000);
      expect(result).toBe(75);
    });

    it('should calculate progress from file count when file progress is 0', () => {
      const result = component['calculateSyncProgress'](0, 50, 100, 0, 0);
      expect(result).toBe(50);
    });

    it('should calculate progress from byte count when file progress and count are 0', () => {
      const result = component['calculateSyncProgress'](0, 0, 0, 500, 1000);
      expect(result).toBe(50);
    });

    it('should return 0 when no progress data is available', () => {
      const result = component['calculateSyncProgress'](0, 0, 0, 0, 0);
      expect(result).toBe(0);
    });

    it('should round progress to nearest integer', () => {
      const result = component['calculateSyncProgress'](0, 33, 100, 0, 0);
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
      cephfsService.getSnapshotMirrorStatus.mockReturnValue(
        of(mockSnapshotMirrorStatusResponse)
      );
      component.fsName = 'test-fs';

      component.loadMirrorPaths();

      expect(cephfsService.getSnapshotMirrorStatus).toHaveBeenCalledWith('test-fs');
      expect(component.mirrorPaths.length).toBe(2);
    });

    it('should set empty array on error', () => {
      cephfsService.getSnapshotMirrorStatus.mockReturnValue(
        throwError(() => new Error('API Error'))
      );
      component.fsName = 'test-fs';

      component.loadMirrorPaths();

      expect(component.mirrorPaths).toEqual([]);
    });

    it('should not call service when fsName is empty', () => {
      component.fsName = '';

      component.loadMirrorPaths();

      expect(cephfsService.getSnapshotMirrorStatus).not.toHaveBeenCalled();
    });
  });

  describe('getSyncStatusIcon', () => {
    it('should return correct icon for syncing status', () => {
      const icon = component.getSyncStatusIcon('syncing');
      expect(icon).toBeDefined();
    });

    it('should return correct icon for idle status', () => {
      const icon = component.getSyncStatusIcon('idle');
      expect(icon).toBeDefined();
    });

    it('should return correct icon for failed status', () => {
      const icon = component.getSyncStatusIcon('failed');
      expect(icon).toBeDefined();
    });

    it('should return default icon for unknown status', () => {
      const icon = component.getSyncStatusIcon('unknown');
      expect(icon).toBeDefined();
    });
  });

  describe('getSyncStatusClass', () => {
    it('should return correct class for syncing status', () => {
      const cssClass = component.getSyncStatusClass('syncing');
      expect(cssClass).toBe('text-info');
    });

    it('should return correct class for idle status', () => {
      const cssClass = component.getSyncStatusClass('idle');
      expect(cssClass).toBe('text-muted');
    });

    it('should return correct class for failed status', () => {
      const cssClass = component.getSyncStatusClass('failed');
      expect(cssClass).toBe('text-danger');
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

      component.onSelectionChange(mockSelection);

      expect(component.selection).toBe(mockSelection);
    });
  });
});

// Made with Bob
