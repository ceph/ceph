import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, Router } from '@angular/router';
import { of } from 'rxjs';

import { CephfsMirroringPathsComponent } from './cephfs-mirroring-paths.component';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RouterTestingModule } from '@angular/router/testing';

describe('CephfsMirroringPathsComponent', () => {
  let component: CephfsMirroringPathsComponent;
  let fixture: ComponentFixture<CephfsMirroringPathsComponent>;
  let listSnapshotDirsSpy: jest.SpyInstance;

  const createActivatedRoute = (params: { fsName?: string } = {}) => ({
    snapshot: { params }
  });

  const createRouterWithState = (state: Record<string, string> | null) => ({
    getCurrentNavigation: () => (state ? { extras: { state } } : null)
  });

  const cephfsServiceMock = {
    listSnapshotDirs: jest.fn()
  };

  configureTestBed({
    declarations: [CephfsMirroringPathsComponent],
    imports: [RouterTestingModule],
    providers: [
      { provide: ActivatedRoute, useValue: createActivatedRoute({ fsName: 'fs1' }) },
      { provide: Router, useValue: createRouterWithState(null) },
      { provide: CephfsService, useValue: cephfsServiceMock }
    ]
  });

  function createComponent() {
    fixture = TestBed.createComponent(CephfsMirroringPathsComponent);
    component = fixture.componentInstance;
    listSnapshotDirsSpy = cephfsServiceMock.listSnapshotDirs;
    return fixture;
  }

  beforeEach(() => {
    jest.clearAllMocks();
    cephfsServiceMock.listSnapshotDirs.mockReturnValue(of(['/']));
    createComponent();
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should set localFsName from route params on init', () => {
    fixture.detectChanges();
    expect(component.localFsName).toBe('fs1');
  });

  it('should call listSnapshotDirs with localFsName', () => {
    fixture.detectChanges();
    expect(listSnapshotDirsSpy).toHaveBeenCalledWith('fs1');
  });

  it('should map snapshot dirs to table rows and expose via mirroringPaths$', (done) => {
    listSnapshotDirsSpy.mockReturnValue(of(['/path1', '/path2']));
    fixture.detectChanges();
    component.mirroringPaths$.subscribe((rows) => {
      expect(rows.length).toBe(2);
      expect(rows[0]).toEqual({
        path: '/path1',
        type: '',
        status: '',
        snapshot_schedule: '',
        last_sync: ''
      });
      expect(rows[1].path).toBe('/path2');
      done();
    });
  });

  it('should display localFsName, remoteFsName and remoteClusterName in the template', () => {
    fixture.detectChanges();
    const el = fixture.nativeElement as HTMLElement;
    expect(el.textContent).toContain('fs1');
    expect(el.textContent).toContain('-');
    expect(el.textContent).toContain('Local filesystem');
    expect(el.textContent).toContain('Remote filesystem');
    expect(el.textContent).toContain('Remote cluster');
    expect(el.textContent).toContain('Mirroring paths');
  });
});
