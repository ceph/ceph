import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { of, throwError } from 'rxjs';

import { MirroringScheduleConflictComponent } from './mirroring-schedule-conflict.component';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';

describe('MirroringScheduleConflictComponent', () => {
  let component: MirroringScheduleConflictComponent;
  let fixture: ComponentFixture<MirroringScheduleConflictComponent>;

  const snapshotScheduleServiceMock = {
    getSnapshotSchedule: jest.fn(),
    scheduleMatchesPath: jest.fn(),
    parseScheduleCopy: jest.fn((schedule: string) => schedule)
  };

  const mockSchedules = [
    {
      path: '/internal/path',
      rel_path: '/volumes/g1/sv1',
      schedule: '2w',
      start: '2024-01-01T00:00:00Z',
      retention: { w: 2 },
      active: true,
      fs: 'testfs',
      subvol: 'sv1',
      group: 'g1'
    }
  ];

  function setInputs(fsName: string, paths: string[]): void {
    fixture.componentRef.setInput('fsName', fsName);
    fixture.componentRef.setInput('paths', paths);
    fixture.detectChanges();
    tick();
  }

  beforeEach(async () => {
    jest.clearAllMocks();
    snapshotScheduleServiceMock.getSnapshotSchedule.mockReturnValue(of(mockSchedules));
    snapshotScheduleServiceMock.scheduleMatchesPath.mockImplementation(
      (_schedule: { rel_path?: string }, targetPath: string) =>
        targetPath === '/volumes/g1/sv1'
    );

    await TestBed.configureTestingModule({
      declarations: [MirroringScheduleConflictComponent],
      providers: [
        { provide: CephfsSnapshotScheduleService, useValue: snapshotScheduleServiceMock }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    })
      .overrideComponent(MirroringScheduleConflictComponent, {
        set: { template: '' }
      })
      .compileComponents();

    fixture = TestBed.createComponent(MirroringScheduleConflictComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  it('should initialise with default values', () => {
    expect(component.existingSchedules).toEqual([]);
    expect(component.scheduleConflictAction).toBe('keep');
    expect(component.loading).toBe(false);
  });

  it('should load existing schedules for selected paths', fakeAsync(() => {
    const emitSpy = jest.spyOn(component.existingSchedulesChange, 'emit');

    setInputs('testfs', ['/volumes/g1/sv1']);

    expect(snapshotScheduleServiceMock.getSnapshotSchedule).toHaveBeenCalledWith(
      '/',
      'testfs',
      true
    );
    expect(component.existingSchedules).toHaveLength(1);
    expect(component.existingSchedules[0].path).toBe('/volumes/g1/sv1');
    expect(component.existingSchedules[0].existingSchedule).toBe('2w');
    expect(emitSpy).toHaveBeenCalledWith(component.existingSchedules);
    expect(component.loading).toBe(false);
  }));

  it('should clear schedules when paths are empty', fakeAsync(() => {
    const emitSpy = jest.spyOn(component.existingSchedulesChange, 'emit');

    setInputs('testfs', []);

    expect(snapshotScheduleServiceMock.getSnapshotSchedule).not.toHaveBeenCalled();
    expect(component.existingSchedules).toEqual([]);
    expect(emitSpy).toHaveBeenCalledWith([]);
  }));

  it('should handle API errors when loading schedules', fakeAsync(() => {
    snapshotScheduleServiceMock.getSnapshotSchedule.mockReturnValue(
      throwError(() => new Error('API error'))
    );
    const emitSpy = jest.spyOn(component.existingSchedulesChange, 'emit');

    setInputs('testfs', ['/volumes/g1/sv1']);

    expect(component.existingSchedules).toEqual([]);
    expect(emitSpy).toHaveBeenCalledWith([]);
    expect(component.loading).toBe(false);
  }));

  it('should emit conflict action changes', () => {
    const emitSpy = jest.spyOn(component.conflictActionChange, 'emit');

    component.onConflictActionChange({ value: 'replace' });

    expect(component.scheduleConflictAction).toBe('replace');
    expect(emitSpy).toHaveBeenCalledWith('replace');
  });

  it('should update row action and emit schedules', fakeAsync(() => {
    const emitSpy = jest.spyOn(component.existingSchedulesChange, 'emit');

    setInputs('testfs', ['/volumes/g1/sv1']);

    component.setRowAction('/volumes/g1/sv1@2w', 'replace');

    expect(component.existingSchedules[0].action).toBe('replace');
    expect(emitSpy).toHaveBeenCalledWith(component.existingSchedules);
  }));
});
