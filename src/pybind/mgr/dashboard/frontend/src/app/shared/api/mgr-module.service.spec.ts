import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing';
import { of as observableOf, throwError as observableThrowError } from 'rxjs';

import { configureTestBed } from '~/testing/unit-test-helper';
import { MgrModuleService } from './mgr-module.service';
import { CdTableSelection } from '../models/cd-table-selection';
import { NotificationService } from '~/app/shared/services/notification.service';
import { MgrModuleListComponent } from '~/app/ceph/cluster/mgr-modules/mgr-module-list/mgr-module-list.component';
import { ToastrModule } from 'ngx-toastr';
import { SharedModule } from '../shared.module';
import { BlockUIService } from 'ng-block-ui';

describe('MgrModuleService', () => {
  let service: MgrModuleService;
  let httpTesting: HttpTestingController;
  let blockUIService: BlockUIService;

  configureTestBed({
    declarations: [MgrModuleListComponent],
    imports: [HttpClientTestingModule, SharedModule, ToastrModule.forRoot()],
    providers: [MgrModuleService]
  });

  beforeEach(() => {
    service = TestBed.inject(MgrModuleService);
    httpTesting = TestBed.inject(HttpTestingController);
    blockUIService = TestBed.inject(BlockUIService);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list', () => {
    service.list().subscribe();
    const req = httpTesting.expectOne('api/mgr/module');
    expect(req.request.method).toBe('GET');
  });

  it('should call getConfig', () => {
    service.getConfig('foo').subscribe();
    const req = httpTesting.expectOne('api/mgr/module/foo');
    expect(req.request.method).toBe('GET');
  });

  it('should call updateConfig', () => {
    const config = { foo: 'bar' };
    service.updateConfig('xyz', config).subscribe();
    const req = httpTesting.expectOne('api/mgr/module/xyz');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body.config).toEqual(config);
  });

  it('should call enable', () => {
    service.enable('foo').subscribe();
    const req = httpTesting.expectOne('api/mgr/module/foo/enable');
    expect(req.request.method).toBe('POST');
  });

  it('should call disable', () => {
    service.disable('bar').subscribe();
    const req = httpTesting.expectOne('api/mgr/module/bar/disable');
    expect(req.request.method).toBe('POST');
  });

  it('should call getOptions', () => {
    service.getOptions('foo').subscribe();
    const req = httpTesting.expectOne('api/mgr/module/foo/options');
    expect(req.request.method).toBe('GET');
  });

  describe('should update module state', () => {
    let component: MgrModuleListComponent;
    let notificationService: NotificationService;
    let fixture: ComponentFixture<MgrModuleListComponent>;

    beforeEach(() => {
      fixture = TestBed.createComponent(MgrModuleListComponent);
      component = fixture.componentInstance;
      notificationService = TestBed.inject(NotificationService);

      component.selection = new CdTableSelection();
      spyOn(notificationService, 'suspendToasties');
      spyOn(blockUIService, 'start');
      spyOn(blockUIService, 'stop');
    });

    it('should enable module', fakeAsync(() => {
      spyOn(service, 'enable').and.returnValue(observableThrowError('y'));
      spyOn(service, 'list').and.returnValues(observableThrowError('z'), observableOf([]));
      component.selection.add({
        name: 'foo',
        enabled: false,
        always_on: false
      });
      const selected = component.selection.first();
      service.updateModuleState(selected.name, selected.enabled);
      tick(service.REFRESH_INTERVAL);
      tick(service.REFRESH_INTERVAL);
      expect(service.enable).toHaveBeenCalledWith('foo');
      expect(service.list).toHaveBeenCalledTimes(2);
      expect(notificationService.suspendToasties).toHaveBeenCalledTimes(2);
      expect(blockUIService.start).toHaveBeenCalled();
      expect(blockUIService.stop).toHaveBeenCalled();
    }));

    it('should disable module', fakeAsync(() => {
      spyOn(service, 'disable').and.returnValue(observableThrowError('x'));
      spyOn(service, 'list').and.returnValue(observableOf([]));
      component.selection.add({
        name: 'bar',
        enabled: true,
        always_on: false
      });
      const selected = component.selection.first();
      service.updateModuleState(selected.name, selected.enabled);
      tick(service.REFRESH_INTERVAL);
      expect(service.disable).toHaveBeenCalledWith('bar');
      expect(service.list).toHaveBeenCalledTimes(1);
      expect(notificationService.suspendToasties).toHaveBeenCalledTimes(2);
      expect(blockUIService.start).toHaveBeenCalled();
      expect(blockUIService.stop).toHaveBeenCalled();
    }));

    it('should not disable module without selecting one', () => {
      expect(component.getTableActionDisabledDesc()).toBeTruthy();
    });

    it('should not disable dashboard module', () => {
      component.selection.selected = [
        {
          name: 'dashboard'
        }
      ];
      expect(component.getTableActionDisabledDesc()).toBeTruthy();
    });

    it('should not disable an always-on module', () => {
      component.selection.selected = [
        {
          name: 'bar',
          always_on: true
        }
      ];
      expect(component.getTableActionDisabledDesc()).toBe('This Manager module is always on.');
    });
  });
});
