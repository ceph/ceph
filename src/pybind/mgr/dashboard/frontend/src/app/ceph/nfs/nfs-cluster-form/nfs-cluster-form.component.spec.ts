import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { of } from 'rxjs';

import {
  CheckboxModule,
  ComboBoxModule,
  GridModule,
  InputModule,
  NumberModule,
  RadioModule,
  SelectModule
} from 'carbon-components-angular';

import { NfsClusterFormComponent } from './nfs-cluster-form.component';
import { HostService } from '~/app/shared/api/host.service';
import { NfsService } from '~/app/shared/api/nfs.service';
import { SharedModule } from '~/app/shared/shared.module';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('NfsClusterFormComponent', () => {
  let component: NfsClusterFormComponent;
  let fixture: ComponentFixture<NfsClusterFormComponent>;
  let httpTesting: HttpTestingController;
  let nfsService: NfsService;
  let taskWrapper: TaskWrapperService;
  let router: Router;

  configureTestBed({
    declarations: [NfsClusterFormComponent],
    imports: [
      BrowserAnimationsModule,
      HttpClientTestingModule,
      ReactiveFormsModule,
      RouterTestingModule,
      SharedModule,
      GridModule,
      InputModule,
      SelectModule,
      ComboBoxModule,
      NumberModule,
      CheckboxModule,
      RadioModule
    ]
  });

  function setup(editing = false, clusterId?: string) {
    nfsService = TestBed.inject(NfsService);
    spyOn(nfsService, 'listClusters').and.returnValue(of([] as any));

    if (editing) {
      spyOn(nfsService, 'getCluster').and.returnValue(
        of({
          name: clusterId,
          virtual_ip: null,
          port: 2049,
          backend: [],
          placement: { count: 2, hosts: ['node1'] },
          enable_rdma: false
        } as any)
      );
    }

    const hostService = TestBed.inject(HostService);
    spyOn(hostService, 'getAllHosts').and.returnValue(
      of([{ hostname: 'node1' }, { hostname: 'node2' }])
    );
    spyOn(hostService, 'getLabels').and.returnValue(of(['nfs', 'ingress']));

    const route = TestBed.inject(ActivatedRoute) as any;
    route.snapshot = {
      data: { editing },
      params: { cluster_id: clusterId }
    };

    fixture = TestBed.createComponent(NfsClusterFormComponent);
    component = fixture.componentInstance;
    httpTesting = TestBed.inject(HttpTestingController);
    taskWrapper = TestBed.inject(TaskWrapperService);
    router = TestBed.inject(Router);
    fixture.detectChanges();
  }

  beforeEach(() => {
    setup();
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize the form with defaults', () => {
    expect(component.nfsForm.get('protocol_version').value).toBe('nfsv4');
    expect(component.nfsForm.get('port').value).toBe(2049);
    expect(component.nfsForm.get('monitoring_port').value).toBe(9587);
    expect(component.nfsForm.get('ingress').value).toBe(false);
    expect(component.nfsForm.get('ingress_mode').value).toBe('haproxy-protocol');
    expect(component.nfsForm.get('ingress_placement_mode').value).toBe('nfs');
    expect(component.nfsForm.get('placement').value).toBe('auto');
  });

  it('should require a valid cluster name format', () => {
    const clusterId = component.nfsForm.get('cluster_id');
    clusterId.setValue('');
    expect(clusterId.hasError('required')).toBeTruthy();

    clusterId.setValue('Bad Name');
    expect(clusterId.hasError('pattern')).toBeTruthy();
  });

  it('should mark cluster_id as notUnique when cluster already exists', fakeAsync(() => {
    spyOn(nfsService, 'exists').and.returnValue(of(true));
    const clusterId = component.nfsForm.get('cluster_id');
    clusterId.markAsDirty();
    clusterId.setValue('existing-cluster');
    tick(500); // DUE_TIMER debounce in CdValidators.unique
    expect(clusterId.hasError('notUnique')).toBeTruthy();
  }));

  it('should accept cluster_id when name is available', fakeAsync(() => {
    spyOn(nfsService, 'exists').and.returnValue(of(false));
    const clusterId = component.nfsForm.get('cluster_id');
    clusterId.markAsDirty();
    clusterId.setValue('nfs-01');
    tick(500);
    expect(clusterId.valid).toBeTruthy();
  }));

  it('should add and remove public networks', () => {
    const initialLength = component.networks.length;
    component.addNetwork();
    expect(component.networks.length).toBe(initialLength + 1);
    component.removeNetwork(1);
    expect(component.networks.length).toBe(initialLength);
  });

  it('should add and remove bind addresses', () => {
    const initialLength = component.bind_addrs.length;
    component.addBindAddr();
    expect(component.bind_addrs.length).toBe(initialLength + 1);
    component.removeBindAddr(1);
    expect(component.bind_addrs.length).toBe(initialLength);
  });

  it('should require virtual IP when ingress is enabled', () => {
    const virtualIp = component.nfsForm.get('virtual_ip');
    component.nfsForm.get('ingress').setValue(true);
    virtualIp.setValue('');
    virtualIp.updateValueAndValidity();
    expect(virtualIp.valid).toBeFalsy();

    virtualIp.setValue('10.20.0.100/24');
    virtualIp.updateValueAndValidity();
    expect(virtualIp.valid).toBeTruthy();
  });

  it('should submit a create cluster request', () => {
    spyOn(nfsService, 'createCluster').and.returnValue(of({} as any));
    spyOn(taskWrapper, 'wrapTaskAroundCall').and.callFake((opts: any) => opts.call);
    spyOn(router, 'navigate');

    component.nfsForm.get('cluster_id').setValue('nfs-01');
    component.nfsForm.get('port').setValue(2049);
    component.nfsForm.get('placement').setValue('hosts');
    component.selectedHosts = ['node1'];
    component.networks.at(0).setValue('10.20.0.0/24');
    component.submitAction();

    expect(taskWrapper.wrapTaskAroundCall).toHaveBeenCalled();
    expect(nfsService.createCluster).toHaveBeenCalled();
    const payload = (nfsService.createCluster as jasmine.Spy).calls.mostRecent().args[0];
    expect(payload.cluster_id).toBe('nfs-01');
    expect(payload.port).toBe(2049);
    expect(payload.networks).toEqual(['10.20.0.0/24']);
    expect(payload.placement.hosts).toEqual(['node1']);
  });

  it('should include ingress details when enabled', () => {
    spyOn(nfsService, 'createCluster').and.returnValue(of({} as any));
    spyOn(taskWrapper, 'wrapTaskAroundCall').and.callFake((opts: any) => opts.call);

    component.nfsForm.get('cluster_id').setValue('nfs-ha');
    component.nfsForm.get('ingress').setValue(true);
    component.nfsForm.get('virtual_ip').setValue('10.20.0.100/24');
    component.nfsForm.get('ingress_mode').setValue('haproxy-protocol');
    component.nfsForm.get('ingress_placement_mode').setValue('custom');
    component.selectedIngressHosts = ['node2'];
    component.submitAction();

    const payload = (nfsService.createCluster as jasmine.Spy).calls.mostRecent().args[0];
    expect(payload.ingress).toBe(true);
    expect(payload.virtual_ip).toBe('10.20.0.100/24');
    expect(payload.ingress_mode).toBe('haproxy-protocol');
    expect(payload.ingress_placement.hosts).toEqual(['node2']);
  });

  it('should enable nfsv3 when protocol version is nfsv3', () => {
    spyOn(nfsService, 'createCluster').and.returnValue(of({} as any));
    spyOn(taskWrapper, 'wrapTaskAroundCall').and.callFake((opts: any) => opts.call);

    component.nfsForm.get('cluster_id').setValue('nfs-v3');
    component.nfsForm.get('protocol_version').setValue('nfsv3');
    component.submitAction();

    const payload = (nfsService.createCluster as jasmine.Spy).calls.mostRecent().args[0];
    expect(payload.enable_nfsv3).toBe(true);
  });

  it('should include rdma and monitoring settings', () => {
    spyOn(nfsService, 'createCluster').and.returnValue(of({} as any));
    spyOn(taskWrapper, 'wrapTaskAroundCall').and.callFake((opts: any) => opts.call);

    component.nfsForm.get('cluster_id').setValue('nfs-rdma');
    component.nfsForm.get('enable_rdma').setValue(true);
    component.nfsForm.get('rdma_port').setValue(20049);
    component.submitAction();

    const payload = (nfsService.createCluster as jasmine.Spy).calls.mostRecent().args[0];
    expect(payload.enable_rdma).toBe(true);
    expect(payload.rdma_port).toBe(20049);
    expect(payload.monitoring_port).toBe(9587);
  });

  it('should extract label string from combobox object in placement spec', () => {
    spyOn(nfsService, 'createCluster').and.returnValue(of({} as any));
    spyOn(taskWrapper, 'wrapTaskAroundCall').and.callFake((opts: any) => opts.call);

    component.nfsForm.get('cluster_id').setValue('nfs-label');
    component.nfsForm.get('placement').setValue('custom');
    component.nfsForm.get('custom_placement_type').setValue('label');
    component.nfsForm.get('label').setValue({ content: '_admin', selected: true });
    component.submitAction();

    const payload = (nfsService.createCluster as jasmine.Spy).calls.mostRecent().args[0];
    expect(payload.placement.label).toBe('_admin');
  });

  describe('edit mode', () => {
    beforeEach(() => {
      // Re-configure the already-spied listClusters and add getCluster spy
      (nfsService.listClusters as jasmine.Spy).and.returnValue(of([] as any));
      spyOn(nfsService, 'getCluster').and.returnValue(
        of({
          name: 'nfs-01',
          ingress: false,
          virtual_ip: null,
          port: 2049,
          backend: [],
          placement: { count: 2, hosts: ['node1'] },
          enable_rdma: false
        } as any)
      );

      const route = TestBed.inject(ActivatedRoute) as any;
      route.snapshot = { data: { editing: true }, params: { cluster_id: 'nfs-01' } };

      fixture = TestBed.createComponent(NfsClusterFormComponent);
      component = fixture.componentInstance;
      fixture.detectChanges();
    });

    it('should set action to EDIT', () => {
      expect(component.isEdit).toBe(true);
      expect(component.clusterId).toBe('nfs-01');
    });

    it('should disable cluster_id when editing', () => {
      expect(component.nfsForm.get('cluster_id').disabled).toBe(true);
    });

    it('should populate form from cluster data', () => {
      expect(component.nfsForm.get('count').value).toBe(2);
      expect(component.nfsForm.get('placement').value).toBe(component.PLACEMENT.host);
      expect(component.selectedHosts).toEqual(['node1']);
    });

    it('should call updateCluster on submit', () => {
      spyOn(nfsService, 'updateCluster').and.returnValue(of({} as any));
      spyOn(taskWrapper, 'wrapTaskAroundCall').and.callFake((opts: any) => opts.call);
      spyOn(router, 'navigate');

      component.submitAction();

      expect(nfsService.updateCluster).toHaveBeenCalledWith('nfs-01', jasmine.any(Object));
      expect(router.navigate).toHaveBeenCalledWith(['/cephfs/nfs']);
    });
  });
});
