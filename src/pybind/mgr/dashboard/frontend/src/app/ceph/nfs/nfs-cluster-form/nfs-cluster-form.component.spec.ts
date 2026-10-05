import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
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
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
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

  beforeEach(() => {
    const hostService = TestBed.inject(HostService);
    spyOn(hostService, 'getAllHosts').and.returnValue(
      of([{ hostname: 'node1' }, { hostname: 'node2' }])
    );
    spyOn(hostService, 'getLabels').and.returnValue(of(['nfs', 'ingress']));

    const orchService = TestBed.inject(OrchestratorService);
    spyOn(orchService, 'status').and.returnValue(of({ available: true, message: null }));

    fixture = TestBed.createComponent(NfsClusterFormComponent);
    component = fixture.componentInstance;
    httpTesting = TestBed.inject(HttpTestingController);
    nfsService = TestBed.inject(NfsService);
    taskWrapper = TestBed.inject(TaskWrapperService);
    router = TestBed.inject(Router);
    fixture.detectChanges();
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
    expect(component.nfsForm.get('ingress_mode').value).toBe('haproxy-standard');
    expect(component.nfsForm.get('ingress_placement_mode').value).toBe('nfs');
    expect(component.nfsForm.get('placement').value).toBe('hosts');
  });

  it('should require a valid cluster name', () => {
    const clusterId = component.nfsForm.get('cluster_id');
    clusterId.setValue('');
    expect(clusterId.valid).toBeFalsy();

    clusterId.setValue('Bad Name');
    expect(clusterId.hasError('pattern')).toBeTruthy();

    clusterId.setValue('nfs-01');
    expect(clusterId.valid).toBeTruthy();
  });

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

  it('should add and remove monitoring addresses', () => {
    const initialLength = component.monitoring_addrs.length;
    component.addMonitoringAddr();
    expect(component.monitoring_addrs.length).toBe(initialLength + 1);
    component.removeMonitoringAddr(1);
    expect(component.monitoring_addrs.length).toBe(initialLength);
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
    component.nfsForm.get('ingress_placement').setValue('hosts');
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
    component.monitoring_addrs.at(0).patchValue({ hostname: 'node1', ip: '192.168.1.20' });
    component.submitAction();

    const payload = (nfsService.createCluster as jasmine.Spy).calls.mostRecent().args[0];
    expect(payload.enable_rdma).toBe(true);
    expect(payload.rdma_port).toBe(20049);
    expect(payload.monitoring_addrs).toEqual([{ hostname: 'node1', ip: '192.168.1.20' }]);
    expect(payload.monitoring_port).toBe(9587);
  });
});
