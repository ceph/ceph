import { HttpResponse } from '@angular/common/http';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { Observable, of } from 'rxjs';

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
import { IngressMode } from '../nfs.constant';

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
    nfsService = TestBed.inject(NfsService);
    spyOn(nfsService, 'listClusters').and.returnValue(of([] as string[]));

    const hostService = TestBed.inject(HostService);
    spyOn(hostService, 'getAllHosts').and.returnValue(
      of([{ hostname: 'node1' }, { hostname: 'node2' }])
    );
    spyOn(hostService, 'getLabels').and.returnValue(of(['nfs', 'ingress']));

    fixture = TestBed.createComponent(NfsClusterFormComponent);
    component = fixture.componentInstance;
    httpTesting = TestBed.inject(HttpTestingController);
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
    expect(component.nfsForm.get('ingress_mode').value).toBe(IngressMode.HaproxyProtocol);
    expect(component.nfsForm.get('ingress_placement_mode').value).toBe('nfs');
    expect(component.nfsForm.get('placement').value).toBe('auto');
  });

  it('should require a valid cluster name', () => {
    const clusterId = component.nfsForm.get('cluster_id');
    clusterId.setValue('');
    expect(clusterId.valid).toBeFalsy();

    clusterId.setValue('Bad Name');
    expect(clusterId.hasError('clusterIdPattern')).toBeTruthy();

    clusterId.setValue('nfs-');
    expect(clusterId.hasError('clusterIdPattern')).toBeTruthy();

    clusterId.setValue('nfs-01');
    expect(clusterId.valid).toBeTruthy();
  });

  it('should reject invalid network ports', () => {
    ['port', 'rdma_port', 'monitoring_port'].forEach((controlName) => {
      const control = component.nfsForm.get(controlName);

      control.setValue(-9);
      expect(control.hasError('min')).toBeTruthy();

      control.setValue(0);
      expect(control.hasError('min')).toBeTruthy();

      control.setValue(70000);
      expect(control.hasError('max')).toBeTruthy();
    });

    expect(component.nfsForm.get('port').valid).toBeFalsy();
    component.nfsForm.get('port').setValue(2049);
    component.nfsForm.get('rdma_port').setValue(20049);
    component.nfsForm.get('monitoring_port').setValue(9587);
    expect(component.nfsForm.get('port').valid).toBeTruthy();
    expect(component.nfsForm.get('rdma_port').valid).toBeTruthy();
    expect(component.nfsForm.get('monitoring_port').valid).toBeTruthy();
  });

  it('should pre-select the only host and label', () => {
    const host = { content: 'ceph-node-00', selected: true };
    const label = { content: 'nfs', selected: true };
    component.hostItems = [host];
    component.labelItems = [label];
    component.createForm();

    expect(component.nfsForm.get('hosts').value).toEqual([host]);
    expect(component.selectedHosts).toEqual(['ceph-node-00']);
    expect(component.bind_addrs.at(0).get('hostname').value).toEqual(host);
    expect(component.nfsForm.get('ingress_hosts').value).toEqual([host]);
    expect(component.selectedIngressHosts).toEqual(['ceph-node-00']);
    expect(component.nfsForm.get('label').value).toEqual(label);
    expect(host.selected).toBe(true);
    expect(label.selected).toBe(true);
  });

  it('should not pre-select when multiple hosts or labels exist', () => {
    expect(component.nfsForm.get('hosts').value).toEqual([]);
    expect(component.selectedHosts).toEqual([]);
    expect(component.bind_addrs.at(0).get('hostname').value).toBe('');
    expect(component.nfsForm.get('ingress_hosts').value).toEqual([]);
    expect(component.nfsForm.get('label').value).toBeNull();
  });

  it('should pre-select from the host service when only one host and label exist', () => {
    const hostService = TestBed.inject(HostService);
    (hostService.getAllHosts as jasmine.Spy).and.returnValue(of([{ hostname: 'ceph-node-00' }]));
    (hostService.getLabels as jasmine.Spy).and.returnValue(of(['nfs']));

    fixture = TestBed.createComponent(NfsClusterFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();

    expect(component.hostItems).toEqual([{ content: 'ceph-node-00', selected: true }]);
    expect(component.labelItems).toEqual([{ content: 'nfs', selected: true }]);
    expect(component.nfsForm.get('hosts').value).toEqual([
      { content: 'ceph-node-00', selected: true }
    ]);
    expect(component.selectedHosts).toEqual(['ceph-node-00']);
    expect(component.bind_addrs.at(0).get('hostname').value).toEqual({
      content: 'ceph-node-00',
      selected: true
    });
    expect(component.nfsForm.get('ingress_hosts').value).toEqual([
      { content: 'ceph-node-00', selected: true }
    ]);
    expect(component.nfsForm.get('label').value).toEqual({ content: 'nfs', selected: true });
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
    spyOn(nfsService, 'createCluster').and.returnValue(of(new HttpResponse<null>()));
    spyOn(taskWrapper, 'wrapTaskAroundCall').and.callFake(
      (opts: { call: Observable<HttpResponse<null>> }) => opts.call
    );
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
    spyOn(nfsService, 'createCluster').and.returnValue(of(new HttpResponse<null>()));
    spyOn(taskWrapper, 'wrapTaskAroundCall').and.callFake(
      (opts: { call: Observable<HttpResponse<null>> }) => opts.call
    );

    component.nfsForm.get('cluster_id').setValue('nfs-ha');
    component.nfsForm.get('ingress').setValue(true);
    component.nfsForm.get('virtual_ip').setValue('10.20.0.100/24');
    component.nfsForm.get('ingress_mode').setValue(IngressMode.HaproxyProtocol);
    component.nfsForm.get('ingress_placement_mode').setValue('custom');
    component.selectedIngressHosts = ['node2'];
    component.submitAction();

    const payload = (nfsService.createCluster as jasmine.Spy).calls.mostRecent().args[0];
    expect(payload.ingress).toBe(true);
    expect(payload.virtual_ip).toBe('10.20.0.100/24');
    expect(payload.ingress_mode).toBe(IngressMode.HaproxyProtocol);
    expect(payload.ingress_placement.hosts).toEqual(['node2']);
  });

  it('should support selecting different ingress modes', () => {
    spyOn(nfsService, 'createCluster').and.returnValue(of(new HttpResponse<null>()));
    spyOn(taskWrapper, 'wrapTaskAroundCall').and.callFake(
      (opts: { call: Observable<HttpResponse<null>> }) => opts.call
    );

    component.nfsForm.get('cluster_id').setValue('nfs-ha-standard');
    component.nfsForm.get('ingress').setValue(true);
    component.nfsForm.get('virtual_ip').setValue('10.20.0.100/24');
    component.nfsForm.get('ingress_mode').setValue(IngressMode.HaproxyStandard);
    component.submitAction();

    let payload = (nfsService.createCluster as jasmine.Spy).calls.mostRecent().args[0];
    expect(payload.ingress_mode).toBe(IngressMode.HaproxyStandard);

    component.nfsForm.get('ingress_mode').setValue(IngressMode.KeepaliveOnly);
    component.submitAction();

    payload = (nfsService.createCluster as jasmine.Spy).calls.mostRecent().args[0];
    expect(payload.ingress_mode).toBe(IngressMode.KeepaliveOnly);
  });

  it('should exclude networks and bind_addrs from the payload when keepalive-only is selected', () => {
    spyOn(nfsService, 'createCluster').and.returnValue(of(new HttpResponse<null>()));
    spyOn(taskWrapper, 'wrapTaskAroundCall').and.callFake(
      (opts: { call: Observable<HttpResponse<null>> }) => opts.call
    );

    component.nfsForm.get('cluster_id').setValue('nfs-ko');
    component.nfsForm.get('ingress').setValue(true);
    component.nfsForm.get('virtual_ip').setValue('10.20.0.100/24');
    component.nfsForm.get('ingress_mode').setValue(IngressMode.KeepaliveOnly);
    // networks and bind_addrs are left at defaults (empty) so the form is valid
    component.submitAction();

    const payload = (nfsService.createCluster as jasmine.Spy).calls.mostRecent().args[0];
    expect(payload.networks).toBeUndefined();
    expect(payload.bind_addrs).toBeUndefined();
  });

  it('should enable nfsv3 when protocol version is nfsv3', () => {
    spyOn(nfsService, 'createCluster').and.returnValue(of(new HttpResponse<null>()));
    spyOn(taskWrapper, 'wrapTaskAroundCall').and.callFake(
      (opts: { call: Observable<HttpResponse<null>> }) => opts.call
    );

    component.nfsForm.get('cluster_id').setValue('nfs-v3');
    component.nfsForm.get('protocol_version').setValue('nfsv3');
    component.submitAction();

    const payload = (nfsService.createCluster as jasmine.Spy).calls.mostRecent().args[0];
    expect(payload.enable_nfsv3).toBe(true);
  });

  it('should include rdma and monitoring settings', () => {
    spyOn(nfsService, 'createCluster').and.returnValue(of(new HttpResponse<null>()));
    spyOn(taskWrapper, 'wrapTaskAroundCall').and.callFake(
      (opts: { call: Observable<HttpResponse<null>> }) => opts.call
    );

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
    spyOn(nfsService, 'createCluster').and.returnValue(of(new HttpResponse<null>()));
    spyOn(taskWrapper, 'wrapTaskAroundCall').and.callFake(
      (opts: { call: Observable<HttpResponse<null>> }) => opts.call
    );

    component.nfsForm.get('cluster_id').setValue('nfs-label');
    component.nfsForm.get('placement').setValue('custom');
    component.nfsForm.get('custom_placement_type').setValue('label');
    component.nfsForm.get('label').setValue({ content: '_admin', selected: true });
    component.submitAction();

    const payload = (nfsService.createCluster as jasmine.Spy).calls.mostRecent().args[0];
    expect(payload.placement.label).toBe('_admin');
  });
});
