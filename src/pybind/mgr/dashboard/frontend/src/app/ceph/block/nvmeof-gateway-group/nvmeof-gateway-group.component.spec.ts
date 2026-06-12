import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { NvmeofGatewayGroupComponent } from './nvmeof-gateway-group.component';
import { GridModule, TabsModule, ModalModule } from 'carbon-components-angular';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { Observable, of, Subject } from 'rxjs';
import { HttpClientModule } from '@angular/common/http';
import { SharedModule } from '~/app/shared/shared.module';
import { provideAnimations } from '@angular/platform-browser/animations';
import { provideToastr } from 'ngx-toastr';
import { Router } from '@angular/router';
import { ModalCdsService } from '~/app/shared/services/modal-cds.service';
import { DeleteConfirmationModalComponent } from '~/app/shared/components/delete-confirmation-modal/delete-confirmation-modal.component';
import { NvmeofGatewayGroupDeleteGuardModalComponent } from './nvmeof-gateway-group-delete-guard-modal.component';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { NvmeofStateService } from '../nvmeof-state.service';

describe('NvmeofGatewayGroupComponent', () => {
  let component: NvmeofGatewayGroupComponent;
  let fixture: ComponentFixture<NvmeofGatewayGroupComponent>;
  let nvmeofService: any;

  beforeEach(async () => {
    const nvmeofServiceSpy = {
      listGatewayGroups: jest.fn().mockReturnValue(of([])),
      listSubsystems: jest.fn().mockReturnValue(of([]))
    };

    const nvmeofStateServiceMock = {
      refresh$: new Subject<void>(),
      requestRefresh: jest.fn()
    };

    await TestBed.configureTestingModule({
      imports: [HttpClientModule, SharedModule, TabsModule, GridModule, ModalModule],
      declarations: [NvmeofGatewayGroupComponent],
      providers: [
        provideAnimations(),
        provideToastr(),
        { provide: NvmeofService, useValue: nvmeofServiceSpy },
        {
          provide: ModalCdsService,
          useValue: { show: jest.fn() }
        },
        { provide: NvmeofStateService, useValue: nvmeofStateServiceMock }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofGatewayGroupComponent);
    component = fixture.componentInstance;
    nvmeofService = TestBed.inject(NvmeofService);
    fixture.detectChanges();
  });

  it('should create the component', () => {
    expect(component).toBeTruthy();
  });

  it('should call listGatewayGroups and getDaemons on initialization', () => {
    expect(nvmeofService.listGatewayGroups).toHaveBeenCalled();
  });

  it('should populate gatewayGroup$ with the correct data', (done) => {
    const mockData = [
      {
        service_type: 'nvmeof',
        service_id: 'rbd.default',
        service_name: 'nvmeof.rbd.default',
        placement: {
          count: 1
        },
        spec: {
          abort_discovery_on_errors: true,
          abort_on_errors: true,
          allowed_consecutive_spdk_ping_failures: 1,
          break_update_interval_sec: 25,
          cluster_connections: 32,
          conn_retries: 10,
          discovery_port: 8009,
          enable_monitor_client: true,
          enable_prometheus_exporter: true,
          group: 'default',
          log_directory: '/var/log/ceph/',
          log_files_enabled: true,
          log_files_rotation_enabled: true,
          log_level: 'INFO',
          max_gws_in_grp: 16,
          max_hosts: 2048,
          max_hosts_per_namespace: 8,
          max_hosts_per_subsystem: 128,
          max_log_directory_backups: 10,
          max_log_file_size_in_mb: 10,
          max_log_files_count: 20,
          max_namespaces: 4096,
          max_namespaces_per_subsystem: 512,
          max_namespaces_with_netmask: 1000,
          max_ns_to_change_lb_grp: 8,
          max_subsystems: 128,
          monitor_timeout: 1,
          notifications_interval: 60,
          omap_file_lock_duration: 20,
          omap_file_lock_on_read: true,
          omap_file_lock_retries: 30,
          omap_file_lock_retry_sleep_interval: 1,
          omap_file_update_attempts: 500,
          omap_file_update_reloads: 10,
          pool: 'rbd',
          port: 5500,
          prometheus_connection_list_cache_expiration: 60,
          prometheus_cycles_to_adjust_speed: 3,
          prometheus_frequency_slow_down_factor: 3,
          prometheus_port: 10008,
          prometheus_startup_delay: 240,
          prometheus_stats_interval: 10,
          rebalance_period_sec: 7,
          rpc_socket_dir: '/var/tmp/',
          rpc_socket_name: 'spdk.sock',
          spdk_path: '/usr/local/bin/nvmf_tgt',
          spdk_ping_interval_in_seconds: 2,
          spdk_protocol_log_level: 'WARNING',
          spdk_timeout: 60,
          state_update_interval_sec: 5,
          state_update_notify: true,
          subsystem_cache_expiration: 5,
          tgt_path: '/usr/local/bin/nvmf_tgt',
          transport_tcp_options: {
            in_capsule_data_size: 8192,
            max_io_qpairs_per_ctrlr: 7
          },
          transports: 'tcp',
          verbose_log_messages: true,
          verify_keys: true,
          verify_listener_ip: true,
          verify_nqns: true
        },
        status: {
          size: 1,
          running: 1,
          last_refresh: new Date('2025-12-01T16:50:21.122930Z'),
          created: new Date('2025-10-16T16:35:09.623842Z'),
          ports: [5500, 4420, 8009, 10008],
          container_image_id: 'image_id_1',
          container_image_name: 'image_name_1'
        },
        events: [
          {
            created: '2025-10-16T16:35:59.879726Z',
            subject: 'service:nvmeof.rbd.default',
            level: 'INFO',
            message: 'service was created'
          }
        ],
        name: 'default',
        gatewayCount: {
          running: 1,
          error: 0
        },
        subSystemCount: 0,
        nodeCount: 0,
        unmanaged: true
      },
      {
        service_type: 'nvmeof',
        service_id: 'rbd.foo',
        service_name: 'nvmeof.rbd.foo',
        placement: {
          hosts: ['ceph-node-01', 'ceph-node-02', 'ceph-node-03']
        },
        spec: {
          abort_discovery_on_errors: true,
          abort_on_errors: true,
          allowed_consecutive_spdk_ping_failures: 1,
          break_update_interval_sec: 25,
          cluster_connections: 32,
          conn_retries: 10,
          discovery_port: 8009,
          enable_monitor_client: true,
          enable_prometheus_exporter: true,
          group: 'foo',
          log_directory: '/var/log/ceph/',
          log_files_enabled: true,
          log_files_rotation_enabled: true,
          log_level: 'INFO',
          max_gws_in_grp: 16,
          max_hosts: 2048,
          max_hosts_per_namespace: 8,
          max_hosts_per_subsystem: 128,
          max_log_directory_backups: 10,
          max_log_file_size_in_mb: 10,
          max_log_files_count: 20,
          max_namespaces: 4096,
          max_namespaces_per_subsystem: 512,
          max_namespaces_with_netmask: 1000,
          max_ns_to_change_lb_grp: 8,
          max_subsystems: 128,
          monitor_timeout: 1,
          notifications_interval: 60,
          omap_file_lock_duration: 20,
          omap_file_lock_on_read: true,
          omap_file_lock_retries: 30,
          omap_file_lock_retry_sleep_interval: 1,
          omap_file_update_attempts: 500,
          omap_file_update_reloads: 10,
          pool: 'rbd',
          port: 5500,
          prometheus_connection_list_cache_expiration: 60,
          prometheus_cycles_to_adjust_speed: 3,
          prometheus_frequency_slow_down_factor: 3,
          prometheus_port: 10008,
          prometheus_startup_delay: 240,
          prometheus_stats_interval: 10,
          rebalance_period_sec: 7,
          rpc_socket_dir: '/var/tmp/',
          rpc_socket_name: 'spdk.sock',
          spdk_path: '/usr/local/bin/nvmf_tgt',
          spdk_ping_interval_in_seconds: 2,
          spdk_protocol_log_level: 'WARNING',
          spdk_timeout: 60,
          state_update_interval_sec: 5,
          state_update_notify: true,
          subsystem_cache_expiration: 5,
          tgt_path: '/usr/local/bin/nvmf_tgt',
          transport_tcp_options: {
            in_capsule_data_size: 8192,
            max_io_qpairs_per_ctrlr: 7
          },
          transports: 'tcp',
          verbose_log_messages: true,
          verify_keys: true,
          verify_listener_ip: true,
          verify_nqns: true
        },
        status: {
          size: 3,
          running: 2,
          last_refresh: new Date('2025-12-01T16:44:42.361882Z'),
          created: new Date('2025-11-11T12:55:32.770910Z'),
          ports: [5500, 4420, 8009, 10008],
          container_image_id: 'image_id_2',
          container_image_name: 'image_name_2'
        },
        events: [
          {
            created: '2025-11-11T12:56:42.509108Z',
            subject: 'service:nvmeof.rbd.foo',
            level: 'INFO',
            message: 'service was created'
          }
        ],
        name: 'foo',
        gatewayCount: {
          running: 2,
          error: 1
        },
        subSystemCount: 0,
        nodeCount: 3,
        unmanaged: true
      }
    ];

    component.gatewayGroup$ = of(mockData as any);

    component.gatewayGroup$.subscribe((data) => {
      expect(data).toEqual(mockData);
      done();
    });
  });

  describe('View details action', () => {
    it('should use routerLink and navigate to the resource page for the selected group', () => {
      component.selection.first = jest.fn().mockReturnValue({ name: 'default' });
      const viewAction = component.tableActions.find((a) => a.name === 'View details');
      expect(viewAction).toBeTruthy();
      expect(viewAction!.click).toBeUndefined();
      const link = (viewAction!.routerLink as Function)();
      expect(link).toBe('/block/nvmeof/gateways/view/default');
    });

    it('should set canBePrimary true for single selection only', () => {
      const viewAction = component.tableActions.find((a) => a.name === 'View details');
      const single = { hasSingleSelection: true } as any;
      const multi = { hasSingleSelection: false } as any;
      expect((viewAction!.canBePrimary as Function)(single)).toBe(true);
      expect((viewAction!.canBePrimary as Function)(multi)).toBe(false);
    });
  });

  describe('Delete Flow with/without Subsystems', () => {
    let mockGroup: any;

    beforeEach(() => {
      mockGroup = {
        service_name: 'nvmeof.rbd.default',
        spec: { group: 'default' },
        subSystemCount: 0
      };
      component.selection.first = jest.fn().mockReturnValue(mockGroup);
    });

    it('should show can-not-delete modal if subsystems exist', () => {
      const mockSubsystems = [{ nqn: 'subsystem-1' }, { nqn: 'subsystem-2' }];
      nvmeofService.listSubsystems.mockReturnValue(of(mockSubsystems));
      const modalService = TestBed.inject(ModalCdsService);

      component.deleteGatewayGroupModal();

      expect(nvmeofService.listSubsystems).toHaveBeenCalledWith('default');
      expect(modalService.show).toHaveBeenCalledWith(NvmeofGatewayGroupDeleteGuardModalComponent, {
        gatewayName: 'default',
        connectedSubsystems: [{ nqn: 'subsystem-1' }, { nqn: 'subsystem-2' }]
      });
    });

    it('should show delete confirmation modal if no subsystems exist', () => {
      nvmeofService.listSubsystems.mockReturnValue(of([]));
      const modalService = TestBed.inject(ModalCdsService);

      component.deleteGatewayGroupModal();

      expect(nvmeofService.listSubsystems).toHaveBeenCalledWith('default');
      expect(modalService.show).toHaveBeenCalledWith(
        DeleteConfirmationModalComponent,
        expect.any(Object)
      );
    });
  });

  it('should refresh table and setup state after gateway group delete completes', () => {
    const modalService = TestBed.inject(ModalCdsService);
    const taskWrapperService = TestBed.inject(TaskWrapperService);
    const nvmeofStateService = TestBed.inject(NvmeofStateService);

    jest.spyOn(modalService, 'show').mockImplementation(() => undefined);
    jest.spyOn(taskWrapperService, 'wrapTaskAroundCall').mockReturnValue(
      new Observable((observer) => {
        observer.complete();
      })
    );

    const refreshBtnSpy = jest.fn();
    component.table = { refreshBtn: refreshBtnSpy } as any;
    const requestRefreshSpy = jest.spyOn(nvmeofStateService, 'requestRefresh');

    component.selection = {
      first: () => ({
        service_name: 'nvmeof.rbd.default',
        spec: { group: 'default' },
        subSystemCount: 0
      }),
      hasSelection: true
    } as any;

    component.deleteGatewayGroupModal();

    const submitActionObservable = (modalService.show as jest.Mock).mock.calls[0][1]
      .submitActionObservable;
    submitActionObservable().subscribe();

    expect(refreshBtnSpy).toHaveBeenCalled();
    expect(requestRefreshSpy).toHaveBeenCalled();
  });
});
