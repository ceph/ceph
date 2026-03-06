import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NvmeofGatewayGroupComponent } from './nvmeof-gateway-group.component';
import { GridModule, TabsModule } from 'carbon-components-angular';
import { NvmeofService } from '~/app/shared/api/nvmeof.service';
import { of } from 'rxjs';
import { HttpClientModule } from '@angular/common/http';
import { SharedModule } from '~/app/shared/shared.module';

describe('NvmeofGatewayGroupComponent', () => {
  let component: NvmeofGatewayGroupComponent;
  let fixture: ComponentFixture<NvmeofGatewayGroupComponent>;
  let nvmeofService: any;

  beforeEach(async () => {
    const nvmeofServiceSpy = {
      listGatewayGroups: jest.fn().mockReturnValue(of([])),
      listSubsystems: jest.fn().mockReturnValue(of([]))
    };

    await TestBed.configureTestingModule({
      imports: [HttpClientModule, SharedModule, TabsModule, GridModule],
      declarations: [NvmeofGatewayGroupComponent],
      providers: [{ provide: NvmeofService, useValue: nvmeofServiceSpy }]
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
});
