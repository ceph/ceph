import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { BehaviorSubject, of } from 'rxjs';

import { NfsClusterResourcePageComponent } from './nfs-cluster-resource-page.component';
import { NfsClusterResourceStateService } from '~/app/shared/services/nfs-cluster-resource-state.service';
import { NfsService } from '~/app/shared/api/nfs.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { DimlessBinaryPerSecondPipe } from '~/app/shared/pipes/dimless-binary-per-second.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { NFSBwIopConfig, NFSCluster } from '../models/nfs-cluster-config';

describe('NfsClusterResourcePageComponent', () => {
  let component: NfsClusterResourcePageComponent;
  let fixture: ComponentFixture<NfsClusterResourcePageComponent>;
  let stateSubject: BehaviorSubject<{ cluster: NFSCluster | null; qos: NFSBwIopConfig | null }>;
  let nfsServiceSpy: {
    enableQosBandwidthForCLuster: jest.Mock;
    enableQosOpsForCLuster: jest.Mock;
  };
  let notificationServiceSpy: { show: jest.Mock };
  let nfsClusterResourceStateServiceSpy: {
    state$: any;
    load: jest.Mock;
  };

  const activeCluster: NFSCluster = {
    name: 'demo-nfs-cluster',
    virtual_ip: null,
    port: 2049,
    monitor_port: 9587,
    deployment_type: 'standalone',
    backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }],
    placement: { count: 10 }
  };

  /** Helper: emit a new cluster (with no QoS) into the state subject. */
  function emitCluster(cluster: NFSCluster | null) {
    stateSubject.next({ cluster, qos: null });
  }

  beforeEach(async () => {
    stateSubject = new BehaviorSubject({ cluster: activeCluster, qos: null });

    nfsServiceSpy = {
      enableQosBandwidthForCLuster: jest.fn().mockReturnValue(of({})),
      enableQosOpsForCLuster: jest.fn().mockReturnValue(of({}))
    };

    notificationServiceSpy = {
      show: jest.fn()
    };

    nfsClusterResourceStateServiceSpy = {
      state$: stateSubject.asObservable(),
      load: jest.fn()
    };

    await TestBed.configureTestingModule({
      declarations: [NfsClusterResourcePageComponent],
      providers: [
        ActionLabelsI18n,
        DimlessBinaryPerSecondPipe,
        FormatterService,
        {
          provide: NfsClusterResourceStateService,
          useValue: nfsClusterResourceStateServiceSpy
        },
        {
          provide: NfsService,
          useValue: nfsServiceSpy
        },
        {
          provide: NotificationService,
          useValue: notificationServiceSpy
        },
        {
          provide: ActivatedRoute,
          useValue: {
            data: of({ section: 'overview' }),
            parent: {
              paramMap: of(convertToParamMap({ cluster_id: 'demo-nfs-cluster' }))
            }
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(NfsClusterResourcePageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should read section from route data', () => {
    expect(component.section).toBe('overview');
  });

  it('should build overview fields after init', () => {
    expect(component.overviewFields.find((f) => f.label === 'Name')?.value).toBe(
      'demo-nfs-cluster'
    );
    expect(component.overviewFields.find((f) => f.label === 'Status')?.value).toBe('Active');
    expect(component.overviewFields.find((f) => f.label === 'Service port')?.value).toBe(2049);
    expect(component.overviewFields.find((f) => f.label === 'Protocol')?.value).toBe('NFSv4');
  });

  it('should show NFSv3 and NFSv4 when enable_nfsv3 is set', () => {
    emitCluster({ ...activeCluster, enable_nfsv3: true });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Protocol')?.value).toBe(
      'NFSv3, NFSv4'
    );
  });

  it('should use backend port when top-level port is missing', () => {
    emitCluster({
      name: 'standalone-nfs',
      virtual_ip: null,
      deployment_type: 'standalone',
      backend: [{ hostname: 'host1', ip: '1.2.3.4', port: 12049, status: 'running' }]
    });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Service port')?.value).toBe(12049);
  });

  it('should show dash when no service port is available', () => {
    emitCluster({
      name: 'standalone-nfs',
      virtual_ip: null,
      deployment_type: 'standalone',
      backend: [{ hostname: 'host1', ip: '1.2.3.4', status: 'running' }]
    });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Service port')?.value).toBe('-');
  });

  it('should show host_pattern placement as a string', () => {
    emitCluster({ ...activeCluster, placement: { host_pattern: 'nfs-*' } });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Placement')?.value).toBe('nfs-*');
  });

  it('should show host_pattern placement from pattern object', () => {
    emitCluster({
      ...activeCluster,
      placement: { host_pattern: { pattern: 'nfs-.*', pattern_type: 'regex' } }
    });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Placement')?.value).toBe('nfs-.*');
  });

  it('should show IOPS Limit Off when qos is null', () => {
    expect(component.overviewFields.find((f) => f.label === 'IOPS Limit')?.value).toBe('Off');
    expect(component.overviewFields.find((f) => f.label === 'Bandwidth limit')?.value).toBe('Off');
  });

  it('should show IOPS Limit On when enable_iops_control is true', () => {
    stateSubject.next({ cluster: activeCluster, qos: { enable_iops_control: true } });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'IOPS Limit')?.value).toBe('On');
  });

  it('should build IOPS tooltip for PerShare qos_type', () => {
    stateSubject.next({
      cluster: activeCluster,
      qos: {
        enable_iops_control: true,
        qos_type: 'PerShare',
        max_export_iops: 5400
      }
    });
    fixture.detectChanges();
    const iopsField = component.overviewFields.find((f) => f.label === 'IOPS Limit');
    expect(iopsField?.value).toBe('On');
    expect(iopsField?.tooltip).toBe('Shares max. IOPs: 5400');
  });

  it('should build IOPS tooltip for PerShare_PerClient qos_type', () => {
    stateSubject.next({
      cluster: activeCluster,
      qos: {
        enable_iops_control: true,
        qos_type: 'PerShare_PerClient',
        max_export_iops: 5400,
        max_client_iops: 8000
      }
    });
    fixture.detectChanges();
    const iopsField = component.overviewFields.find((f) => f.label === 'IOPS Limit');
    expect(iopsField?.value).toBe('On');
    expect(iopsField?.tooltip).toBe('Shares max. IOPs: 5400\nClients max. IOPs: 8000');
  });

  it('should show Bandwidth limit On when enable_bw_control is true', () => {
    stateSubject.next({ cluster: activeCluster, qos: { enable_bw_control: true } });
    fixture.detectChanges();
    expect(component.overviewFields.find((f) => f.label === 'Bandwidth limit')?.value).toBe('On');
  });

  it('should build Bandwidth tooltip for PerShare_PerClient qos_type', () => {
    stateSubject.next({
      cluster: activeCluster,
      qos: {
        enable_bw_control: true,
        qos_type: 'PerShare_PerClient',
        max_export_read_bw: 500 * 1024 * 1024,
        max_export_write_bw: 600 * 1024 * 1024,
        max_client_read_bw: 500 * 1024 * 1024,
        max_client_write_bw: 600 * 1024 * 1024
      }
    });
    fixture.detectChanges();
    const bwField = component.overviewFields.find((f) => f.label === 'Bandwidth limit');
    expect(bwField?.value).toBe('On');
    expect(bwField?.tooltip).toBe(
      'Shares max. bandwidth:\nRead: 500 MiB/s\nWrite: 600 MiB/s\nClients max. bandwidth:\nRead: 500 MiB/s\nWrite: 600 MiB/s'
    );
  });

  it('should toggle performance limits side panel', () => {
    expect(component.isPerformanceLimitsOpen).toBe(false);
    component.openPerformanceLimits();
    expect(component.isPerformanceLimitsOpen).toBe(true);
    component.closePerformanceLimits();
    expect(component.isPerformanceLimitsOpen).toBe(false);
  });

  it('should save performance limits and reload state on success', () => {
    component.openPerformanceLimits();
    component.nfsRateLimitComponent = {
      showValidationErrors: jest.fn().mockReturnValue(true),
      getRateLimitFormValue: jest.fn().mockReturnValue({
        enable_qos: true,
        qos_type: 'PerShare',
        max_export_read_bw: 1024,
        max_export_write_bw: 2048
      }),
      getRateLimitOpsFormValue: jest.fn().mockReturnValue({
        enable_ops: false,
        qos_type: 'PerShare',
        max_export_iops: 0
      })
    } as any;

    component.savePerformanceLimits();

    expect(nfsServiceSpy.enableQosBandwidthForCLuster).toHaveBeenCalledWith({
      cluster_id: 'demo-nfs-cluster',
      qos_type: 'PerShare',
      max_export_read_bw: 1024,
      max_export_write_bw: 2048,
      disable_qos: false
    });
    expect(nfsServiceSpy.enableQosOpsForCLuster).toHaveBeenCalledWith({
      cluster_id: 'demo-nfs-cluster',
      qos_type: 'PerShare',
      max_export_iops: 0,
      disable_ops: true
    });
    expect(notificationServiceSpy.show).toHaveBeenCalledWith(
      NotificationType.success,
      expect.any(String)
    );
    expect(nfsClusterResourceStateServiceSpy.load).toHaveBeenCalledWith('demo-nfs-cluster');
    expect(component.isPerformanceLimitsOpen).toBe(false);
  });

  it('should not save performance limits if validation fails', () => {
    component.openPerformanceLimits();
    component.nfsRateLimitComponent = {
      showValidationErrors: jest.fn().mockReturnValue(false),
      getRateLimitFormValue: jest.fn(),
      getRateLimitOpsFormValue: jest.fn()
    } as any;

    component.savePerformanceLimits();

    expect(nfsServiceSpy.enableQosBandwidthForCLuster).not.toHaveBeenCalled();
    expect(nfsServiceSpy.enableQosOpsForCLuster).not.toHaveBeenCalled();
    expect(component.isPerformanceLimitsOpen).toBe(true);
  });

  it('should set loadError when cluster is missing', () => {
    emitCluster(null);
    fixture.detectChanges();
    expect(component.loadError).toBe(true);
    expect(component.selection).toBeUndefined();
    expect(component.overviewFields.length).toBe(0);
  });
});
