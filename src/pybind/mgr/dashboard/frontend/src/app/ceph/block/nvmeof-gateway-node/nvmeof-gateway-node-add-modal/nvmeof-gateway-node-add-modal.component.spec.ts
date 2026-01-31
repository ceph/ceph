import { ComponentFixture, TestBed } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NvmeofGatewayNodeAddModalComponent } from './nvmeof-gateway-node-add-modal.component';
import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { RouterTestingModule } from '@angular/router/testing';
import { HostService } from '~/app/shared/api/host.service';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { CephServiceService } from '~/app/shared/api/ceph-service.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TaskMessageService } from '~/app/shared/services/task-message.service';
import { of } from 'rxjs';
import { CdTableFetchDataContext } from '~/app/shared/models/cd-table-fetch-data-context';

describe('NvmeofGatewayNodeAddModalComponent', () => {
  let component: NvmeofGatewayNodeAddModalComponent;
  let fixture: ComponentFixture<NvmeofGatewayNodeAddModalComponent>;

  const mockHostService = {
    checkHostsFactsAvailable: jest.fn().mockReturnValue(true),
    list: jest.fn().mockReturnValue(of([{ hostname: 'host1' }, { hostname: 'host2' }]))
  };

  const mockOrchService = {
    status: jest.fn().mockReturnValue(of({ available: true }))
  };

  const mockCephServiceService = {
    update: jest.fn().mockReturnValue(of({}))
  };

  const mockNotificationService = {
    show: jest.fn()
  };

  const mockTaskMessageService = {
    messages: {
      'nvmeof/gateway/node/add': {
        success: jest.fn().mockReturnValue('Success'),
        failure: jest.fn().mockReturnValue('Failure')
      }
    }
  };

  const mockServiceSpec = {
    placement: {
      hosts: ['host1']
    }
  };

  configureTestBed({
    imports: [SharedModule, HttpClientTestingModule, RouterTestingModule],
    declarations: [NvmeofGatewayNodeAddModalComponent],
    providers: [
      { provide: HostService, useValue: mockHostService },
      { provide: OrchestratorService, useValue: mockOrchService },
      { provide: CephServiceService, useValue: mockCephServiceService },
      { provide: NotificationService, useValue: mockNotificationService },
      { provide: TaskMessageService, useValue: mockTaskMessageService },
      { provide: 'groupName', useValue: 'group1' },
      { provide: 'usedHostnames', useValue: ['host1'] },
      { provide: 'serviceSpec', useValue: mockServiceSpec }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofGatewayNodeAddModalComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should load attributes', () => {
    expect(component.groupName).toBe('group1');
    expect(component.usedHostnames).toEqual(['host1']);
    expect(component.serviceSpec).toEqual(mockServiceSpec);
  });

  it('should load hosts and filter used ones', () => {
    const context = new CdTableFetchDataContext(() => undefined);
    component.getHosts(context);

    expect(mockOrchService.status).toHaveBeenCalled();
    expect(mockHostService.list).toHaveBeenCalled();
    expect(component.hosts.length).toBe(1);
    expect(component.hosts[0].hostname).toBe('host2');
  });

  it('should add gateway node', () => {
    component.selection.selected = [{ hostname: 'host2' }];
    component.onSubmit();

    expect(mockCephServiceService.update).toHaveBeenCalledWith({
      placement: { hosts: ['host1', 'host2'] }
    });
    expect(mockNotificationService.show).toHaveBeenCalled();
  });
});
