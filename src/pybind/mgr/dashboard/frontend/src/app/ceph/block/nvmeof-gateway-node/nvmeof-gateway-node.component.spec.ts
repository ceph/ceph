import { ComponentFixture, TestBed } from '@angular/core/testing';
import { of } from 'rxjs';
import { take } from 'rxjs/operators';
import { NvmeofGatewayNodeComponent } from './nvmeof-gateway-node.component';
import { CdTableSelection } from '~/app/shared/models/cd-table-selection';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Host } from '~/app/shared/models/host.interface';
import { HostService } from '~/app/shared/api/host.service';

class HostServiceStub {
  hosts: Host[] = [];

  getAllHosts() {
    return of(this.hosts);
  }
}

describe('NvmeofGatewayNodeComponent', () => {
  let component: NvmeofGatewayNodeComponent;
  let fixture: ComponentFixture<NvmeofGatewayNodeComponent>;
  let hostService: HostServiceStub;
  let authStorageService: AuthStorageService;

  beforeEach(async () => {
    hostService = new HostServiceStub();
    authStorageService = { getPermissions: () => ({ nvmeof: {} }) } as any;

    await TestBed.configureTestingModule({
      declarations: [NvmeofGatewayNodeComponent],
      providers: [
        { provide: HostService, useValue: hostService },
        { provide: AuthStorageService, useValue: authStorageService }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofGatewayNodeComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).to.exist;
  });

  it('should initialize hostname and status columns', () => {
    expect(component.columns.length).to.equal(2);
    expect(component.columns[0].prop).to.equal('hostname');
    expect(component.columns[1].prop).to.equal('status');
  });

  it('should load gateway nodes with normalized status', async () => {
    hostService.hosts = [
      {
        hostname: 'h1',
        status: { state: 'up' },
        addr: '10.0.0.1',
        ceph_version: '',
        labels: [],
        services: [],
        sources: { ceph: true, orchestrator: true },
        service_instances: []
      }
    ];

    const result = await component.gatewayNodes$.pipe(take(1)).toPromise();

    expect(result[0].hostname).to.equal('h1');
    expect(result[0].status).to.equal('up');
  });

  it('should call fetchData and push to subject', () => {
    const calls: any[] = [];
    (component as any).subject = { next: (val: any) => calls.push(val) };
    component.fetchData();
    expect(calls).to.deep.equal([[]]);
  });

  it('should update selection', () => {
    const selection = new CdTableSelection();
    component.updateSelection(selection);
    expect(component.selection).to.equal(selection);
  });
});
