import { ComponentFixture, TestBed } from '@angular/core/testing';
import { configureTestBed } from '~/testing/unit-test-helper';
import { SharedModule } from '~/app/shared/shared.module';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NfsClusterComponent } from './nfs-cluster.component';
import { of } from 'rxjs';
import { OrchestratorService } from '~/app/shared/api/orchestrator.service';
import { NfsService } from '~/app/shared/api/nfs.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { RouterTestingModule } from '@angular/router/testing';

describe('NfsClusterComponent', () => {
  let component: NfsClusterComponent;
  let fixture: ComponentFixture<NfsClusterComponent>;
  let orchestratorService: OrchestratorService;

  configureTestBed({
    declarations: [NfsClusterComponent],
    imports: [HttpClientTestingModule, SharedModule, RouterTestingModule],
    providers: [
      {
        provide: OrchestratorService,
        useValue: {
          status: jest.fn().mockReturnValue(of({}))
        }
      },
      {
        provide: NfsService,
        useValue: {
          nfsClusterList: jest.fn().mockReturnValue(of([]))
        }
      },
      {
        provide: AuthStorageService,
        useValue: {
          getPermissions: jest.fn().mockReturnValue({ nfs: {} })
        }
      }
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NfsClusterComponent);
    component = fixture.componentInstance;
    orchestratorService = TestBed.inject(OrchestratorService);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize orchestrator status on init', () => {
    expect(orchestratorService.status).toHaveBeenCalled();
  });

  it('should initialize columns on init', () => {
    expect(component.columns.length).toBeGreaterThan(0);
  });

  it('should load data on loadData call', () => {
    const spy = jest.spyOn(component.subject, 'next');
    component.loadData();
    expect(spy).toHaveBeenCalledWith([]);
  });

  it('should update selection on updateSelection call', () => {
    const selection = { first: jest.fn() } as any;
    component.updateSelection(selection);
    expect(component.selection).toBe(selection);
  });

  it('should set table actions on init', () => {
    expect(component.tableActions.length).toBeGreaterThan(0);
  });
});
