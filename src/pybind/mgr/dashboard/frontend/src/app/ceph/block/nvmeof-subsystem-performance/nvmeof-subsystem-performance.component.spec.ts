import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { RouterTestingModule } from '@angular/router/testing';
import { ActivatedRoute } from '@angular/router';
import { of } from 'rxjs';

import { NvmeofSubsystemPerformanceComponent } from './nvmeof-subsystem-performance.component';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { Permissions } from '~/app/shared/models/permissions';
import { SharedModule } from '~/app/shared/shared.module';

describe('NvmeofSubsystemPerformanceComponent', () => {
  let component: NvmeofSubsystemPerformanceComponent;
  let fixture: ComponentFixture<NvmeofSubsystemPerformanceComponent>;

  const mockPermissions = new Permissions({ grafana: ['read'] });

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofSubsystemPerformanceComponent],
      imports: [HttpClientTestingModule, RouterTestingModule, SharedModule],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            parent: {
              params: of({ subsystem_nqn: 'nqn.2016-06.io.spdk:cnode1' })
            },
            queryParams: of({ group: 'group1' })
          }
        },
        {
          provide: AuthStorageService,
          useValue: {
            getPermissions: jest.fn().mockReturnValue(mockPermissions)
          }
        }
      ]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofSubsystemPerformanceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set subsystemNQN and groupName from route params', fakeAsync(() => {
    component.ngOnInit();
    tick();
    expect(component.subsystemNQN).toBe('nqn.2016-06.io.spdk:cnode1');
    expect(component.groupName).toBe('group1');
  }));

  it('should have grafana read permission', () => {
    expect(component.permissions.grafana.read).toBeTruthy();
  });
});
