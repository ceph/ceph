import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { RouterTestingModule } from '@angular/router/testing';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { NvmeofGatewayGroupDeleteGuardModalComponent } from './nvmeof-gateway-group-delete-guard-modal.component';

describe('NvmeofGatewayGroupDeleteGuardModalComponent', () => {
  let component: NvmeofGatewayGroupDeleteGuardModalComponent;
  let fixture: ComponentFixture<NvmeofGatewayGroupDeleteGuardModalComponent>;
  let router: Router;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [RouterTestingModule],
      declarations: [NvmeofGatewayGroupDeleteGuardModalComponent],
      providers: [
        { provide: 'gatewayName', useValue: 'gateway-dev' },
        {
          provide: 'connectedSubsystems',
          useValue: [{ nqn: 'subsystem-1' }, { nqn: 'subsystem-2' }]
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(NvmeofGatewayGroupDeleteGuardModalComponent);
    component = fixture.componentInstance;
    router = TestBed.inject(Router);
    jest.spyOn(router, 'navigate').mockImplementation();
    fixture.detectChanges();
  });

  it('should create the modal component', () => {
    expect(component).toBeTruthy();
  });

  it('should load dynamic inputs', () => {
    expect(component.gatewayName).toBe('gateway-dev');
    expect(component.connectedSubsystems).toEqual([{ nqn: 'subsystem-1' }, { nqn: 'subsystem-2' }]);
  });

  it('should navigate to subsystem detail page and close modal', () => {
    const closeSpy = jest.spyOn(component, 'closeModal').mockImplementation();

    component.navigateToSubsystem('subsystem-1');

    expect(router.navigate).toHaveBeenCalledWith(
      ['/block/nvmeof/subsystems', 'subsystem-1', 'overview'],
      { queryParams: { group: 'gateway-dev' } }
    );
    expect(closeSpy).toHaveBeenCalled();
  });
});
