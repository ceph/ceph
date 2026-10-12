import { ComponentFixture, TestBed } from '@angular/core/testing';
import { Router } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ModalModule } from 'carbon-components-angular';
import { DeleteGuardModalComponent } from './delete-guard-modal.component';

describe('DeleteGuardModalComponent', () => {
  let component: DeleteGuardModalComponent;
  let fixture: ComponentFixture<DeleteGuardModalComponent>;
  let router: Router;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ModalModule],
      declarations: [DeleteGuardModalComponent],
      providers: [
        {
          provide: Router,
          useValue: { navigate: jest.fn() }
        },
        { provide: 'resourceName', useValue: 'my-pool' },
        { provide: 'resourceType', useValue: 'Pool' }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(DeleteGuardModalComponent);
    component = fixture.componentInstance;
    router = TestBed.inject(Router);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set default values', () => {
    expect(component.resourceName).toBe('my-pool');
    expect(component.resourceType).toBe('Pool');
    expect(component.connectedItems).toEqual([]);
    expect(component.message).toContain('connected items');
    expect(component.connectedItemsLabel).toContain('View connected items');
  });

  it('should navigate to item route and close modal', () => {
    const closeSpy = jest.spyOn(component, 'closeModal');
    const item = {
      name: 'subsystem-1',
      route: ['/block/nvmeof/subsystems', 'subsystem-1', 'overview'],
      queryParams: { group: 'default' }
    };

    component.navigateToItem(item);

    expect(router.navigate).toHaveBeenCalledWith(
      ['/block/nvmeof/subsystems', 'subsystem-1', 'overview'],
      { queryParams: { group: 'default' } }
    );
    expect(closeSpy).toHaveBeenCalled();
  });

  it('should not navigate if item has no route', () => {
    const closeSpy = jest.spyOn(component, 'closeModal');
    const item = { name: 'no-route-item' };

    component.navigateToItem(item);

    expect(router.navigate).not.toHaveBeenCalled();
    expect(closeSpy).not.toHaveBeenCalled();
  });

  it('should use default resourceType when not provided', async () => {
    TestBed.resetTestingModule();
    await TestBed.configureTestingModule({
      imports: [ModalModule],
      declarations: [DeleteGuardModalComponent],
      providers: [
        { provide: Router, useValue: { navigate: jest.fn() } },
        { provide: 'resourceName', useValue: 'test-resource' }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    const defaultFixture = TestBed.createComponent(DeleteGuardModalComponent);
    const defaultComponent = defaultFixture.componentInstance;
    defaultFixture.detectChanges();

    expect(defaultComponent.resourceType).toBe('resource');
  });
});
