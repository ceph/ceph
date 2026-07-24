import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterModule } from '@angular/router';

import { NvmeofSetupCardsComponent } from './nvmeof-setup-cards.component';

describe('NvmeofSetupCardsComponent', () => {
  let component: NvmeofSetupCardsComponent;
  let fixture: ComponentFixture<NvmeofSetupCardsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [NvmeofSetupCardsComponent, RouterModule.forRoot([])]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofSetupCardsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render 3 setup step cards', () => {
    const cards = fixture.nativeElement.querySelectorAll('cd-setup-step-card');
    expect(cards.length).toBe(3);
  });

  it('should not show completion link when isAllConfigured is false', () => {
    component.isAllConfigured = false;
    fixture.detectChanges();
    const link = fixture.nativeElement.querySelector('.nvmeof-setup-cards__completion');
    expect(link).toBeNull();
  });

  it('should show completion link when isAllConfigured is true', () => {
    component.isAllConfigured = true;
    fixture.detectChanges();
    const link = fixture.nativeElement.querySelector('.nvmeof-setup-cards__completion');
    expect(link).toBeTruthy();
  });

  it('should emit viewStatus when the completion link is clicked', () => {
    component.isAllConfigured = true;
    fixture.detectChanges();

    const emitSpy = jest.spyOn(component.viewStatus, 'emit');
    const link = fixture.nativeElement.querySelector('.nvmeof-setup-cards__completion a');
    link.click();

    expect(emitSpy).toHaveBeenCalledTimes(1);
  });

  describe('setup state', () => {
    const getStepCards = () =>
      fixture.debugElement.queryAll((el) => el.name === 'cd-setup-step-card');

    it('should show gateway-only setup state when gateway exists but subsystems and namespaces do not', () => {
      component.hasGatewayGroups = true;
      component.hasSubsystems = false;
      component.hasNamespaces = false;
      fixture.detectChanges();

      const cards = getStepCards();
      expect(cards[0].componentInstance.statusMessage).toBe(
        'Gateway group configured successfully.'
      );
      expect(cards[1].componentInstance.statusMessage).toBe(
        'No subsystem configured for this cluster yet.'
      );
      expect(cards[2].componentInstance.statusMessage).toBe(
        'No namespace allocated or mapped yet.'
      );
    });

    it('should show subsystem-complete setup state when gateway and subsystem exist', () => {
      component.hasGatewayGroups = true;
      component.hasSubsystems = true;
      component.hasNamespaces = false;
      fixture.detectChanges();

      const cards = getStepCards();
      expect(cards[0].componentInstance.statusMessage).toBe(
        'Gateway group configured successfully.'
      );
      expect(cards[1].componentInstance.statusMessage).toBe('Subsystem configured successfully.');
      expect(cards[2].componentInstance.statusMessage).toBe(
        'No namespace allocated or mapped yet.'
      );
    });
  });

  it('should keep subsystem and namespace info messages when hasGatewayGroups is false', () => {
    component.hasGatewayGroups = false;
    component.hasSubsystems = false;
    component.hasNamespaces = false;
    fixture.detectChanges();

    const cardElements = fixture.debugElement.queryAll((el) => el.name === 'cd-setup-step-card');
    const subsystemCard = cardElements[1].componentInstance;
    const namespaceCard = cardElements[2].componentInstance;

    expect(subsystemCard.statusMessage).toBe('No subsystem configured for this cluster yet.');
    expect(namespaceCard.statusMessage).toBe('No namespace allocated or mapped yet.');
  });

  it('should display original info messages when hasGatewayGroups is true', () => {
    component.hasGatewayGroups = true;
    component.hasSubsystems = false;
    component.hasNamespaces = false;
    fixture.detectChanges();

    const cardElements = fixture.debugElement.queryAll((el) => el.name === 'cd-setup-step-card');
    const subsystemCard = cardElements[1].componentInstance;
    const namespaceCard = cardElements[2].componentInstance;

    expect(subsystemCard.statusMessage).toBe('No subsystem configured for this cluster yet.');
    expect(namespaceCard.statusMessage).toBe('No namespace allocated or mapped yet.');
  });

  it('should display gateway info message when hasGatewayGroups is false', () => {
    component.hasGatewayGroups = false;
    fixture.detectChanges();

    const gatewayCard = fixture.debugElement.queryAll((el) => el.name === 'cd-setup-step-card')[0]
      .componentInstance;

    expect(gatewayCard.statusMessage).toBe('No gateway groups configured for this cluster yet.');
  });

  it('should display success messages when all setup steps are configured', () => {
    component.hasGatewayGroups = true;
    component.hasSubsystems = true;
    component.hasNamespaces = true;
    fixture.detectChanges();

    const cardElements = fixture.debugElement.queryAll((el) => el.name === 'cd-setup-step-card');

    expect(cardElements[0].componentInstance.statusMessage).toBe(
      'Gateway group configured successfully.'
    );
    expect(cardElements[1].componentInstance.statusMessage).toBe(
      'Subsystem configured successfully.'
    );
    expect(cardElements[2].componentInstance.statusMessage).toBe('Namespaces mapped successfully.');
  });

  it('should update all step messages when gateway groups are removed after full configuration', () => {
    component.hasGatewayGroups = true;
    component.hasSubsystems = true;
    component.hasNamespaces = true;
    fixture.detectChanges();

    const getCards = () => fixture.debugElement.queryAll((el) => el.name === 'cd-setup-step-card');

    expect(getCards()[0].componentInstance.statusMessage).toBe(
      'Gateway group configured successfully.'
    );

    component.hasGatewayGroups = false;
    component.hasSubsystems = false;
    component.hasNamespaces = false;
    fixture.detectChanges();

    expect(getCards()[0].componentInstance.statusMessage).toBe(
      'No gateway groups configured for this cluster yet.'
    );
    expect(getCards()[1].componentInstance.statusMessage).toBe(
      'No subsystem configured for this cluster yet.'
    );
    expect(getCards()[2].componentInstance.statusMessage).toBe(
      'No namespace allocated or mapped yet.'
    );
  });
});
