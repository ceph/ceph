import { ComponentFixture, TestBed } from '@angular/core/testing';
import { CephfsService } from '~/app/shared/api/cephfs.service';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CephfsMirroringEntityComponent } from './cephfs-mirroring-entity.component';

describe('CephfsMirroringEntityComponent', () => {
  let component: CephfsMirroringEntityComponent;
  let fixture: ComponentFixture<CephfsMirroringEntityComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [CephfsMirroringEntityComponent],
      providers: [ActionLabelsI18n, { provide: CephfsService, useValue: {} }]
    }).compileComponents();

    fixture = TestBed.createComponent(CephfsMirroringEntityComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create the component', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize columns correctly on ngOnInit', () => {
    component.ngOnInit();
    expect(component.columns.length).toBe(4);
    expect(component.columns[0].prop).toBe('entity');
    expect(component.columns[1].prop).toBe('mdsCaps');
    expect(component.columns[2].prop).toBe('monCaps');
    expect(component.columns[3].prop).toBe('osdCaps');
  });

  it('should initialize the form with user_entity control', () => {
    component.ngOnInit();
    expect(component.entityForm.contains('user_entity')).toBeTrue();
  });

  it('should emit entitySelected when updateSelection is called', () => {
    spyOn(component.entitySelected, 'emit');
    const selection = { first: () => ({ entity: 'foo' }) } as any;
    component.updateSelection(selection);
    expect(component.entitySelected.emit).toHaveBeenCalledWith('foo');
  });

  it('should emit entitySelected with null if no selection', () => {
    spyOn(component.entitySelected, 'emit');
    const selection = { first: () => undefined } as any;
    component.updateSelection(selection);
    expect(component.entitySelected.emit).toHaveBeenCalledWith(null);
  });

  it('should set isCreatingNewEntity and show warnings/info on onCreateEntitySelected', () => {
    component.isCreatingNewEntity = false;
    component.showCreateRequirementsWarning = false;
    component.showCreateCapabilitiesInfo = false;
    component.onCreateEntitySelected();
    expect(component.isCreatingNewEntity).toBeTrue();
    expect(component.showCreateRequirementsWarning).toBeTrue();
    expect(component.showCreateCapabilitiesInfo).toBeTrue();
  });

  it('should set isCreatingNewEntity to false and show warnings/info on onExistingEntitySelected', () => {
    component.isCreatingNewEntity = true;
    component.showSelectRequirementsWarning = false;
    component.showSelectEntityInfo = false;
    component.onExistingEntitySelected();
    expect(component.isCreatingNewEntity).toBeFalse();
    expect(component.showSelectRequirementsWarning).toBeTrue();
    expect(component.showSelectEntityInfo).toBeTrue();
  });

  it('should hide create requirements warning on onDismissCreateRequirementsWarning', () => {
    component.showCreateRequirementsWarning = true;
    component.onDismissCreateRequirementsWarning();
    expect(component.showCreateRequirementsWarning).toBeFalse();
  });

  it('should hide create capabilities info on onDismissCreateCapabilitiesInfo', () => {
    component.showCreateCapabilitiesInfo = true;
    component.onDismissCreateCapabilitiesInfo();
    expect(component.showCreateCapabilitiesInfo).toBeFalse();
  });

  it('should hide select requirements warning on onDismissSelectRequirementsWarning', () => {
    component.showSelectRequirementsWarning = true;
    component.onDismissSelectRequirementsWarning();
    expect(component.showSelectRequirementsWarning).toBeFalse();
  });

  it('should hide select entity info on onDismissSelectEntityInfo', () => {
    component.showSelectEntityInfo = true;
    component.onDismissSelectEntityInfo();
    expect(component.showSelectEntityInfo).toBeFalse();
  });
});
