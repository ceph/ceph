import { ComponentFixture, TestBed } from '@angular/core/testing';
import { By } from '@angular/platform-browser';

import { TextInputListComponent } from './text-input-list.component';

describe('TextInputListComponent', () => {
  let component: TextInputListComponent;
  let fixture: ComponentFixture<TextInputListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TextInputListComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(TextInputListComponent);
    component = fixture.componentInstance;
    component.addButtonLabel = 'Add network mask';
    component.registerOnChange(jasmine.createSpy('onChange'));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render values without an auto-appended empty trailing input', () => {
    component.writeValue(['10.0.0.0/24', '192.168.1.0/24']);
    fixture.detectChanges();

    const inputs = fixture.debugElement.queryAll(By.css('.cds-input-group input'));
    expect(inputs.length).toBe(2);
    expect(inputs[0].nativeElement.value).toBe('10.0.0.0/24');
    expect(inputs[1].nativeElement.value).toBe('192.168.1.0/24');
  });

  it('should render label then helper text before the inputs', () => {
    component.label = 'Gateway discovery networks';
    component.helperText =
      'Specify subnet masks to discover gateway listeners for this gateway group.';
    component.writeValue([]);
    fixture.detectChanges();

    const label = fixture.debugElement.query(By.css('cds-text-label'));
    const helper = fixture.debugElement.query(By.css('.cds--form__helper-text'));
    const input = fixture.debugElement.query(By.css('.cds-input-group input'));
    expect(label.nativeElement.textContent.trim()).toContain('Gateway discovery networks');
    expect(helper.nativeElement.textContent).toContain('Specify subnet masks');

    const ordered = Array.from(
      fixture.nativeElement.querySelectorAll(
        'cds-text-label, .cds--form__helper-text, .cds-input-group input'
      )
    );
    expect(ordered[0]).toBe(label.nativeElement);
    expect(ordered[1]).toBe(helper.nativeElement);
    expect(ordered[2]).toBe(input.nativeElement);
  });

  it('should render one empty input for an empty value', () => {
    component.writeValue([]);
    fixture.detectChanges();

    const inputs = fixture.debugElement.queryAll(By.css('.cds-input-group input'));
    expect(inputs.length).toBe(1);
    expect(inputs[0].nativeElement.value).toBe('');
  });

  it('should add an empty input when the add button is clicked', () => {
    component.writeValue(['10.0.0.0/24']);
    fixture.detectChanges();

    const addButton = fixture.debugElement.query(By.css('button[cdsButton="tertiary"]'));
    addButton.triggerEventHandler('click', null);
    fixture.detectChanges();

    expect(component.values).toEqual(['10.0.0.0/24', '']);
    expect(fixture.debugElement.queryAll(By.css('.cds-input-group input')).length).toBe(2);
  });

  it('should emit non-empty values on input change', () => {
    component.writeValue(['10.0.0.0/24']);
    component.onInputChange(0, '192.168.0.0/16');

    expect((component as any).onChange).toHaveBeenCalledWith(['192.168.0.0/16']);
  });

  it('should clear the last remaining input instead of removing the row', () => {
    component.writeValue(['10.0.0.0/24']);
    component.deleteInput(0);

    expect(component.values).toEqual(['']);
    expect((component as any).onChange).toHaveBeenCalledWith([]);
  });

  it('should remove a row when more than one input exists', () => {
    component.writeValue(['10.0.0.0/24', '192.168.1.0/24']);
    component.deleteInput(0);

    expect(component.values).toEqual(['192.168.1.0/24']);
    expect((component as any).onChange).toHaveBeenCalledWith(['192.168.1.0/24']);
  });

  it('should show the trash button only when there is more than one row', () => {
    component.writeValue(['10.0.0.0/24']);
    fixture.detectChanges();
    expect(fixture.debugElement.queryAll(By.css('cds-icon-button')).length).toBe(0);

    component.writeValue(['10.0.0.0/24', '192.168.1.0/24']);
    fixture.detectChanges();
    expect(fixture.debugElement.queryAll(By.css('cds-icon-button')).length).toBe(2);
  });

  it('should show caller-provided invalidText when invalid is set', () => {
    component.invalid = true;
    component.invalidText = 'Custom list error';
    fixture.detectChanges();

    expect(component.showInvalid).toBe(true);
    expect(fixture.nativeElement.textContent).toContain('Custom list error');
  });

  it('should not show an error message when invalidText is empty', () => {
    component.invalid = true;
    component.invalidText = '';
    fixture.detectChanges();

    expect(component.showInvalid).toBe(false);
    expect(fixture.debugElement.query(By.css('.cds--form-requirement'))).toBeNull();
  });
});
