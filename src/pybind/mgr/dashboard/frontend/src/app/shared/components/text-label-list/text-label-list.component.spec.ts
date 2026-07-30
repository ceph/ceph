import { ComponentFixture, TestBed } from '@angular/core/testing';
import { TextLabelListComponent } from './text-label-list.component';
import { By } from '@angular/platform-browser';

describe('TextLabelListComponent', () => {
  let component: TextLabelListComponent;
  let fixture: ComponentFixture<TextLabelListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TextLabelListComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(TextLabelListComponent);
    component = fixture.componentInstance;
    component.label = 'DNS Names';
    component.registerOnChange(jasmine.createSpy('onChange'));
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should call writeValue and render values', () => {
    component.writeValue(['foo', 'bar']);
    fixture.detectChanges();

    const inputs = fixture.debugElement.queryAll(By.css('cds-text-label input'));
    expect(inputs.length).toBe(3);
    expect(inputs[0].nativeElement.value).toBe('foo');
    expect(inputs[1].nativeElement.value).toBe('bar');
    expect(inputs[2].nativeElement.value).toBe('');
  });

  it('should call writeValue empty and render one', () => {
    component.writeValue([]);
    fixture.detectChanges();

    const inputs = fixture.debugElement.queryAll(By.css('cds-text-label input'));
    expect(inputs.length).toBe(1);
    expect(inputs[0].nativeElement.value).toBe('');
  });

  it('should treat a string value as a single item', () => {
    component.writeValue('openid profile email');
    fixture.detectChanges();

    const inputs = fixture.debugElement.queryAll(By.css('cds-text-label input'));
    expect(inputs.length).toBe(2);
    expect(inputs[0].nativeElement.value).toBe('openid profile email');
    expect(inputs[1].nativeElement.value).toBe('');
  });

  it('should not spread a string into individual characters', () => {
    component.writeValue('openid');
    fixture.detectChanges();

    const inputs = fixture.debugElement.queryAll(By.css('cds-text-label input'));
    expect(inputs.length).toBe(2);
    expect(inputs[0].nativeElement.value).toBe('openid');
  });

  it('should treat null like an empty list', () => {
    component.writeValue(null);
    fixture.detectChanges();

    const inputs = fixture.debugElement.queryAll(By.css('cds-text-label input'));
    expect(inputs.length).toBe(1);
    expect(inputs[0].nativeElement.value).toBe('');
  });

  it('should call onTouch on input changes', () => {
    spyOn(component as any, 'onTouched');

    component.onInputChange(0, 'foo');

    expect((component as any).onTouched).toHaveBeenCalled();
  });

  it('should update the value at the given index', () => {
    component.writeValue(['foo', '']);
    component.onInputChange(2, 'bar');

    expect(component.values[2]).toBe('bar');
  });

  it('should return non empty values on input changes', () => {
    component.writeValue(['foo', 'bar', '']);
    component.onInputChange(3, 'test');
    fixture.detectChanges();

    const inputs = fixture.debugElement.queryAll(By.css('cds-text-label input'));
    expect(inputs.length).toBe(5);
    expect((component as any).onChange).toHaveBeenCalledWith(['foo', 'bar', 'test']);
  });

  it('should remove the item at the given index', () => {
    component.writeValue(['foo', 'bar']);
    component.deleteInput(0);

    expect(component['values']).toEqual(['bar', '']);
    expect(component['onChange']).toHaveBeenCalledWith(['bar']);
  });

  it('should add an empty input if all items are deleted', () => {
    component.writeValue(['foo']);
    component.deleteInput(0);

    expect(component['values']).toEqual(['']);
    expect(component['onChange']).toHaveBeenCalledWith([]);
  });

  it('should update values correctly on deletion', () => {
    component.writeValue(['foo', 'bar', 'test']);
    component.deleteInput(1);

    expect(component['values']).toEqual(['foo', 'test', '']);
    expect(component['onChange']).toHaveBeenCalledWith(['foo', 'test']);
  });

  describe('label positioning', () => {
    it('should render the label text only in the first cds-text-label', () => {
      component.label = 'DNS Names';
      component.writeValue(['a', 'b']);
      fixture.detectChanges();

      const labels = fixture.debugElement.queryAll(By.css('cds-text-label'));
      // First label contains the visible label text
      const firstLabelText = labels[0].nativeElement.textContent;
      expect(firstLabelText).toContain('DNS Names');
    });

    it('should NOT render the label text in subsequent cds-text-label elements', () => {
      component.label = 'DNS Names';
      component.writeValue(['a', 'b']);
      fixture.detectChanges();

      const labels = fixture.debugElement.queryAll(By.css('cds-text-label'));
      // Second and third rows should not show the label text (uses zero-width space instead)
      for (let i = 1; i < labels.length; i++) {
        const textContent = labels[i].nativeElement.textContent;
        expect(textContent).not.toContain('DNS Names');
      }
    });

    it('should project a zero-width space as content for subsequent rows', () => {
      component.writeValue(['a', 'b']);
      fixture.detectChanges();

      // The template uses &#8203; for rows after the first; the component
      // must render 3 cds-text-label elements (a, b, and empty trailing input).
      const labels = fixture.debugElement.queryAll(By.css('cds-text-label'));
      expect(labels.length).toBe(3);
    });
  });
});
