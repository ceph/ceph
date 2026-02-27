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
});
