import { ComponentFixture, TestBed } from '@angular/core/testing';
import { InlineMessageComponent } from './inline-message.component';
import { ButtonModule, IconModule, LayoutModule } from 'carbon-components-angular';

describe('InlineMessageComponent', () => {
  let component: InlineMessageComponent;
  let fixture: ComponentFixture<InlineMessageComponent>;
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [InlineMessageComponent],
      imports: [LayoutModule, IconModule, ButtonModule]
    }).compileComponents();

    fixture = TestBed.createComponent(InlineMessageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have default values for inputs', () => {
    expect(component.collapsible).toBe(false);
    expect(component.title).toBe('');
    expect(component.onClose).toBeDefined();
  });

  it('should set collapsible input', () => {
    component.collapsible = true;
    expect(component.collapsible).toBe(true);
  });

  it('should set title input', () => {
    component.title = 'Test Title';
    expect(component.title).toBe('Test Title');
  });

  it('should call onClose and set isDismissed to true when Close is called', () => {
    const onCloseSpy = jasmine.createSpy('onClose');
    component.onClose = onCloseSpy;
    component.isDismissed = false;
    component.close();
    expect(component.isDismissed).toBe(true);
    expect(onCloseSpy).toHaveBeenCalled();
  });

  it('should toggle isTruncated when toggleContent is called', () => {
    component.isTruncated = false;
    component.toggleContent();
    expect(component.isTruncated).toBe(true);
    component.toggleContent();
    expect(component.isTruncated).toBe(false);
  });

  it('should have icons property set to Icons enum', () => {
    expect(component.icons).toBeDefined();
  });
});
