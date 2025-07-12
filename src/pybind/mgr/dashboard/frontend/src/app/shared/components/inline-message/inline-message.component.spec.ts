import { ComponentFixture, TestBed } from '@angular/core/testing';

import { InlineMessageComponent } from './inline-message.component';

describe('InlineMessageComponent', () => {
  let component: InlineMessageComponent;
  let fixture: ComponentFixture<InlineMessageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [InlineMessageComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(InlineMessageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should have default values for inputs and properties', () => {
    expect(component.dismissible).toBe(false);
    expect(component.title).toBe('');
    expect(component.isCollapsed).toBe(false);
    expect(component.isTruncated).toBe(false);
    expect(component.icons).toBeDefined();
  });

  it('should set isCollapsed and isTruncated to true on onClose()', () => {
    component.isCollapsed = false;
    component.isTruncated = false;
    component.onClose();
    expect(component.isCollapsed).toBe(true);
    expect(component.isTruncated).toBe(true);
  });

  it('should toggle isTruncated on toggleContent()', () => {
    component.isTruncated = false;
    component.toggleContent();
    expect(component.isTruncated).toBe(true);
    component.toggleContent();
    expect(component.isTruncated).toBe(false);
  });

  it('should accept input values for dismissible and title', () => {
    component.dismissible = true;
    component.title = 'Test Title';
    expect(component.dismissible).toBe(true);
    expect(component.title).toBe('Test Title');
  });
});
