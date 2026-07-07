import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DetailsCardComponent } from './details-card.component';
import { ProductiveCardComponent } from '../productive-card/productive-card.component';

describe('DetailsCardComponent', () => {
  let component: DetailsCardComponent;
  let fixture: ComponentFixture<DetailsCardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [DetailsCardComponent],
      imports: [ProductiveCardComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(DetailsCardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should filter hidden details', () => {
    component.details = [
      { label: 'Visible', value: 'test', type: 'text' },
      { label: 'Hidden', value: 'test', type: 'text', hidden: true }
    ];
    const visible = component.getVisibleDetails();
    expect(visible.length).toBe(1);
    expect(visible[0].label).toBe('Visible');
  });

  it('should detect disabled status', () => {
    expect(component.isStatusDisabled('Disabled')).toBe(true);
    expect(component.isStatusDisabled('disabled')).toBe(true);
    expect(component.isStatusDisabled('Enabled')).toBe(false);
  });

  it('should emit editClicked event', () => {
    spyOn(component.editClicked, 'emit');
    component.onEditClick();
    expect(component.editClicked.emit).toHaveBeenCalled();
  });
  it('should handle empty details array', () => {
    component.details = [];
    const visible = component.getVisibleDetails();
    expect(visible.length).toBe(0);
  });

  it('should handle undefined details', () => {
    component.details = undefined;
    const visible = component.getVisibleDetails();
    expect(visible.length).toBe(0);
  });
});
