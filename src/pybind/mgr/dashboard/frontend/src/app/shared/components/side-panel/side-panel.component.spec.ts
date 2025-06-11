import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SidePanelComponent } from './side-panel.component';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('SidePanelComponent', () => {
  let component: SidePanelComponent;
  let fixture: ComponentFixture<SidePanelComponent>;

  configureTestBed({
    declarations: [SidePanelComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SidePanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should show overlay only when expanded is true and slideOver is false', () => {
    component.expanded = true;
    component.overlay = true;
    fixture.detectChanges();

    const overlay = fixture.nativeElement.querySelector('.overlay');
    expect(overlay).toBeTruthy();
  });

  it('should call close when overlay is clicked', () => {
    spyOn(component, 'close');
    component.expanded = true;
    component.overlay = true;
    fixture.detectChanges();

    const overlay = fixture.nativeElement.querySelector('.overlay');
    overlay.click();
    expect(component.close).toHaveBeenCalled();
  });

  it('should call close when close button is clicked', () => {
    spyOn(component, 'close');
    component.expanded = true;
    fixture.detectChanges();

    const button = fixture.nativeElement.querySelector('cds-icon-button');
    button.click();
    expect(component.close).toHaveBeenCalled();
  });

  it('should show header text when headerText is provided', () => {
    component.headerText = 'Test header text';
    fixture.detectChanges();

    const header = fixture.nativeElement.querySelector('.panel-header');
    expect(header.textContent).toContain('Test header text');
  });
});
