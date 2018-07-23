import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AlertModule } from 'ngx-bootstrap';

import { configureTestBed } from '../../unit-test-helper';
import { WarningPanelComponent } from './warning-panel.component';

describe('WarningPanelComponent', () => {
  let component: WarningPanelComponent;
  let fixture: ComponentFixture<WarningPanelComponent>;

  configureTestBed({
    declarations: [WarningPanelComponent],
    imports: [AlertModule.forRoot()]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(WarningPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
