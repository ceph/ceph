import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbAlertModule } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { LoadingPanelComponent } from './loading-panel.component';

describe('LoadingPanelComponent', () => {
  let component: LoadingPanelComponent;
  let fixture: ComponentFixture<LoadingPanelComponent>;

  configureTestBed({
    declarations: [LoadingPanelComponent],
    imports: [NgbAlertModule]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LoadingPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
