import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AlertModule } from 'ngx-bootstrap/alert';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { LoadingPanelComponent } from './loading-panel.component';

describe('LoadingPanelComponent', () => {
  let component: LoadingPanelComponent;
  let fixture: ComponentFixture<LoadingPanelComponent>;

  configureTestBed({
    declarations: [LoadingPanelComponent],
    imports: [AlertModule.forRoot()]
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
