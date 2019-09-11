import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AlertModule } from 'ngx-bootstrap/alert';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { AlertPanelComponent } from '../alert-panel/alert-panel.component';
import { ViewCacheComponent } from './view-cache.component';

describe('ViewCacheComponent', () => {
  let component: ViewCacheComponent;
  let fixture: ComponentFixture<ViewCacheComponent>;

  configureTestBed({
    declarations: [ViewCacheComponent, AlertPanelComponent],
    imports: [AlertModule.forRoot()]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ViewCacheComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
