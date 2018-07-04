import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AlertModule } from 'ngx-bootstrap';

import { configureTestBed } from '../../../../testing/unit-test-helper';
import { ErrorPanelComponent } from '../error-panel/error-panel.component';
import { InfoPanelComponent } from '../info-panel/info-panel.component';
import { WarningPanelComponent } from '../warning-panel/warning-panel.component';
import { ViewCacheComponent } from './view-cache.component';

describe('ViewCacheComponent', () => {
  let component: ViewCacheComponent;
  let fixture: ComponentFixture<ViewCacheComponent>;

  configureTestBed({
    declarations: [
      ErrorPanelComponent,
      InfoPanelComponent,
      ViewCacheComponent,
      WarningPanelComponent
    ],
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
