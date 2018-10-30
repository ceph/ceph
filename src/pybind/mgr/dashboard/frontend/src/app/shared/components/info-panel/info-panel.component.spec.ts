import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AlertModule } from 'ngx-bootstrap/alert';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { InfoPanelComponent } from './info-panel.component';

describe('InfoPanelComponent', () => {
  let component: InfoPanelComponent;
  let fixture: ComponentFixture<InfoPanelComponent>;

  configureTestBed({
    declarations: [InfoPanelComponent],
    imports: [AlertModule.forRoot()],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(InfoPanelComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
