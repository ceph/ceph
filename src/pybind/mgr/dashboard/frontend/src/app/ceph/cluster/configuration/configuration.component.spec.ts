import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { SharedModule } from '../../../shared/shared.module';
import { ConfigurationDetailsComponent } from './configuration-details/configuration-details.component';
import { ConfigurationComponent } from './configuration.component';

describe('ConfigurationComponent', () => {
  let component: ConfigurationComponent;
  let fixture: ComponentFixture<ConfigurationComponent>;

  configureTestBed({
    declarations: [ConfigurationComponent, ConfigurationDetailsComponent],
    imports: [
      BrowserAnimationsModule,
      SharedModule,
      FormsModule,
      NgbNavModule,
      HttpClientTestingModule,
      RouterTestingModule
    ],
    providers: i18nProviders
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigurationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should check header text', () => {
    expect(fixture.debugElement.query(By.css('.datatable-header')).nativeElement.textContent).toBe(
      ['Name', 'Description', 'Current value', 'Default', 'Editable'].join('')
    );
  });
});
