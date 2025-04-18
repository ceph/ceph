import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormsModule } from '@angular/forms';
import { By } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { RouterTestingModule } from '@angular/router/testing';

import { NgbNavModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { ConfigurationDetailsComponent } from './configuration-details/configuration-details.component';
import { ConfigurationComponent } from './configuration.component';
import { TableComponent } from '~/app/shared/datatable/table/table.component';

describe('ConfigurationComponent', () => {
  let component: ConfigurationComponent;
  let fixture: ComponentFixture<ConfigurationComponent>;

  configureTestBed({
    declarations: [ConfigurationComponent, ConfigurationDetailsComponent, TableComponent],
    imports: [
      BrowserAnimationsModule,
      SharedModule,
      FormsModule,
      NgbNavModule,
      HttpClientTestingModule,
      RouterTestingModule
    ]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConfigurationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  // TODO: Re-write this unit test to reflect latest changes on datatble markup
  it.skip('should check header text', () => {
    const cdTableEl = fixture.debugElement.query(By.directive(TableComponent));
    const cdTableComponent: TableComponent = cdTableEl.componentInstance;
    cdTableComponent.ngAfterViewInit();
    fixture.detectChanges();
    const actual = fixture.debugElement.query(By.css('thead')).nativeElement.textContent.trim();
    const expected = 'Name  Description  Current value  Default  Editable';
    expect(actual).toBe(expected);
  });
});
