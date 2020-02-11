import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BsModalService } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../shared/shared.module';
import { RgwUserDetailsComponent } from './rgw-user-details.component';

describe('RgwUserDetailsComponent', () => {
  let component: RgwUserDetailsComponent;
  let fixture: ComponentFixture<RgwUserDetailsComponent>;

  configureTestBed({
    declarations: [RgwUserDetailsComponent],
    imports: [HttpClientTestingModule, SharedModule, TabsModule.forRoot()],
    providers: [BsModalService, i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwUserDetailsComponent);
    component = fixture.componentInstance;
    component.selection = new CdTableSelection();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should show correct "System" info', () => {
    component.selection.selected = [
      { uid: '', email: '', system: 'true', keys: [], swift_keys: [] }
    ];
    component.ngOnChanges();
    fixture.detectChanges();

    const detailsTab = fixture.debugElement.nativeElement.querySelectorAll(
      '.table.table-striped.table-bordered tr td'
    );
    expect(detailsTab[6].textContent).toEqual('System');
    expect(detailsTab[7].textContent).toEqual('Yes');

    component.selection.selected[0].system = 'false';
    component.ngOnChanges();
    fixture.detectChanges();

    expect(detailsTab[7].textContent).toEqual('No');
  });
});
