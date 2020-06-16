import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BsModalService } from 'ngx-bootstrap/modal';
import { TabsModule } from 'ngx-bootstrap/tabs';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../shared/shared.module';
import { RgwUserS3Key } from '../models/rgw-user-s3-key';
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

    const detailsTab = fixture.debugElement.nativeElement.querySelector('tab[heading="Details"]');
    expect(detailsTab).toBeFalsy();
    const keysTab = fixture.debugElement.nativeElement.querySelector('tab[heading="Keys"]');
    expect(keysTab).toBeFalsy();
  });

  it('should show "Details" tab', () => {
    component.selection.selected = [{ uid: 'myUsername' }];
    component.selection.hasSingleSelection = true;
    fixture.detectChanges();

    const detailsTab = fixture.debugElement.nativeElement.querySelector('tab[heading="Details"]');
    expect(detailsTab).toBeTruthy();
    const keysTab = fixture.debugElement.nativeElement.querySelector('tab[heading="Keys"]');
    expect(keysTab).toBeFalsy();
  });

  it('should show "Keys" tab', () => {
    const s3Key = new RgwUserS3Key();
    component.selection.selected = [{ keys: [s3Key] }];
    component.selection.hasSingleSelection = true;
    component.selection.hasSelection = true;
    component.ngOnChanges();
    fixture.detectChanges();

    const detailsTab = fixture.debugElement.nativeElement.querySelector('tab[heading="Details"]');
    expect(detailsTab).toBeTruthy();
    const keysTab = fixture.debugElement.nativeElement.querySelector('tab[heading="Keys"]');
    expect(keysTab).toBeTruthy();
  });

  it('should show correct "System" info', () => {
    component.selection.selected = [
      { uid: '', email: '', system: 'true', keys: [], swift_keys: [] }
    ];
    component.selection.hasSelection = true;
    component.selection.hasSingleSelection = true;
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
