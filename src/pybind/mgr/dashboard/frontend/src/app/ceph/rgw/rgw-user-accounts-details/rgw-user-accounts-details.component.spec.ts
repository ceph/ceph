import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwUserAccountsDetailsComponent } from './rgw-user-accounts-details.component';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { TableKeyValueComponent } from '~/app/shared/datatable/table-key-value/table-key-value.component';
import { CdDatePipe } from '~/app/shared/pipes/cd-date.pipe';

describe('RgwUserAccountsDetailsComponent', () => {
  let component: RgwUserAccountsDetailsComponent;
  let fixture: ComponentFixture<RgwUserAccountsDetailsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwUserAccountsDetailsComponent, TableKeyValueComponent],
      providers: [DimlessBinaryPipe, CdDatePipe]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwUserAccountsDetailsComponent);
    component = fixture.componentInstance;
    component.selection = { quota: {}, bucket_quota: {} };
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
