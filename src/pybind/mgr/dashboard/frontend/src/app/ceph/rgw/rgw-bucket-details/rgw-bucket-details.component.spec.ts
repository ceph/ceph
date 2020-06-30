import { ComponentFixture, TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../../testing/unit-test-helper';
import { CdTableSelection } from '../../../shared/models/cd-table-selection';
import { SharedModule } from '../../../shared/shared.module';
import { RgwBucketDetailsComponent } from './rgw-bucket-details.component';

describe('RgwBucketDetailsComponent', () => {
  let component: RgwBucketDetailsComponent;
  let fixture: ComponentFixture<RgwBucketDetailsComponent>;

  configureTestBed({
    declarations: [RgwBucketDetailsComponent],
    imports: [SharedModule],
    providers: [i18nProviders]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwBucketDetailsComponent);
    component = fixture.componentInstance;
    component.selection = new CdTableSelection();
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
