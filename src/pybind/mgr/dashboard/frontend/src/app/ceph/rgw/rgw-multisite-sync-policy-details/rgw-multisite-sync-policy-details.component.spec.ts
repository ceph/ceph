import { configureTestBed } from '~/testing/unit-test-helper';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwMultisiteSyncPolicyDetailsComponent } from './rgw-multisite-sync-policy-details.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { ModalModule } from 'carbon-components-angular';
import { SharedModule } from '~/app/shared/shared.module';

describe('RgwMultisiteSyncPolicyDetailsComponent', () => {
  let component: RgwMultisiteSyncPolicyDetailsComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncPolicyDetailsComponent>;

  configureTestBed({
    declarations: [RgwMultisiteSyncPolicyDetailsComponent],
    imports: [HttpClientTestingModule, PipesModule, ModalModule, SharedModule]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(RgwMultisiteSyncPolicyDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
