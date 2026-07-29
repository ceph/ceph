import { configureTestBed } from '~/testing/unit-test-helper';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwMultisiteSyncPolicyComponent } from './rgw-multisite-sync-policy.component';
import { HttpClientModule } from '@angular/common/http';
import { TitleCasePipe } from '@angular/common';

import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { ModalModule } from 'carbon-components-angular';
import { RgwMultisiteTabsComponent } from '../rgw-multisite-tabs/rgw-multisite-tabs.component';
import { SharedModule } from '~/app/shared/shared.module';
import { RgwMultisiteSyncPolicyDetailsComponent } from '../rgw-multisite-sync-policy-details/rgw-multisite-sync-policy-details.component';
import { RouterTestingModule } from '@angular/router/testing';

describe('RgwMultisiteSyncPolicyComponent', () => {
  let component: RgwMultisiteSyncPolicyComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncPolicyComponent>;

  configureTestBed({
    declarations: [
      RgwMultisiteSyncPolicyComponent,
      RgwMultisiteTabsComponent,
      RgwMultisiteSyncPolicyDetailsComponent
    ],
    imports: [HttpClientModule, PipesModule, ModalModule, SharedModule, RouterTestingModule],
    providers: [TitleCasePipe]
  });

  beforeEach(async () => {
    fixture = TestBed.createComponent(RgwMultisiteSyncPolicyComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
