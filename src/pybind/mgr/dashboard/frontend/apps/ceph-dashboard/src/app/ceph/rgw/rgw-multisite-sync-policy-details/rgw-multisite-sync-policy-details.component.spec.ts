import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwMultisiteSyncPolicyDetailsComponent } from './rgw-multisite-sync-policy-details.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ToastrModule } from 'ngx-toastr';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { ModalModule } from 'carbon-components-angular';

describe('RgwMultisiteSyncPolicyDetailsComponent', () => {
  let component: RgwMultisiteSyncPolicyDetailsComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncPolicyDetailsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteSyncPolicyDetailsComponent],
      imports: [HttpClientTestingModule, ToastrModule.forRoot(), PipesModule, ModalModule]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteSyncPolicyDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
