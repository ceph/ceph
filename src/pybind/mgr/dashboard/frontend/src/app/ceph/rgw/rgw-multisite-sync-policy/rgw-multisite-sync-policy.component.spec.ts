import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwMultisiteSyncPolicyComponent } from './rgw-multisite-sync-policy.component';
import { HttpClientModule } from '@angular/common/http';
import { TitleCasePipe } from '@angular/common';
import { ToastrModule } from 'ngx-toastr';
import { PipesModule } from '~/app/shared/pipes/pipes.module';

describe('RgwMultisiteSyncPolicyComponent', () => {
  let component: RgwMultisiteSyncPolicyComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncPolicyComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteSyncPolicyComponent],
      imports: [HttpClientModule, ToastrModule.forRoot(), PipesModule],
      providers: [TitleCasePipe]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwMultisiteSyncPolicyComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
