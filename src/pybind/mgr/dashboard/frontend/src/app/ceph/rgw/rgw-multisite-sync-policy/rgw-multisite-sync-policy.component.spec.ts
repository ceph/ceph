import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwMultisiteSyncPolicyComponent } from './rgw-multisite-sync-policy.component';
import { HttpClientModule } from '@angular/common/http';
import { TitleCasePipe } from '@angular/common';

describe('RgwMultisiteSyncPolicyComponent', () => {
  let component: RgwMultisiteSyncPolicyComponent;
  let fixture: ComponentFixture<RgwMultisiteSyncPolicyComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwMultisiteSyncPolicyComponent],
      imports: [HttpClientModule],
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
