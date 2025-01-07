import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwUserAccountsDetailsComponent } from './rgw-user-accounts-details.component';

describe('RgwUserAccountsDetailsComponent', () => {
  let component: RgwUserAccountsDetailsComponent;
  let fixture: ComponentFixture<RgwUserAccountsDetailsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwUserAccountsDetailsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwUserAccountsDetailsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
