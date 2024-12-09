import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwUserAccountsComponent } from './rgw-user-accounts.component';

describe('RgwUserAccountsComponent', () => {
  let component: RgwUserAccountsComponent;
  let fixture: ComponentFixture<RgwUserAccountsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwUserAccountsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwUserAccountsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
