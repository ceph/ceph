import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap } from '@angular/router';
import { of } from 'rxjs';
import { NO_ERRORS_SCHEMA } from '@angular/core';

import { RgwUserAccountsResourceSidebarComponent } from './rgw-user-accounts-resource-sidebar.component';

describe('RgwUserAccountsResourceSidebarComponent', () => {
  let component: RgwUserAccountsResourceSidebarComponent;
  let fixture: ComponentFixture<RgwUserAccountsResourceSidebarComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwUserAccountsResourceSidebarComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: of(convertToParamMap({ accountName: 'account1' })),
            data: of({
              account: {
                id: 'RGW11111111111111111',
                name: 'account1'
              }
            })
          }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwUserAccountsResourceSidebarComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should build sidebar items', () => {
    expect(component.sidebarItems.length).toBe(2);
  });

  it('should expose resolved account name for header', () => {
    expect(component.accountName).toBe('account1');
  });
});
