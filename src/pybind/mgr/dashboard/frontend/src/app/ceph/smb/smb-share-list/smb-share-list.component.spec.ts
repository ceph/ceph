import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbShareListComponent } from './smb-share-list.component';

describe('SmbShareListComponent', () => {
  let component: SmbShareListComponent;
  let fixture: ComponentFixture<SmbShareListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SmbShareListComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbShareListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
