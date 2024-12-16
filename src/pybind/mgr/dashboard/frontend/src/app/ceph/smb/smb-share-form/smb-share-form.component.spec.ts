import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbShareFormComponent } from './smb-share-form.component';

describe('SmbShareFormComponent', () => {
  let component: SmbShareFormComponent;
  let fixture: ComponentFixture<SmbShareFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SmbShareFormComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbShareFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
