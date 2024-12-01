import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbClusterFormComponent } from './smb-cluster-form.component';

describe('SmbClusterFormComponent', () => {
  let component: SmbClusterFormComponent;
  let fixture: ComponentFixture<SmbClusterFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SmbClusterFormComponent ]
    })
    .compileComponents();

    fixture = TestBed.createComponent(SmbClusterFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
