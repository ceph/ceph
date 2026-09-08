import { ComponentFixture, TestBed } from '@angular/core/testing';

import { RgwStorageClassListComponent } from './rgw-storage-class-list.component';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { SharedModule } from '~/app/shared/shared.module';

import { RouterTestingModule } from '@angular/router/testing';

describe('RgwStorageClassListComponent', () => {
  let component: RgwStorageClassListComponent;
  let fixture: ComponentFixture<RgwStorageClassListComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, SharedModule, RouterTestingModule],
      providers: [NgbActiveModal],
      declarations: [RgwStorageClassListComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(RgwStorageClassListComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
