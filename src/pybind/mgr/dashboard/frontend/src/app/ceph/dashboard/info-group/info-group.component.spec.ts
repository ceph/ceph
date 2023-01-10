import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NgbPopoverModule } from '@ng-bootstrap/ng-bootstrap';

import { SharedModule } from '~/app/shared/shared.module';
import { configureTestBed } from '~/testing/unit-test-helper';
import { InfoGroupComponent } from './info-group.component';

describe('InfoGroupComponent', () => {
  let component: InfoGroupComponent;
  let fixture: ComponentFixture<InfoGroupComponent>;

  configureTestBed({
    imports: [NgbPopoverModule, SharedModule, HttpClientTestingModule],
    declarations: [InfoGroupComponent]
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(InfoGroupComponent);
    component = fixture.componentInstance;
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('Setting groupTitle makes title visible', () => {
    const groupTitle = 'Group Title';
    component.groupTitle = groupTitle;
    fixture.detectChanges();
    const titleDiv = fixture.debugElement.nativeElement.querySelector('.info-group-title');

    expect(titleDiv.textContent).toContain(groupTitle);
  });
});
