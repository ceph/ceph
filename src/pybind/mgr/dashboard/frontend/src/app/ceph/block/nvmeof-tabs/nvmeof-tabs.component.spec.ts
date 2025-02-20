import { ComponentFixture, TestBed } from '@angular/core/testing';

import { NvmeofTabsComponent } from './nvmeof-tabs.component';

describe('NvmeofTabsComponent', () => {
  let component: NvmeofTabsComponent;
  let fixture: ComponentFixture<NvmeofTabsComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [NvmeofTabsComponent]
    }).compileComponents();

    fixture = TestBed.createComponent(NvmeofTabsComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
