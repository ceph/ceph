import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ClickableTileComponent } from './clickable-tile.component';

describe('ClickableTileComponent', () => {
  let component: ClickableTileComponent;
  let fixture: ComponentFixture<ClickableTileComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ClickableTileComponent],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();

    fixture = TestBed.createComponent(ClickableTileComponent);
    component = fixture.componentInstance;
    component.title = 'Set up mirroring';
    component.description = 'Configure mirroring for a filesystem.';
    component.icon = 'replicate';
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render title and description', () => {
    const element: HTMLElement = fixture.nativeElement;

    expect(element.textContent).toContain('Set up mirroring');
    expect(element.textContent).toContain('Configure mirroring for a filesystem.');
  });

  it('should emit tileClick when the tile is clicked', () => {
    const tileClickSpy = jest.fn();
    component.tileClick.subscribe(tileClickSpy);

    fixture.nativeElement.querySelector('cds-clickable-tile')?.dispatchEvent(new Event('click'));

    expect(tileClickSpy).toHaveBeenCalledTimes(1);
  });
});
