import {
  AfterContentInit,
  Component,
  ElementRef,
  EventEmitter,
  Input,
  Output
} from '@angular/core';

@Component({
  selector: 'cd-side-panel',
  templateUrl: './side-panel.component.html',
  styleUrl: './side-panel.component.scss'
})
export class SidePanelComponent implements AfterContentInit {
  @Input() expanded = false;
  @Input() headerText = '';
  @Input() overlay = true;
  @Input() size: 'sm' | 'md' | 'lg' = 'lg';

  @Output() closed = new EventEmitter<void>();
  hasFooter = false;

  constructor(private host: ElementRef) {}

  ngAfterContentInit() {
    // Look for .panel-footer inside projected content
    const footer = this.host.nativeElement.querySelector('.panel-footer');
    this.hasFooter = !!footer;
  }

  close() {
    this.closed.emit();
  }
}
