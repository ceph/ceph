import { Component, Input } from '@angular/core';
import { Icons } from '../../enum/icons.enum';

@Component({
  selector: 'cd-inline-message',
  templateUrl: './inline-message.component.html',
  styleUrl: './inline-message.component.scss'
})
export class InlineMessageComponent {
  @Input()
  dismissible = false;
  @Input()
  title = '';

  isCollapsed = false;
  isTruncated = false;
  icons = Icons;

  onClose() {
    this.isCollapsed = true;
    this.isTruncated = true;
  }

  toggleContent() {
    this.isTruncated = !this.isTruncated;
  }
}
