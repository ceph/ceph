import { Component, Input } from '@angular/core';
import { IconSize } from '../../enum/icons.enum';

@Component({
  selector: 'cd-inline-message',
  templateUrl: './inline-message.component.html',
  styleUrl: './inline-message.component.scss',
  standalone: false
})
export class InlineMessageComponent {
  // collapsible when true will show read more/read less button
  @Input()
  collapsible = false;
  // title to show in the message

  @Input()
  title = '';

  // callback function to execute onclose
  @Input()
  onClose?: () => void = () => {};

  isTruncated = false;
  iconSize = IconSize;
  isDismissed = false;

  close() {
    this.isDismissed = true;
    if (this.onClose) {
      this.onClose();
    }
  }

  toggleContent() {
    this.isTruncated = !this.isTruncated;
  }
}
