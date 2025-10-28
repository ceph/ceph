import { Component, Input } from '@angular/core';
import { Icons } from '../../enum/icons.enum';

@Component({
  selector: 'cd-inline-message',
  templateUrl: './inline-message.component.html',
  styleUrl: './inline-message.component.scss'
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
  icons = Icons;
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
