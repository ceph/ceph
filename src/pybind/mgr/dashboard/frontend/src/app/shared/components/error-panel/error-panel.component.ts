import { Component, EventEmitter, Input, Output } from '@angular/core';

@Component({
  selector: 'cd-error-panel',
  templateUrl: './error-panel.component.html',
  styleUrls: ['./error-panel.component.scss']
})
export class ErrorPanelComponent {
  /**
   * The title to be displayed. Defaults to 'Error'.
   * @type {string}
   */
  @Input() title = 'Error';

  /**
   * The event that is triggered when the 'Back' button is pressed.
   * @type {EventEmitter<any>}
   */
  @Output() backAction = new EventEmitter();
}
