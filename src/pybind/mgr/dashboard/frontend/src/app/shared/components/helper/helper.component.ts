import { Component, Input } from '@angular/core';
import { ICON_TYPE, IconSize } from '../../enum/icons.enum';
import { HelperType } from '../../enum/cd-helper.enum';

@Component({
  selector: 'cd-helper',
  templateUrl: './helper.component.html',
  styleUrls: ['./helper.component.scss'],
  standalone: false
})
export class HelperComponent {
  private static nextId = 0;
  isPopoverOpen = false;
  helperType = HelperType;
  // Generating unique ID for tooltip to avoid duplicate-id accessibility violations
  tooltipId = `cd-tooltip-${++HelperComponent.nextId}`;

  // Tooltip: Displayed on hover or focus and contains contextual, helpful, and nonessential information.
  // Popover: Displayed on click and can contain varying text and interactive elements
  @Input() type: HelperType.tooltip | HelperType.popover = HelperType.tooltip; // Default to tooltip for backward compatibility
  @Input() class: string;
  @Input() html: any;
  @Input() iconSize = IconSize.size16;
  @Input() iconType = ICON_TYPE.info;

  togglePopover() {
    this.isPopoverOpen = !this.isPopoverOpen;
  }

  closePopover() {
    if (this.type === HelperType.popover && this.isPopoverOpen) {
      this.isPopoverOpen = false;
    }
  }
}
