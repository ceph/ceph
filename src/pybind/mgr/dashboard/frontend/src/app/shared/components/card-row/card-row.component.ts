import { Component, Input, OnChanges } from '@angular/core';
import { Icons } from '~/app/shared/enum/icons.enum';
import { HardwareNameMapping } from '~/app/shared/enum/hardware.enum';

@Component({
  selector: 'cd-card-row',
  templateUrl: './card-row.component.html',
  styleUrls: ['./card-row.component.scss']
})
export class CardRowComponent implements OnChanges {
  @Input()
  title: string;

  @Input()
  link: string;

  @Input()
  data: any;

  @Input()
  summaryType = 'default';

  @Input()
  dropdownData: any;

  hwNames = HardwareNameMapping;
  icons = Icons;
  total: number;
  dropdownTotalError: number = 0;
  dropdownToggled: boolean = false;

  ngOnChanges(): void {
    if (this.data.total || this.data.total === 0) {
      this.total = this.data.total;
    } else if (this.summaryType === 'iscsi') {
      this.total = this.data.up + this.data.down || 0;
    } else {
      this.total = this.data;
    }

    if (this.dropdownData) {
      if (this.title == 'Host') {
        this.dropdownTotalError = this.dropdownData.host.flawed;
      }
    }
  }

  toggleDropdown(): void {
    this.dropdownToggled = !this.dropdownToggled;
  }
}
