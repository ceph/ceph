import { Component, Input, OnChanges } from '@angular/core';
import { Icons } from '~/app/shared/enum/icons.enum';

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

  icons = Icons;
  total: number;

  ngOnChanges(): void {
    if (this.data.total || this.data.total === 0) {
      this.total = this.data.total;
    } else if (this.summaryType === 'iscsi') {
      this.total = this.data.up + this.data.down || 0;
    } else {
      this.total = this.data;
    }
  }
}
