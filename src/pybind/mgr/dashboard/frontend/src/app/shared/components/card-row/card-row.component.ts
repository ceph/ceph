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

  icons = Icons;
  statusList: {}[];
  successKeys: string[] = ['success', 'in', 'up'];

  ngOnInit(): void {
    this.getStatus();
  }

  ngOnChanges(): void {
    if (this.title === 'PG') {
      this.data = this.data.categoryPgAmount;
    }
    this.getStatus();
  }

  getStatus() {
    this.statusList = [];
    for (var key in this.data) {
      if (key !== 'total' && this.data[key]) {
        switch (key) {
          case 'success':
          case 'clean':
          case 'up':
          case 'in':
            if (this.data[key] !== this.data['total']) {
              this.statusList.push({
                value: this.data[key],
                class: 'text-success',
                icon: this.icons.success,
                tooltip: key
              });
            }
            break;
          case 'info':
            this.statusList.push({
              value: this.data[key],
              class: 'text-info',
              icon: this.icons.danger,
              tooltip: key
            });
            break;
          case 'warn':
          case 'warning':
            this.statusList.push({
              value: this.data[key],
              class: 'text-warning',
              icon: this.icons.warning,
              tooltip: key
            });
            break;
          case 'nearFull':
            this.statusList.push({
              value: this.data[key],
              class: 'text-warning',
              icon: '',
              tooltip: key
            });
            break;
          case 'error':
          case 'unknown':
          case 'down':
            this.statusList.push({
              value: this.data[key],
              class: 'text-danger',
              icon: this.icons.danger,
              tooltip: key
            });
            break;
          case 'full':
          case 'out':
            this.statusList.push({
              value: this.data[key],
              class: 'text-danger',
              icon: '',
              tooltip: key
            });
            break;
          case 'working':
            this.statusList.push({
              value: this.data[key],
              class: 'text-warning',
              icon: this.icons.spinner,
              tooltip: key
            });
            break;
          default:
            this.statusList.push({ value: this.data[key], class: '', tooltip: key });
            break;
        }
      }
    }
    this.statusList;
  }
}
