import { Component, ElementRef, Input, OnInit, ViewChild } from '@angular/core';
import { UsageService } from '../../services/usage.service';

@Component({
  selector: 'cd-usage-bar',
  templateUrl: './usage-bar.component.html',
  styleUrls: ['./usage-bar.component.scss']
})
export class UsageBarComponent implements OnInit {
  @Input() total: number = 0;
  @Input() used: any = 0;
  @Input() warningThreshold?: number = 0;
  @Input() errorThreshold?: number = 0;
  @Input() isBinary = true;
  @Input() decimals = 0;
  @Input() title = $localize`usage`;

  @ViewChild('myComponent') myComponent!: ElementRef;

  color: string = '#0043ce';
  data: any = [{group: $localize `Capacity`, value: this.used}]
  options = {
    resizable: false,
    meter: {
      showLabels: false
    },
    tooltip: {
      enabled: true
    },
    color: {
      scale: {
        Capacity: '#0043ce'
      }
    },
    height: '1rem',
    width: '12rem',
    toolbar: {
      enabled: false
    }
  };

  constructor(private usageService: UsageService) {}

  ngOnInit() {
    //TODO: need to remove
    console.log(getComputedStyle(document.body).getPropertyValue('cds-support-error'))
    const { usedPercentage, color } = this.usageService.getUsageInfo(
      this.used,
      this.total,
      this.warningThreshold,
      this.errorThreshold
    );

    this.data[0].value = usedPercentage;
    this.color = color;
  }
}
