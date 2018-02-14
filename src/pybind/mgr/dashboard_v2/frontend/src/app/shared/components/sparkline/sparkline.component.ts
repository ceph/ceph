import { Component, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import { Input } from '@angular/core';

@Component({
  selector: 'cd-sparkline',
  templateUrl: './sparkline.component.html',
  styleUrls: ['./sparkline.component.scss']
})
export class SparklineComponent implements OnInit, OnChanges {
  @Input() data: any;
  @Input()
  style = {
    height: '30px',
    width: '100px'
  };

  public colors: Array<any> = [
    {
      backgroundColor: 'rgba(40,140,234,0.2)',
      borderColor: 'rgba(40,140,234,1)',
      pointBackgroundColor: 'rgba(40,140,234,1)',
      pointBorderColor: '#fff',
      pointHoverBackgroundColor: '#fff',
      pointHoverBorderColor: 'rgba(40,140,234,0.8)'
    }
  ];

  options = {
    animation: {
      duration: 0
    },
    responsive: true,
    maintainAspectRatio: false,
    legend: {
      display: false
    },
    elements: {
      line: {
        borderWidth: 1
      }
    },
    tooltips: {
      enabled: true
    },
    scales: {
      yAxes: [
        {
          display: false
        }
      ],
      xAxes: [
        {
          display: false
        }
      ]
    }
  };

  public datasets: Array<any> = [
    {
      data: []
    }
  ];

  public labels: Array<any> = [];

  constructor() {}

  ngOnInit() {}

  ngOnChanges(changes: SimpleChanges) {
    this.datasets[0].data = changes['data'].currentValue;
    this.datasets = [...this.datasets];
    this.labels = [...Array(changes['data'].currentValue.length)];
  }
}
