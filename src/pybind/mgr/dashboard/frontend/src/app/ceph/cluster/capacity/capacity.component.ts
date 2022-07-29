import { Component, OnInit, ViewChild } from '@angular/core';
import _ from 'lodash';
import { CapacityService } from '~/app/shared/api/capacity.service';
import { BaseChartDirective } from 'ng2-charts'
import  * as Chart from 'chart.js'

@Component({
  selector: 'cd-capacity',
  templateUrl: './capacity.component.html',
  styleUrls: ['./capacity.component.scss']
})
export class CapacityComponent implements OnInit {
  @ViewChild("canvasChart")
  chart: BaseChartDirective;

  public barChartOptions: Chart.ChartOptions = {
    responsive: true,
    scales: {
      yAxes: [{
        type: "linear"
      }]
    }
  };
  public barChartOptionsLog: Chart.ChartOptions = {
    responsive: true,
    scales: {
      yAxes: [{
        type: "logarithmic"
      }]
    }
  };
  public labels: string[] = [];
  public barChartLegend: boolean = true;
  public barChartData: any[] = [
  ];
  chartType: string = "line";

  constructor(private capacityService: CapacityService) { }

  ngOnInit(): void {
    this.capacityService.list().subscribe(data => this.getData(data));
  }

  getData(data: any) {
    let actual: any[] = []
    let quadratic: any[] = []
    let linear: any[] = []
    let exp: any[] = []
    let labels: any[] = []
    data.actual[0].map((element: any, i: any) => {
      const y = Math.max(data.actual[1][i], 0);
      const d = new Date(element);
      const dateStr = d.toDateString();
      actual.push({x: dateStr, y: y});
    });
    data.quadratic[0].map((element: any, i: any) => {
      const y = Math.max(data.quadratic[1][i], 0);
      const d = new Date(element);
      const dateStr = d.toDateString();
      labels.push(dateStr);
      quadratic.push({x: dateStr, y: y});
    });
    data.linear[0].map((element: any, i: any) => {
      const y = Math.max(data.linear[1][i], 0);
      const d = new Date(element);
      const dateStr = d.toDateString();
      linear.push({x: dateStr, y: y});
    });
    data.exp[0].map((element: any, i: any) => {
      const y = Math.max(data.exp[1][i], 0);
      const d = new Date(element);
      const dateStr = d.toDateString();
      exp.push({x: dateStr, y: y});
    });
    this.labels = labels;
    this.barChartData = [
      {data: actual, label: 'actual'},
      {data: quadratic, label: 'Quadratic prediction'},
      {data: linear, label: 'Linear prediction'},
      {data: exp, label: 'Exponential prediction'},
    ];
  }
}
