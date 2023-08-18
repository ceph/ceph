import { Component, Input } from '@angular/core';
import { RoutedTabsInterface } from '../../models/routed-tab.interface';

@Component({
  selector: 'cd-routed-tabs',
  templateUrl: './routed-tabs.component.html',
  styleUrls: ['./routed-tabs.component.scss']
})
export class RoutedTabsComponent {
  @Input() tabs: RoutedTabsInterface[];

  @Input() activeTab?: string;

  constructor() {}
}
