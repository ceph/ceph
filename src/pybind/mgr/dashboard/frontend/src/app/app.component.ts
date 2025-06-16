import { Component, ViewChild } from '@angular/core';
import { NgbPopoverConfig, NgbTooltipConfig } from '@ng-bootstrap/ng-bootstrap';
import { CliTerminalComponent } from './shared/components/cli-console/cli-terminal.component';

@Component({
  selector: 'cd-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  @ViewChild(CliTerminalComponent) cliTerminal: CliTerminalComponent;
  constructor(popoverConfig: NgbPopoverConfig, tooltipConfig: NgbTooltipConfig) {
    popoverConfig.autoClose = 'outside';
    popoverConfig.container = 'body';
    popoverConfig.placement = 'bottom';
    tooltipConfig.container = 'body';
  }
}