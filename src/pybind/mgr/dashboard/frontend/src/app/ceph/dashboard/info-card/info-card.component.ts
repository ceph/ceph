import {Component, Input, OnInit} from '@angular/core';
import {InfoCard} from "./info-card";

@Component({
  selector: 'cd-info-card',
  templateUrl: './info-card.component.html',
  styleUrls: ['./info-card.component.scss']
})
export class InfoCardComponent {
  @Input('infoCard') infoCard: InfoCard;
}
