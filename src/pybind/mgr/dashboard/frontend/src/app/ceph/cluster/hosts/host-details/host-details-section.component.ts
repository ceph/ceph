import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, ParamMap } from '@angular/router';

@Component({
  selector: 'cd-host-details-section',
  templateUrl: './host-details-section.component.html',
  standalone: false
})
export class HostDetailsSectionComponent implements OnInit {
  hostname = '';
  section = '';

  constructor(private route: ActivatedRoute) {}

  ngOnInit(): void {
    this.route.parent?.paramMap.subscribe((pm: ParamMap) => {
      this.hostname = pm.get('hostname') ?? '';
    });
    this.section = this.route.snapshot.data['section'] ?? '';
  }
}
