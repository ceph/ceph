import { Component, HostListener, OnDestroy, OnInit } from '@angular/core';
import { NavigationEnd, Router, RouterEvent } from '@angular/router';

import { Subscription } from 'rxjs';
import { filter } from 'rxjs/operators';

import { DocService } from '~/app/shared/services/doc.service';

@Component({
  selector: 'cd-error',
  templateUrl: './error.component.html',
  styleUrls: ['./error.component.scss']
})
export class ErrorComponent implements OnDestroy, OnInit {
  header: string;
  message: string;
  section: string;
  section_info: string;
  button_name: string;
  button_route: string;
  icon: string;
  docUrl: string;
  source: string;
  routerSubscription: Subscription;

  constructor(private router: Router, private docService: DocService) {}

  ngOnInit() {
    this.fetchData();
    this.routerSubscription = this.router.events
      .pipe(filter((event: RouterEvent) => event instanceof NavigationEnd))
      .subscribe(() => {
        this.fetchData();
      });
  }

  @HostListener('window:beforeunload', ['$event']) unloadHandler(event: Event) {
    event.returnValue = false;
  }

  fetchData() {
    try {
      this.router.onSameUrlNavigation = 'reload';
      this.message = history.state.message;
      this.header = history.state.header;
      this.section = history.state.section;
      this.section_info = history.state.section_info;
      this.button_name = history.state.button_name;
      this.button_route = history.state.button_route;
      this.icon = history.state.icon;
      this.source = history.state.source;
      this.docUrl = this.docService.urlGenerator(this.section);
    } catch (error) {
      this.router.navigate(['/error']);
    }
  }

  ngOnDestroy() {
    if (this.routerSubscription) {
      this.routerSubscription.unsubscribe();
    }
  }
}
