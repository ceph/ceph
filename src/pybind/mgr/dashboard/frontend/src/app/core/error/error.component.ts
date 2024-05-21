import { HttpClient } from '@angular/common/http';
import { Component, HostListener, OnDestroy, OnInit } from '@angular/core';
import { NavigationEnd, Router, RouterEvent } from '@angular/router';

import { Subscription } from 'rxjs';
import { filter } from 'rxjs/operators';

import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { DocService } from '~/app/shared/services/doc.service';
import { NotificationService } from '~/app/shared/services/notification.service';

@Component({
  selector: 'cd-error',
  templateUrl: './error.component.html',
  styleUrls: ['./error.component.scss']
})
export class ErrorComponent implements OnDestroy, OnInit {
  header: string;
  message: string;
  section: string;
  sectionInfo: string;
  icon: string;
  docUrl: string;
  source: string;
  routerSubscription: Subscription;
  uiConfig: string;
  uiApiPath: string;
  buttonRoute: string;
  buttonName: string;
  buttonTitle: string;
  secondaryButtonRoute: string;
  secondaryButtonName: string;
  secondaryButtonTitle: string;
  component: string;

  constructor(
    private router: Router,
    private docService: DocService,
    private http: HttpClient,
    private notificationService: NotificationService
  ) {}

  ngOnInit() {
    this.fetchData();
    this.routerSubscription = this.router.events
      .pipe(filter((event: RouterEvent) => event instanceof NavigationEnd))
      .subscribe(() => {
        this.fetchData();
      });
  }

  doConfigure() {
    this.http.post(`ui-api/${this.uiApiPath}/configure`, {}).subscribe({
      next: () => {
        this.notificationService.show(NotificationType.info, `Configuring ${this.component}`);
      },
      error: (error: any) => {
        this.notificationService.show(NotificationType.error, error);
      },
      complete: () => {
        setTimeout(() => {
          this.router.navigate([this.uiApiPath]);
          this.notificationService.show(NotificationType.success, `Configured ${this.component}`);
        }, 3000);
      }
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
      this.sectionInfo = history.state.section_info;
      this.icon = history.state.icon;
      this.source = history.state.source;
      this.uiConfig = history.state.uiConfig;
      this.uiApiPath = history.state.uiApiPath;
      this.buttonRoute = history.state.button_route;
      this.buttonName = history.state.button_name;
      this.buttonTitle = history.state.button_title;
      this.secondaryButtonRoute = history.state.secondary_button_route;
      this.secondaryButtonName = history.state.secondary_button_name;
      this.secondaryButtonTitle = history.state.secondary_button_title;
      this.component = history.state.component;
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
