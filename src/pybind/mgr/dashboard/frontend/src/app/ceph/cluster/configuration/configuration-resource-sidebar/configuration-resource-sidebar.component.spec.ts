import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ActivatedRoute, convertToParamMap, ParamMap } from '@angular/router';
import { NO_ERRORS_SCHEMA } from '@angular/core';
import { BehaviorSubject } from 'rxjs';

import { ConfigurationResourceSidebarComponent } from './configuration-resource-sidebar.component';
import { ConfigurationResourceStateService } from '~/app/shared/services/configuration-resource-state.service';

describe('ConfigurationResourceSidebarComponent', () => {
  let component: ConfigurationResourceSidebarComponent;
  let fixture: ComponentFixture<ConfigurationResourceSidebarComponent>;

  let mockConfigurationResourceStateService: { load: jest.Mock };
  let paramMapSubject: BehaviorSubject<ParamMap>;

  beforeEach(async () => {
    // Mock the state service
    mockConfigurationResourceStateService = {
      load: jest.fn()
    };

    // Use a BehaviorSubject to control the route parameters dynamically
    paramMapSubject = new BehaviorSubject<ParamMap>(convertToParamMap({ name: 'test_config' }));

    await TestBed.configureTestingModule({
      declarations: [ConfigurationResourceSidebarComponent],
      providers: [
        {
          provide: ActivatedRoute,
          useValue: {
            paramMap: paramMapSubject.asObservable()
          }
        }
      ],
      // Tells the compiler to ignore <cd-sidebar-layout> in the HTML template
      schemas: [NO_ERRORS_SCHEMA]
    })
      // Override the component's internal provider to inject our mock
      .overrideComponent(ConfigurationResourceSidebarComponent, {
        set: {
          providers: [
            {
              provide: ConfigurationResourceStateService,
              useValue: mockConfigurationResourceStateService
            }
          ]
        }
      })
      .compileComponents();

    fixture = TestBed.createComponent(ConfigurationResourceSidebarComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should create', () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe('Initialization', () => {
    it('should read the name param, build sidebar items, and call load() on the state service', () => {
      // Act
      fixture.detectChanges(); // Triggers ngOnInit

      // Assert
      expect(component.configurationOption).toBe('test_config');

      // Verify load was called
      expect(mockConfigurationResourceStateService.load).toHaveBeenCalledWith('test_config');

      // Verify sidebar items were built
      expect(component.sidebarItems.length).toBe(1);
      expect(component.sidebarItems[0].label).toBe('Overview');
      expect(component.sidebarItems[0].route).toEqual([
        '/configuration',
        'test_config',
        'overview'
      ]);
      expect(component.sidebarItems[0].routerLinkActiveOptions).toEqual({ exact: true });
    });

    it('should gracefully handle a missing name param', () => {
      // Arrange
      paramMapSubject.next(convertToParamMap({}));

      // Act
      fixture.detectChanges();

      // Assert
      expect(component.configurationOption).toBe('');
      expect(mockConfigurationResourceStateService.load).toHaveBeenCalledWith('');
      expect(component.sidebarItems[0].route).toEqual(['/configuration', '', 'overview']);
    });
  });

  describe('configurationTitle getter', () => {
    it('should return an empty string if configurationOption is not set', () => {
      component.configurationOption = '';
      expect(component.configurationTitle).toBe('');
    });

    it('should decode URI encoded configuration options', () => {
      component.configurationOption = 'test%20config';
      expect(component.configurationTitle).toBe('test config');
    });

    it('should fallback to the raw string if URI decoding fails (malformed URI)', () => {
      // '%' alone throws a URIError when passed to decodeURIComponent
      component.configurationOption = 'test%';
      expect(component.configurationTitle).toBe('test%');
    });
  });

  describe('ngOnDestroy', () => {
    it('should unsubscribe from route param subscriptions', () => {
      fixture.detectChanges();

      const unsubscribeSpy = jest.spyOn((component as any).sub, 'unsubscribe');

      component.ngOnDestroy();

      expect(unsubscribeSpy).toHaveBeenCalled();
    });
  });
});
