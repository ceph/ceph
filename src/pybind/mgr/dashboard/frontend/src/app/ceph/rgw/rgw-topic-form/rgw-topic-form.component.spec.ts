import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwTopicFormComponent } from './rgw-topic-form.component';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RouterTestingModule } from '@angular/router/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TextAreaJsonFormatterService } from '~/app/shared/services/text-area-json-formatter.service';
import { SharedModule } from '~/app/shared/shared.module';
import { ToastrModule } from 'ngx-toastr';
import { GridModule, InputModule, SelectModule } from 'carbon-components-angular';
import { NO_ERRORS_SCHEMA } from '@angular/compiler';

describe('RgwTopicFormComponent', () => {
  let component: RgwTopicFormComponent;
  let fixture: ComponentFixture<RgwTopicFormComponent>;
  let textAreaJsonFormatterService: TextAreaJsonFormatterService;

  const mockNotificationService = {
    show: jest.fn()
  };
  const mockRouter = {
    navigate: jest.fn(),
    url: '/rgw/topic/create'
  };

  const mockActivatedRoute = {
    snapshot: {
      paramMap: {
        get: jest.fn()
      }
    }
  };
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwTopicFormComponent],
      imports: [
        ReactiveFormsModule,
        RouterTestingModule,
        HttpClientTestingModule,
        SharedModule,
        ToastrModule.forRoot(),
        SelectModule,
        GridModule,
        InputModule
      ],
      providers: [
        RgwTopicService,
        TextAreaJsonFormatterService,
        {
          provide: ActionLabelsI18n,
          useValue: { CREATE: 'Create', EDIT: 'Edit', Delete: 'Delete' }
        },
        {
          provide: NotificationService,
          useValue: mockNotificationService
        },
        { provide: 'Router', useValue: mockRouter },
        { provide: 'ActivatedRoute', useValue: mockActivatedRoute }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwTopicFormComponent);
    component = fixture.componentInstance;
    textAreaJsonFormatterService = TestBed.inject(TextAreaJsonFormatterService);
    fixture.detectChanges();
  });

  it('should create the component', () => {
    expect(component).toBeTruthy();
  });

  it('should initialize the form correctly', () => {
    const form = component.topicForm;
    expect(form).toBeDefined();
    expect(form.get('name')).toBeDefined();
    expect(form.get('owner')).toBeDefined();
    expect(form.get('push_endpoint')).toBeDefined();
  });

  it('should generate push endpoint for HTTP', () => {
    component.topicForm.setValue({
      owner: 'dashboard',
      name: 'Testtopic',
      OpaqueData: 'test@123',
      endpointType: 'HTTP',
      fqdn: 'localhost',
      port: '80',
      push_endpoint: 'http://localhost:80',
      verify_ssl: false,
      persistent: 'true',
      max_retries: '3',
      time_to_live: '100',
      retry_sleep_duration: '10',
      policy: '{}',
      cloud_events: 'true',
      user: '',
      password: '',
      vhost: '',
      ca_location: '',
      amqp_exchange: '',
      ack_level: '',
      use_ssl: false,
      kafka_brokers: '',
      mechanism: '',
      enable_ssl: false
    });
    component.generatePushEndpoint(false);
    expect(component.topicForm.get('push_endpoint')?.value).toBe('http://localhost:80');
  });

  it('should format JSON in the policy field on text area change', () => {
    const formatSpy = jest.spyOn(textAreaJsonFormatterService, 'format');
    const textArea: any = { nativeElement: { value: '{"key": "value"}' } };
    component.textAreaOnChange(textArea);
    expect(formatSpy).toHaveBeenCalledWith(textArea);
  });

  it('should generate HTTP push endpoint', () => {
    component.selectedOption = 'HTTP';
    component.topicForm.patchValue({
      fqdn: 'example.com',
      port: '8080',
      enable_ssl: false
    });

    component.generatePushEndpoint();
    expect(component.topicForm.get('push_endpoint')?.value).toBe('http://example.com:8080');
  });

  it('should generate AMQP push endpoint with auth', () => {
    component.selectedOption = 'AMQP';
    component.topicForm.patchValue({
      user: 'guest',
      password: 'guest',
      fqdn: 'mq.example.com',
      port: '5672',
      vhost: '/'
    });

    component.generatePushEndpoint();
    expect(component.topicForm.get('push_endpoint')?.value).toBe(
      'amqps://guest:guest@mq.example.com:5672/'
    );
  });

  it('should disable verify_ssl and use_ssl when enable_ssl is false', () => {
    component.topicForm.patchValue({ enable_ssl: false });
    component.topicForm.get('enable_ssl')?.setValue(false);
    fixture.detectChanges();

    expect(component.topicForm.get('verify_ssl')?.value).toBe(false);
    expect(component.topicForm.get('use_ssl')?.value).toBe(false);
  });
});
