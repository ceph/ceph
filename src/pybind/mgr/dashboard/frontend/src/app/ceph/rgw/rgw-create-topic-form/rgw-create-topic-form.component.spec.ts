import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwCreateTopicFormComponent } from './rgw-create-topic-form.component';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { NotificationService } from '~/app/shared/services/notification.service';
import { RouterTestingModule } from '@angular/router/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { HttpClientTestingModule } from '@angular/common/http/testing';
import { of } from 'rxjs';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { TextAreaJsonFormatterService } from '~/app/shared/services/text-area-json-formatter.service';
import { SharedModule } from '~/app/shared/shared.module';
import { ToastrModule } from 'ngx-toastr';
import { GridModule, InputModule, SelectModule } from 'carbon-components-angular';
import { NO_ERRORS_SCHEMA } from '@angular/compiler';

describe('RgwCreateTopicFormComponent', () => {
  let component: RgwCreateTopicFormComponent;
  let fixture: ComponentFixture<RgwCreateTopicFormComponent>;
  let rgwTopicService: RgwTopicService;
  let notificationService: NotificationService;
  let textAreaJsonFormatterService: TextAreaJsonFormatterService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwCreateTopicFormComponent],
      imports: [
        ReactiveFormsModule,
        RouterTestingModule,
        HttpClientTestingModule,
        SharedModule,
        ToastrModule.forRoot(),
        SelectModule,
        SharedModule,
        ReactiveFormsModule,
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
          useValue: { show: jest.fn() }
        }
      ],
      schemas: [NO_ERRORS_SCHEMA]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwCreateTopicFormComponent);
    component = fixture.componentInstance;
    rgwTopicService = TestBed.inject(RgwTopicService);
    notificationService = TestBed.inject(NotificationService);
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

  it('should submit the form with valid data', () => {
    component.topicForm.setValue({
      owner: 'dashboard',
      name: 'Test Topic',
      push_endpoint: 'http://localhost:80',
      OpaqueData: '',
      persistent: '',
      max_retries: '',
      time_to_live: '',
      retry_sleep_duration: '',
      policy: '{}',
      endpointType: 'HTTP',
      port: '80',
      verify_ssl: true,
      enable_ssl: true,
      cloud_events: '',
      user: '',
      password: '',
      vhost: '',
      ca_location: '',
      amqp_exchange: '',
      amqp_ack_level: '',
      use_ssl: false,
      kafka_ack_level: '',
      kafka_brokers: '',
      mechanism: '',
      fqdn: 'localhost'
    });

    // Spy on the 'create' method of RgwTopicService
    const createSpy = jest.spyOn(rgwTopicService, 'create').mockReturnValue(of({}));

    component.submitAction();

    expect(createSpy).toHaveBeenCalled();
    expect(notificationService.show).toHaveBeenCalledWith(
      expect.anything(),
      'Topic created successfully'
    );
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
      time_to_live: '100', // 100 seconds
      retry_sleep_duration: '10', // 10 seconds
      policy: '{}',
      cloud_events: 'true',
      user: 'admin',
      password: 'admin',
      vhost: '/',
      ca_location: 'localhost',
      amqp_exchange: 'test',
      amqp_ack_level: 'test',
      use_ssl: false,
      kafka_ack_level: 'test',
      kafka_brokers: 'test',
      mechanism: 'test',
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
  it('should load topic data when editing', () => {
    const topicData = {
      name: 'Test Topic',
      owner: 'dashboard',
      dest: {
        push_endpoint: 'http://localhost:80',
        persistent: true,
        max_retries: 3,
        time_to_live: 100,
        retry_sleep_duration: 10,
        push_endpoint_args:
          'ca_location=localhost&mechanism=test&verify_ssl=true&cloud_events=true&amqp_exchange=test&amqp_ack_level=test&use_ssl=false&kafka_ack_level=test&kafka_brokers=test'
      },
      opaqueData: 'test@123',
      policy: '{}'
    };

    jest.spyOn(rgwTopicService, 'get').mockReturnValue(of(topicData as any));
    component.loadTopicData('test-topic-id');

    expect(component.topicForm.get('name')?.value).toBe('Test Topic');
    expect(component.topicForm.get('owner')?.value).toBe('dashboard');
    expect(component.topicForm.get('push_endpoint')?.value).toBe('http://localhost:80');
  });

  it('should handle select change and update endpoint', () => {
    const event = { target: { value: 'AMQP' } } as any;
    component.onSelectChange(event);

    expect(component.selectedOption).toBe('AMQP');
    expect(component.topicForm.get('port')?.value).toBe('5671');
  });

  it('should handle secure SSL change and update port', () => {
    component.selectedOption = 'HTTP';
    component.onSecureSslChange(true);

    expect(component.secure_sslflag).toBe(true);
    expect(component.topicForm.get('port')?.value).toBe('443');
  });
});
