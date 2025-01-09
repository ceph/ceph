import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RgwTopicListComponent } from './rgw-topic-list.component';
import { RgwTopicService } from '~/app/shared/api/rgw-topic.service';
import { AuthStorageService } from '~/app/shared/services/auth-storage.service';
import { of } from 'rxjs';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { TableComponent } from '~/app/shared/datatable/table/table.component';
import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { Permission } from '~/app/shared/models/permissions';

describe('RgwTopicListComponent', () => {
  let component: RgwTopicListComponent;
  let fixture: ComponentFixture<RgwTopicListComponent>;
  let rgwTopicService: RgwTopicService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [RgwTopicListComponent, TableComponent],
      providers: [
        {
          provide: RgwTopicService,
          useValue: {
            listTopic: jest.fn()
          }
        },
        {
          provide: AuthStorageService,
          useValue: {
            getPermissions: jest.fn().mockReturnValue({ rgw: {} })
          }
        },
        ActionLabelsI18n
      ]
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(RgwTopicListComponent);
    component = fixture.componentInstance;
    rgwTopicService = TestBed.inject(RgwTopicService);
    fixture.detectChanges();
  });

  it('should create the component', () => {
    expect(component).toBeTruthy();
  });

  it('should have columns defined', () => {
    const expectedColumns: CdTableColumn[] = [
      { name: $localize`Name`, prop: 'name', flexGrow: 2 },
      { name: $localize`Owner`, prop: 'owner', flexGrow: 2 },
      { name: $localize`Amazon resource name`, prop: 'arn', flexGrow: 2 },
      { name: $localize`Push endpoint`, prop: 'dest.push_endpoint', flexGrow: 2 }
    ];
    expect(component.columns).toEqual(expectedColumns);
  });

  it('should fetch topics successfully', () => {
    const mockTopics = [
      {
        name: 'Topic1',
        owner: 'Owner1',
        arn: 'arn:aws:...',
        dest: { push_endpoint: 'http://example.com' }
      },
      {
        name: 'Topic2',
        owner: 'Owner2',
        arn: 'arn:aws:...',
        dest: { push_endpoint: 'http://example.com' }
      }
    ];

    // Mock the listTopic service call
    rgwTopicService.listTopic.mockReturnValue(of(mockTopics));

    // Trigger the fetch
    component.fetchData();
    fixture.detectChanges();

    component.topic$.subscribe((topics) => {
      expect(topics).toEqual(mockTopics);
    });
  });

  it('should update selection correctly', () => {
    const mockSelection = { selected: [1, 2] };
    component.updateSelection(mockSelection);

    expect(component.selection).toEqual(mockSelection);
  });
});
