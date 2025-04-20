import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DataViewerPageComponent } from './data-viewer-page.component';

describe('DataViewerPageComponent', () => {
  let component: DataViewerPageComponent;
  let fixture: ComponentFixture<DataViewerPageComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [DataViewerPageComponent]
    })
    .compileComponents();
    
    fixture = TestBed.createComponent(DataViewerPageComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
