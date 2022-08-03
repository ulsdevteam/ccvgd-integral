import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TableDisplayV2Component } from './table-display-V2.component';

describe('FilterComponent', () => {
  let component: TableDisplayV2Component;
  let fixture: ComponentFixture<TableDisplayV2Component>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TableDisplayV2Component ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TableDisplayV2Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
