import { Component, Input, OnInit } from '@angular/core';
import { FormControl } from '@angular/forms';
import { OneLevelResult, TableData } from '../services/village-name.service';

@Component({
  selector: 'app-table-display-v22',
  templateUrl: './table-display-v22.component.html',
  styleUrls: ['./table-display-v22.component.css'],
})
export class TableDisplayV22Component implements OnInit {
  ethnicGroups = new FormControl();
  @Input('tabledata') table: TableData;

  fullList;
  filteredList;

  constructor() {}

  ngOnInit(): void {
    console.log('this', this);
    this.fullList = this.table.data;
    this.filteredList = this.fullList;

    console.log('this.filteredList', this.filteredList.length);

    this.ethnicGroups.valueChanges.subscribe((value) => {
      console.log('value', value);

      this.filteredList = this.fullList.filter((row) => {
        for (let v of value) {
          // 这个checkbox
          console.log('v', v);
          if (row.category1.includes(v)) {
            return true;
          }
        }
        return false;
      });

      if (value.length == 0) {
        this.filteredList = this.fullList;
      }
    });
  }
}
