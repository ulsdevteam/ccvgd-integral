import { Component, Input, OnInit } from '@angular/core';
import { LastName, TableData } from '../services/village-name.service';
import { FormControl } from '@angular/forms';

@Component({
  selector: 'app-table-display-v23',
  templateUrl: './table-display-v23.component.html',
  styleUrls: ['./table-display-v23.component.css'],
})
export class TableDisplayV23Component implements OnInit {
  fullList;
  filteredList;
  lastname = new FormControl();
  @Input('tabledata') table: TableData;

  constructor() {}

  ngOnInit(): void {
    this.fullList = this.table.data;
    this.filteredList = this.fullList;

    this.lastname.valueChanges.subscribe((value) => {
      //console.log("value",value);

      this.filteredList = this.fullList.filter((row) => {
        for (let v of value) {
          // 这个checkbox
          console.log('v', v);

          if (row.firstLastNameId.includes(v)) {
            return true;
          }
          if (row.secondLastNameId.includes(v)) {
            return true;
          }
          if (row.thirdLastNameId.includes(v)) {
            return true;
          }
          if (row.fourthLastNameId.includes(v)) {
            return true;
          }
          if (row.fifthLastNameId.includes(v)) {
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
