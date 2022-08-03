import {Component, Input, OnInit} from '@angular/core';
import {NaturalDisaster, TableData} from '../services/village-name.service';
import {FormBuilder, FormGroup, } from '@angular/forms';

@Component({
  selector: 'app-table-display',
  templateUrl: './table-display.component.html',
  styleUrls: ['./table-display.component.css']
})
export class TableDisplayComponent implements OnInit {

    // table: original data from server
    // 所有的非数据以外的数据这里来
    @Input('table') table: TableData;

    filterFormGroup: FormGroup;

    // fullList: 复制server拿过来的整个table的信息里面只复制data的部分
    fullList = [];
    // filteredList：最后展示的数据
    filteredList = [];

    constructor(private fb: FormBuilder) {

    }

    ngOnInit(): void {
      console.log('here is the table from table-display',this.table);
      this.fullList = this.table.data;
      this.filteredList = this.table.data;
      console.log('table:',this.table);

      // 优化：这里可以用map
      // 动态产生filter fields
      this.filterFormGroup = this.fb.group(this.formFields());

      this.filterFormGroup.valueChanges.subscribe((value)=> {
        console.log(value);

        this.filteredList = this.fullList.filter( row => {
          for(let checkbox of Object.keys(value)) {
            // 这个checkbox
            if( value[checkbox] && checkbox === row.category.type) {
              return true;
            }
          }
          return false;
        });

        // 这个地方二级filter要改
        if(this.filteredList.length == 0){
          this.filteredList = this.fullList;
        }


      });



  }
  /*Dynamically generate filterFormGroup fields*/
  private formFields() {
    let formControls = {};
    if(this.table.filters != null){
      for (let val of this.table.filters) {
        formControls[val.type] = [false];
      }
    }
    return formControls;
  }
}
