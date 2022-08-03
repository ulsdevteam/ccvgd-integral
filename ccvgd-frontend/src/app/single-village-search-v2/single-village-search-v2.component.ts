
import { Component, OnInit } from '@angular/core';
import { FormControl } from '@angular/forms';
import { Router, RouterModule } from '@angular/router';
import {
  VillageNameService,
  Village,
  TableData,
  VillageNameDisplay,
} from '../services/village-name.service';
import { SingleVillageSearchResultService } from '../services/single-village-search-result.service';
import { StateServiceService } from '../services/state-service.service';

@Component({
  selector: 'app-single-village-search-v2',
  templateUrl: './single-village-search-v2.component.html',
  styleUrls: ['./single-village-search-v2.component.css'],
})
export class SingleVillageSearchV2Component implements OnInit {
  myControl = new FormControl();

  value = '输入村名，如‘太平店村’ ；Input Village Name: ‘Tai ping dian cun’';

  options: Village[] = [];
  filteredOptions: Village[] = [];
  temp: VillageNameDisplay[] = [{
    data: [
      {
        isSelected: false /*ui: backend dont need*/,
        name: 'string',
        province: 'string',
        city: 'string',
        county: 'string',
        id: 'string',
      },
      {
        isSelected: false /*ui: backend dont need*/,
        name: 'string2',
        province: 'string2',
        city: 'string2',
        county: 'string2',
        id: 'string2',
      },
    ],
  }];

  choose: Village;

  searchResult: TableData[];
  display: boolean = false;

  selectedTable: any = [];

  constructor(
    private villageNameService: VillageNameService,
    private villageSearchResultService: SingleVillageSearchResultService,
    private stateService: StateServiceService,
    private router: Router
  ) {}

  ngOnInit(): void {
    this.init();
  }

  async init() {
    this.temp = await this.villageNameService.getVillages();
    this.filteredOptions = this.temp[0].data;
    this.options = this.filteredOptions;
    console.log('this.filteredOptions', this.filteredOptions);
  }

  async filter(value: string) {
    if (value === '') {
      await this.init();
    } else {
      const filterValue = value;
      console.log('filterValue', filterValue);
      this.temp = [await this.villageNameService.filterVillages(value)];
      console.log('this.temp', this.temp);
      this.filteredOptions = this.temp[0].data;
      this.filteredOptions = this.options.filter((option) =>
        option.name.includes(filterValue)
      );
    }
  }

  displayFn(village): string {
    return village ? village.name : '';
  }

  // async search(choose: Village): Promise<void>
  search(choose: Village) {
    console.log('choose ', choose);
    // this.searchResult = (
    //   await this.villageSearchResultService.searchEncap(choose)
    // ).tables;
    // console.log('this is the searchResult', this.searchResult);

    this.display = true;

    // router go to single-village-search-result page
    // BUG do not use stateService
    // this.stateService.data = this.searchResult;

    // if (this.stateService.data) {
    //   window.localStorage.setItem(
    //     'result',
    //     JSON.stringify(this.stateService.data)
    //   );
    // } else {
    //   console.log('😨 no stateService data or search results');
    // }

    window.localStorage.setItem('choose', JSON.stringify(choose));

    this.router.navigate(['/multi-village-search-result']);
  }

  onSelect(table: TableData) {
    this.selectedTable = table;
    console.log('selected table: ', this.selectedTable);
  }

  async goToComponentB(): Promise<void> {}
}
