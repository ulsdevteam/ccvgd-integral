import { MatTableDataSource } from "@angular/material/table";
import { Component, ElementRef, Input, OnInit, QueryList, ViewChild, ViewChildren, ViewEncapsulation } from "@angular/core";
import { MultiVillageFilterService } from "../services/multi-village-filter.service";
import { MatPaginator } from "@angular/material/paginator";
import { Router } from "@angular/router";
import {COMMA, ENTER} from '@angular/cdk/keycodes';
import {MatChipInputEvent, MatChipsModule} from '@angular/material/chips';
import { environment } from '../../environments/environment';

//TODO
export interface PeriodicElement {
  name: string;
  position: number;
  weight: number;
  symbol: string;
}

export interface BasicVillageInformation {
  gazetteerId: number;
  gazetteerName: string;
  villageId: string;
  villageName: string;
  province: string;
  city: string;
  county: string;
  category1: string;
  data: string;
  unit: string;
}

export interface Fruit {
  name: string;
}

@Component({
  selector: "app-muiti-village-results",
  templateUrl: "./muiti-village-results.component.html",
  styleUrls: ["./muiti-village-results.component.css"],
  encapsulation: ViewEncapsulation.None,
})
export class MuitiVillageResultsComponent implements OnInit {
  @ViewChildren(MatPaginator) mainPaginator = new QueryList<MatPaginator>();
  // @Input() dataSource;
  // @ViewChild("mainPaginator") mainPaginator: MatPaginator;

  loading: boolean = false;
  userInput: any = {};
  getAllResponses: any = {};

  inputSearchField: any;


  dataSource;

  searchResultData: any = {}
  displayResultsData: any[] = []

  userSelectionList: any[] = []

  //for chips list
  selectable = true;
  removable = true;
  addOnBlur = true;
  readonly separatorKeysCodes = [ENTER, COMMA] as const;
  fruits: Fruit[] = [
    {name: 'Lemon'},
    {name: 'Lime'},
    {name: 'Apple'},
  ];

  mapFromCHToEN = new Map([
    ["村庄基本信息", "village"],
    ["村志基本信息", "gazetteerinformation"],
    ["自然环境", "naturalenvironment"],
    ["自然灾害", "naturaldisasters"],
    ["姓氏", "fourthlastNames"],
    ["第一次拥有或购买年份", "firstavailabilityorpurchase"],
    ["民族", "ethnicgroups"],
    ["人口", "population"],
    ["军事政治", "military"],
    ["经济原始数据", "economy"],
    ["统一单位经济", "economyunity"],
    ["计划生育", "familyplanning"],
    ["教育", "education"]
  ]);

  getVillageidForDownload: string;

  constructor(private multiVillageFilterService: MultiVillageFilterService,private router: Router) {}



  getUrlParams (key): string {
    const queryString = window.location.search;
    const urlParams = new URLSearchParams(queryString);
    return urlParams.get(key)
  }

  ngOnInit(): void {
    this.loading = true;
    const userSelection = window.localStorage.getItem('userSelection');
    // const userValue = this.getUrlParams('userValue');
    // [TODO] decode userValue and return it
    // const userValuePlainText = decode(userValue);
    this.userSelectionList = JSON.parse(userSelection);

    const userList = window.localStorage.getItem('userList');
    // const userList = this.getUrlParams('userList');
    // [TODO] decode userList and return it
    // const userListPlainText = decode(userList);
    this.userInput = JSON.parse(userList);

    // const userValueEncoded = encode(userValue);
    console.log('[debug]', this.userSelectionList)
    console.log("[debug] this.userInput",this.userInput)
    this.getData();
    // console.log("userSelectionList", this.userSelectionList)
  }
  removeGazetteerId(rawArray) {
    let resultsDisplay = [];
    for(let item in rawArray) {
      // gazetteerId
      if(rawArray[item] !== "village_id") {
        resultsDisplay.push(rawArray[item]);
      }
    }
    return resultsDisplay;
  }

  

  async getData() {
    // console.log('get it !', await this.multiVillageFilterService.getUserList);
    // this.userInput = await this.multiVillageFilterService.getUserList;

    console.log("[debug] this.userInput",this.userInput)

    this.searchResultData  = await this.multiVillageFilterService.onPostMultiVillages(this.userInput);
    console.log('[debug] result data', this.searchResultData);
    this.loading = false;
    if(this.searchResultData.code === 4001) {
      alert("Result is invalid, will navigate to search page");
      setTimeout(async ()=>{
        // await this.router.navigate(["/multi-village-search"]);
      }, 3000);
    }
    if(this.searchResultData.data && this.searchResultData.data.length === 0) {
      alert(`后端返回报错 !  \n  error message: ${this.searchResultData.error}`);
      setTimeout(async ()=>{
        // await this.router.navigate(["/multi-village-search"]);
      }, 3000);
    }
  
        for(let index in this.searchResultData) {
          this.dataSource = this.searchResultData[index].data;
          for(let item in this.userSelectionList) {

            if(this.userSelectionList[item].selectedTopic === this.searchResultData[index].tableNameChinese) {

              //advance filter - display only user selected categories
              const each_res = this.userSelectionList[item].hasCategory === true ? this.searchResultData[index].data.filter(i => 
                this.userSelectionList[item].category1List.indexOf(i.category1) !== -1) : this.searchResultData[index].data;

                console.log(each_res)
              this.displayResultsData.push({
                topicName: this.searchResultData[index].tableNameChinese,
                dataSource: new MatTableDataSource(each_res),
                displayedColumns: this.removeGazetteerId(this.searchResultData[index].field),
                selected_Categories: this.userSelectionList[item]. hasCategory ? 
                this.userSelectionList[item].category1List : null,
                downloadUrl: `${environment.API_ROOT}advancesearch/download/?village=${this.userInput.villageid.toString()}&topic=${this.mapFromCHToEN.get(this.searchResultData[index].tableNameChinese)}`
              })
            }
          }
        }

        this.mainPaginator.changes.subscribe(a => a.forEach((b, index) => 
        this.displayResultsData[index].dataSource.paginator = b));

        this.onlyGetSelectedCategoriesRow();
  }

  
    filterDataSource(event: Event, currentDataSource) {
      // console.log("currentDataSource",currentDataSource)
    const filterValue = (event.target as HTMLInputElement).value;
    currentDataSource.filter = filterValue.trim().toLowerCase();
    if (currentDataSource.paginator) currentDataSource.paginator.firstPage();
  }

  downloadCurrentTopic(event, topicName) {


  }

  onlyGetSelectedCategoriesRow() {}

  add(event: MatChipInputEvent, currentDataSource): void {
    const value = (event.value || '').trim();

    console.log("event.input.value",event)

    currentDataSource.filter = event.input.value.trim().toLowerCase();
    // if (currentDataSource.paginator) currentDataSource.paginator.firstPage();

    // Add our vlaue
    if (value) {
      this.fruits.push({name: event.input.value});
    }
    
    // Clear the input value
    // event.chipInput!.clear();
    if (event.input) {
      event.input.value = '';
       }
  }

  remove(eachCategory): void {
    const index = this.fruits.indexOf(eachCategory);

    if (index >= 0) {
      this.fruits.splice(index, 1);
    }
  }

}
