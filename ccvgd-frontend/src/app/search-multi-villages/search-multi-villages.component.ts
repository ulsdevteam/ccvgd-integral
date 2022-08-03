import {
  VillageNameService,
  TableData,
} from "./../services/village-name.service";
import {
  Component,
  ElementRef,
  OnInit,
  Renderer2,
  ViewChild,
  Inject,
  ViewEncapsulation
} from "@angular/core";
import { MatPaginator } from '@angular/material/paginator';
import { ProvinceCityCountyService } from "../services/province-city-county.service";
import { MatTableDataSource } from "@angular/material/table";
import { MatCheckboxChange } from "@angular/material/checkbox";
import { Router } from "@angular/router";
import { MultiVillageFilterService } from "../services/multi-village-filter.service";
import { HttpClient } from "@angular/common/http";
import { Input, Output, EventEmitter } from "@angular/core";
import { MatTabGroup } from "@angular/material/tabs";
// import {ThemePalette} from '@angular/material/core';
import { Category, CheckList, PostDataToSearch, Year, DisplayTopicCategory } from "./modals/formatData";
import { HttpServiceService } from '../services/http-service.service';
import categoryConfig from './mapMainCategoryToSubCategoryConfig';
import mapSubCategoryToNewSubCategory from './mapSubCategoryToNewSubCategory';
import displayedMiddleTabsFilteredToolTip from './displayedMiddleTabsFilteredToolTip';
import mapCategory1ToTooltips from './mapCategory1ToTooltips';

// import { MatSelectionList } from "@angular/material/list";
import { MatListOption, MatSelectionListChange } from '@angular/material/list'
import {MatDialog, MatDialogRef} from '@angular/material/dialog';
import {MatButtonModule} from '@angular/material/button';
import { DialogComponent } from './dialog/dialog.component';
import { MatCardModule } from "@angular/material/card";
const {mapMainCategoryToSubCategoryConfig, middleTabsMap} = categoryConfig;

interface StringMap {
  [index:string]: String;
}

@Component({
  selector: "app-search-multi-villages",
  templateUrl: "./search-multi-villages.component.html",
  styleUrls: ["./search-multi-villages.component.css"],
  encapsulation: ViewEncapsulation.None
})
export class SearchMultiVillagesComponent implements OnInit {
  @ViewChild("myYearDiv", { static: false }) myYearDiv: ElementRef;
  @ViewChild("tabGroup") tabGroup: MatTabGroup;
  @ViewChild("villagePaginator") paginator: MatPaginator;
  @Input() okIsClick;

  comfirmDelete: boolean;
  options;
  //TODO this is fake data for province, need change later
  provinceList: string[] = [];
  //city and county got from database with 100 values
  cityList: string[] = [];
  countyList: string[] = [];
  displayedColumns: string[] = [
    "isSelected",
    "village_name",
    "province",
    "city",
    "county",
  ];

  displayedMiddleTabs: string[] = [
    "村庄基本信息-Village Information",
    "村志基本信息-Gazetteer Information",
    "自然环境-Natural Environment",
    "自然灾害-Natural Disasters",
    "姓氏-Last Names",
    "第一次拥有或购买年份-Year of First Availability/Purchase",
    "民族-Ethnic Groups",
    "人口-Population",
    "军事政治-Military & Politics",
    "经济原始数据-Economy Original",
    "统一单位经济-Economy Unity",
    "计划生育-Family Planning",
    "教育-Education",
  ];

  displayedMiddleTabsFiltered: string[] = [
    "村庄基本信息-Village Information",
    "村志基本信息-Gazetteer Information",
    "自然环境-Natural Environment",
    "自然灾害-Natural Disasters",
    "姓氏-Last Names",
    "第一次拥有或购买年份-Year of First Availability/Purchase",
    "民族-Ethnic Groups",
    "人口-Population",
    "军事政治-Military & Politics",
    "经济原始数据-Economy Original",
    "统一单位经济-Economy Unity",
    "计划生育-Family Planning",
    "教育-Education",
  ];




  dataSource;
  selectedValue: string;
  provinceSearch: string;
  citySearch: string;
  countySearch: string;
  villageSearch: string;
  totalList: any = {};
  checkItems = new Map();
  tempcheckItems: string[] = [];
  rightToptempcheckItems: string[] = [];
  startYearInput: string = "";
  endYearInput: string = "";
  searchCollectorInput: string;
  multiSearchResult: any;
  selectedTabLabel: string;
  loading: boolean = false;
  searchResult: TableData[];

  villageidList: any = [];

  middleBoxCategory1: string[] = [];
  middleBoxCategory2: string[] = [];

  category1Map = new Map();
  cat1Cat2Map = new Map();

  middleTabsMap = new Map();

  middleTabsEng2ChineseMap = new Map([
    ["村志基本信息-Gazetteer Information", "村志基本信息"],
    ["村志基本信息", "村志基本信息-Gazetteer Information"],
    ["自然环境-Natural Environment", "自然环境"],
    ["自然环境", "自然环境-Natural Environment"],
    ["自然灾害-Natural Disasters", "自然灾害"],
    ["自然灾害","自然灾害-Natural Disasters"],
    ["姓氏-Last Names", "姓氏"],
    ["姓氏", "姓氏-Last Names"],
    ["第一次拥有或购买年份-Year of First Availability/Purchase","第一次拥有或购买年份"],
    ["第一次拥有或购买年份", "第一次拥有或购买年份-Year of First Availability/Purchase"],
    ["民族-Ethnic Groups","民族"],
    ["民族","民族-Ethnic Groups"],
    ["人口-Population","人口"],
    ["人口", "人口-Population"],
    ["军事政治-Military & Politics", "军事政治"],
    ["军事政治", "军事政治-Military & Politics"],
    ["经济原始数据-Economy Original", "经济原始数据"],
    ["经济原始数据","经济原始数据-Economy Original"],
    ["统一单位经济-Economy Unity","统一单位经济"],
    ["统一单位经济","统一单位经济-Economy Unity"],
    ["计划生育-Family Planning","计划生育"],
    ["计划生育", "计划生育-Family Planning"],
    ["教育-Education","教育"],
    ["教育","教育-Education"],
  ]);

  mapMainCategoryToSubCategoryConfig = []

  mapSubCategoryToNewSubCategory:StringMap = {}

  displayedMiddleTabsFilteredToolTip = new Map();


  postVillagesTopics = {
    villageid: [""],
    topic: [""],
    //TODO
    // topic: ['economy'],
  };
  categoryResult: any = {};
  singleYearList: string[] = [];
  singleYearMap = new Map();
  singleYearSelected: string;

  postYearData = {
    villageid: [],
    topic: [],
    year: [],
    year_range: [],
  };

  //TODO *****************************
  masterSelected: boolean;
  allTopicChecked: boolean;
  category1Selected: any = [];
  multiVillages_checkList: CheckList[] = [];
  multiVillages_checkedList: CheckList[] = [];
  checkedVillagesID: any[];
  //search
  postDataToSearch: PostDataToSearch[] = [];
  //search - select all
  filteredData: any[];
  //middle - category
  topicCategory: any[];

  category2_checkedList: any[];
  displayTopicCategory: DisplayTopicCategory[] = [];
  resultSelectedTopics: any[];

  //
  currentSelectedTopic: string;
  displayCategory2: string;
  //data
  responseData: any;
  //year -left top
  topicYear: Year[] = [];
  totalYearOnly: any[] = [];
  checked_year_only: any[] = [];
  inputed_year_range: any[] = [];
  //post
  finalPostTopicList: any[] = [];
  value1 = "";
  mapCategory1ToTooltips = {};

  //
  currentTopicData: any;
  category1Set = new Set();
  category2Set = new Set();
  category3Set = new Set();
  //

  // name *******
  allNamesData: any[] = [];
  filteredNamesData: any[] = [];
  showAllNamesDataList: any[] = [];
  showAllNamesDataListUnique: any[] = [];
  numsOfLastNames: any[] = [];
  nameOfVillage: any[] = [];
  isNamesTab: boolean = false;
  isHiddenPanel: boolean = true;

  oldVilliagesChecklist: CheckList[] = [];

  userList: any = {}; // request parameters


  defaultTopicList = ["village","gazetteerinformation","naturalenvironment","naturaldisasters", "fourthlastNames",
  "firstavailabilityorpurchase", "ethnicgroups", "population", "military", "economy","economyunity", "familyplanning", "education"];
  defaultTopics_InCh = ["村庄基本信息","村志基本信息","自然环境","自然灾害", "姓氏",
    "第一次拥有或购买年份","民族","人口","军事政治","经济原始数据","统一单位经济","计划生育","教育"];


  constructor(
    private villageNameService: VillageNameService,
    private provinceCityCountyService: ProvinceCityCountyService,
    private renderer: Renderer2,
    private el: ElementRef,
    private router: Router, // private multiVillageFilterService: MultiVillageFilterService,
    private http: HttpClient,
    private multiVillageFilterService: MultiVillageFilterService,
    private httpService : HttpServiceService,
    public dialog: MatDialog
  ) {
    this.masterSelected = false;
    // console.log(this.filteredData);
    // this.provinceList = this.provinceCityCountyService.getProvince();
    // filter some main categories in mapMainCategoryToSubCategoryConfig
    this.mapMainCategoryToSubCategoryConfig = mapMainCategoryToSubCategoryConfig;
    this.mapSubCategoryToNewSubCategory = mapSubCategoryToNewSubCategory;
    this.displayedMiddleTabsFilteredToolTip = displayedMiddleTabsFilteredToolTip;
    this.mapCategory1ToTooltips = mapCategory1ToTooltips;
    this.middleTabsMap = middleTabsMap;
    this.displayedMiddleTabsFiltered = this.displayedMiddleTabsFiltered.filter(item => !this.mapMainCategoryToSubCategoryConfig[this.middleTabsEng2ChineseMap.get(item)]);
  }
  updateVillages(lang:number=0) {
    this.multiVillages_checkList = [];
    this.villageNameService.getVillages(lang).then((results) => {
      const resultsOption = [];
      results.forEach(result => {
        this.totalList = result.data;
        result.data.map((item) => {
          if (this.cityList.includes(item.city) === false) {
            //push to array and prevent duplicate items
            //BUG
            if (this.provinceList.indexOf(item.province) == -1) {
              this.provinceList.push(item.province);
            }
            if (this.cityList.indexOf(item.city) == -1) {
              this.cityList.push(item.city);
            }
            if (this.countyList.indexOf(item.county) == -1) {
              this.countyList.push(item.county);
            }
          }
          this.multiVillages_checkList.push({
            village_id: item.id,
            village_name: item.name,
            province: item.province,
            city: item.city,
            county: item.county,
            isSelected: false
          });
        });
        resultsOption.push(...result.data);
      });
      this.options = new MatTableDataSource(resultsOption);
      this.options.paginator = this.paginator;
      this.filteredData = this.options.filteredData;
    });
  }
  ngOnInit(): void {
    this.villageNameService.getMessage().subscribe(value => {
      this.updateVillages(value)
    })
    this.updateVillages();
  }

  applyFilter(event: Event) {
    const filterValue = (event.target as HTMLInputElement).value;
    this.options.filter = filterValue.trim().toLowerCase();
  }
   //********************* for checkbox field ************************************* */

  // keep masterSelected result as old villages checklist
    keepOldChecklist() {
      // if all selected
      if(this.masterSelected) {
        this.oldVilliagesChecklist = this.multiVillages_checkedList;
        this.masterSelected = false;
      }
    }

    recoverCheckedList() {
      if(this.oldVilliagesChecklist.length<=0) {
        return
      }
      this.multiVillages_checkList = Array.from(new Set(this.multiVillages_checkList.concat(this.oldVilliagesChecklist)));
      this.getCheckedItemList();
    }

   changeProvince(data: Event) {
    this.options.filter = data;

    this.cityList = [];
    this.options.filteredData.map((item) => {
      if (!this.cityList.includes(item.city)) {
        this.cityList.push(item.city);
      }
    });

    this.countyList = [];
    this.options.filteredData.map((item) => {
      if (!this.countyList.includes(item.county)) {
        this.countyList.push(item.county);
      }
    });
    this.filteredData = this.options.filteredData;
    // keep master selected result
     this.keepOldChecklist();
     this.recoverCheckedList();
  }

  changeCity(data: Event) {
    this.options.filter = data;

    this.countyList = [];
    this.options.filteredData.map((item) => {
      if (!this.countyList.includes(item.county)) {
        this.countyList.push(item.county);
      }
    });
    this.filteredData = this.options.filteredData;
    // keep master selected result
    this.keepOldChecklist()
    this.recoverCheckedList();
  }

  changeCounty(data) {
    this.options.filter = data;
    this.filteredData = this.options.filteredData;
    // keep master selected result
    this.keepOldChecklist()
    this.recoverCheckedList();
  }
  
  // The master checkbox will check/ uncheck all items
  checkUncheckAll() {
    if(!this.masterSelected) {
      // clear all selected
      this.topicCategory = [];
      this.category2_checkedList = [];
    }
    // update checkLsit

    this.multiVillages_checkList = [].concat(this.oldVilliagesChecklist);


    console.log('[debug] select all', this.filteredData);
    this.filteredData.map(item => {
      this.multiVillages_checkList.push({
        village_id: item.id,
        village_name: item.name,
        province: item.province,
        city: item.city,
        county: item.county,
        isSelected: false
      });
    })
    for(let i = 0; i < this.multiVillages_checkList.length; i++) {
      this.multiVillages_checkList[i].isSelected = this.masterSelected;
    }
    this.getCheckedItemList();
  }

  //check if all the checkbox selected
  async isAllCheckBoxSelected(event: MatCheckboxChange, element) {
    if(!this.masterSelected) {
      this.topicCategory = [];
      this.category2_checkedList = [];
    }

    let checkedItemID = this.multiVillages_checkList.findIndex((obj => obj.village_id === element.id));
    this.multiVillages_checkList[checkedItemID].isSelected = event.checked ? true : false;
    this.getCheckedItemList();
    this.getYearWithTopic();
  }

  //here all the items are checked 
  getCheckedItemList() {
    this.multiVillages_checkedList = [];

    for(let i = 0; i < this.multiVillages_checkList.length; i++) {
      if(this.multiVillages_checkList[i].isSelected)
      this.multiVillages_checkedList.push(this.multiVillages_checkList[i]);
    }
     this.getListOfchecked_VillagesID();
  }

  //*************************** post and get data ******************************* */
  //1. get list of village id and post request with all the topic -- as default

  getListOfchecked_VillagesID() {
    this.checkedVillagesID = [];
    for(let i in this.multiVillages_checkedList) {
      this.checkedVillagesID.push(this.multiVillages_checkedList[i].village_id);
    }
    if(this.checkedVillagesID.length > 0) this.processRequest();
  }

  deleteVillage(event, checkedItem) {
    const dialogRef = this.dialog.open(DialogComponent, {
      // width: '250px'
    });

    dialogRef.afterClosed().subscribe(result => {

      if(result) {
        let deleteElementIndex = this.multiVillages_checkList.findIndex(item => item === checkedItem);
        this.multiVillages_checkList[deleteElementIndex].isSelected = false;
        this.getCheckedItemList();
      }
    });

  }
  hiddenPanle() {
    this.isHiddenPanel = !this.isHiddenPanel;
  }
  // getDefaultTopics() 

  async processRequest() {
    // update userlist
    this.userList = {
      villageid: this.checkedVillagesID,
      topic: this.defaultTopicList
    };
    const response =
    await this.multiVillageFilterService.onPostMultiVillages(this.userList);

    this.responseData = response;
    this.getTopicWithCategories();
    this.getYearWithTopic();
  }

  getUniqueListByKey(inputList:any[], key) {
    return [...new Map(inputList.map(item => [item[key], item])).values()]
  }

  getAllCurrentLastNames() {

    let currentLastNameRow = "";
    this.isNamesTab = true;
    // console.log("ming ", this.currentTopicData.data)
    this.allNamesData = this.currentTopicData.data;

    let village_info = {
      villageId: "",
      name: "",
      num: "",
      fiveLastNames:""
    };
    
    // this.showAllNamesDataList = []
    // const arr = [];
    
    for(let i = 0; i < this.allNamesData.length; i++) {
      console.log(this.allNamesData[i])

      // village_info.villageId = this.allNamesData[i].villageId;
      // village_info.name = this.allNamesData[i].gazetteerName;
      // village_info.num = this.allNamesData[i].totalNumberOfLastNameInVillage;
      // village_info.fiveLastNames = `${this.allNamesData[i].firstLastNameId}${this.allNamesData[i].secondLastNameId}
      // ${this.allNamesData[i].thirdLastNameId}${this.allNamesData[i].fourthLastNameId}
      // ${this.allNamesData[i].fifthLastNameId}`
      this.showAllNamesDataList.push({
        name: this.allNamesData[i].gazetteerName,
        num: this.allNamesData[i].totalNumberOfLastNameInVillage,
        fiveLastNames: `${this.allNamesData[i].firstLastNameId}${this.allNamesData[i].secondLastNameId}
        ${this.allNamesData[i].thirdLastNameId}${this.allNamesData[i].fourthLastNameId}
        ${this.allNamesData[i].fifthLastNameId}`
      })
    }
  
    this.showAllNamesDataListUnique = this.getUniqueListByKey(this.showAllNamesDataList, "name");
    // console.log("result", arr)
    // this.showAllNamesDataList = this.getUniqueListByKey(this.showAllNamesDataList, "name");
    // console.log("result", this.showAllNamesDataList)

  }

  // [tag] select topic category1
  getTopicWithCategories() {
    //by default - hard coded
    this.topicCategory = [];
    let totalResults = [];
    //
    this.category1Set.clear();
    this.category2Set.clear();
    this.category3Set.clear();
    // clear mapMainCategoryToSubCategoryConfig data
    for(let each in this.mapMainCategoryToSubCategoryConfig) {
      this.mapMainCategoryToSubCategoryConfig[each].data = [];
    }

    const targetSet = new Set();
    for(let key in this.mapMainCategoryToSubCategoryConfig){
      targetSet.add(this.mapMainCategoryToSubCategoryConfig[key].target)
    }

    const checkedIndex = this.multiVillages_checkList.filter(item => item.isSelected === true);
    if(this.currentSelectedTopic === undefined) this.currentSelectedTopic = "村庄基本信息";

    for(let index in this.responseData) {
      // choose currentTab as 村庄基本信息, we add data to mapMainCategoryToSubCategoryConfig[item]
      if(this.mapMainCategoryToSubCategoryConfig[this.responseData[index].tableNameChinese] && this.mapMainCategoryToSubCategoryConfig[this.responseData[index].tableNameChinese].target == this.currentSelectedTopic) {
        let currentExtraCategoryData = this.responseData[index];
        this.mapMainCategoryToSubCategoryConfig[this.responseData[index].tableNameChinese].data = currentExtraCategoryData.data;
        // don't display name view
        this.isNamesTab = false;
      }
      else if(this.responseData[index].tableNameChinese === this.currentSelectedTopic) {
        this.currentTopicData = this.responseData[index];
        if(this.responseData[index].tableNameChinese === "姓氏")  this.getAllCurrentLastNames();
        else {
          // don't display name view
          this.isNamesTab = false;
          for(let item in this.responseData[index].data){
            // category1
            this.responseData[index].data[item].category1 && this.category1Set.add(this.responseData[index].data[item].category1);
            // category2
            // this.responseData[index].data[item].category2 && this.category2Set.add(this.responseData[index].data[item].category2);
            // category3
            // this.responseData[index].data[item].category3 && this.category2Set.add(this.responseData[index].data[item].category3);
          }
        }
    }
  }
    // merge main category as sub-category based on mapMainCategoryToSubCategoryConfig
    if(targetSet.has(this.currentSelectedTopic)) {
      for(let each in this.mapMainCategoryToSubCategoryConfig) {
        if(this.mapMainCategoryToSubCategoryConfig[each].target != this.currentSelectedTopic) {
          continue
        }
        // find display name depends on tableNameChinese
        const displayNameFormapMainCategoryToSubCategoryConfig = this.displayedMiddleTabs.find(item=>{
          return item.indexOf(each)!=-1
        });
        this.category1Set.add(displayNameFormapMainCategoryToSubCategoryConfig); // add this selection with display name
      }
    }
    if(this.allTopicChecked) {
      // automatically checked
      this.displayTopicCategory.push({
        selectedTopic: this.currentSelectedTopic,
        category1List: Array.from(this.category1Set),
        hasCategory: true
      })
      this.displayTopicCategory = this.removeDuplicates(this.displayTopicCategory, "selectedTopic");
    }

    // merge some sub-categorys to new sub-category based on mapSubCategoryToNewSubCategory
    const getChinesePart = (v) => {
      return v.split(" ")[0]
    }

    const newCategory1Set = new Set();
    for (let category of this.category1Set) {
      if(this.mapSubCategoryToNewSubCategory[category as string]) {
        newCategory1Set.add(this.mapSubCategoryToNewSubCategory[category as string])
      } else {
        newCategory1Set.add(category)
      }
    }
    this.category1Set = newCategory1Set;

}

  removeDuplicates(originalArray, prop) {
    var newArray = [];
    var lookupObject  = [];
    // this.topicCategory = [];
    for(var i in originalArray) {
       lookupObject[originalArray[i][prop]] = originalArray[i];
    }
    for(i in lookupObject) {
        newArray.push(lookupObject[i]);
    }
     return newArray;
    }

  tabChanged(event) {
    console.log("this response", this.responseData)
    this.allTopicChecked = false;
    this.currentSelectedTopic = this.displayedMiddleTabsFiltered[event.index].split('-')[0]
    this.getTopicWithCategories();
    // this.getYearWithTopic();
    //clear name sets
  }

  //TODO
  getYearWithTopic() {
    this.topicYear = [];
    this.totalYearOnly = [];

    if(Object.prototype.toString.call(this.responseData) === '[object Array]') {
      let matchIndex = this.responseData.findIndex(item => item.tableNameChinese === this.currentSelectedTopic);

      for(let eachID in this.checkedVillagesID) {
        if(this.responseData[matchIndex] && this.responseData[matchIndex].year) {
          let year_index = this.responseData[matchIndex].year.findIndex( (item) =>  {
          if(item[this.checkedVillagesID[eachID]]) {
            const currentTopicEN = this.middleTabsMap.get(this.currentSelectedTopic);
            const getYearObject = item[this.checkedVillagesID[eachID]][0][currentTopicEN];
            for(let eachYearIndex in getYearObject.year_only) {
              if(this.totalYearOnly.indexOf(getYearObject.year_only[eachYearIndex]) === -1) {
                this.totalYearOnly.push(getYearObject.year_only[eachYearIndex]);
                this.totalYearOnly.sort();
              }
            }
          }
        });
        }
      }

      this.topicYear.push({
        topic: this.currentSelectedTopic,
        village_id: this.checkedVillagesID,
        total_year_only: this.totalYearOnly
      })
    }
  }

  checkboxYear(event, selectedYear, checked) {
    // let selectedYear = year.toString();
    if(checked) {
      this.checked_year_only.push(selectedYear);
    }
    else{
      let removeIndex = this.checked_year_only.indexOf(selectedYear);
      if(removeIndex > -1 ) this.checked_year_only.splice(removeIndex, this.checked_year_only.length)
    }


    const values = Object.keys(this.checked_year_only).map(it => this.checked_year_only[it]);


  }

  async postFinalRequest() {

    let resnew = await this.multiVillageFilterService.onPostMultiVillages(
      {
        //BUG 1.checkedall 2. fourthlastNames -- ask backend
          villageid: this.checkedVillagesID,
          // topic: ["gazetteerinformation","naturalenvironment","naturaldisasters", "fourthlastNames",
          //   "firstavailabilityorpurchase","ethnicgroups","population", "military", "economy",
          //   "familyplanning", "education"],
          topic: this.finalPostTopicList.length > 0 ? this.finalPostTopicList : this.defaultTopicList,
          year: this.checked_year_only,
          year_range: this.inputed_year_range
          // year_range: [2009, 2012],
      }
    );
  }

    //if search button is clicked, go to results page
    async goToPage() {
    const mapToOriginalSubCategory = (categoryList) => {
      if(!categoryList || categoryList.length == 0){
        return [];
      }
      const newCategory = new Set();
      for (let category of categoryList){
        let flag = false;
        for (let mapsubcategory in this.mapSubCategoryToNewSubCategory){
          if(this.mapSubCategoryToNewSubCategory[mapsubcategory] == category) {
            flag = true;
            newCategory.add(mapsubcategory)
          }
        }
        if(!flag) {
          newCategory.add(category)
        }
      }
      return newCategory;
    }

      // map newSubCategory to original sub categories
      this.displayTopicCategory.forEach(item => {
        item.category1List = Array.from(mapToOriginalSubCategory(item.category1List));
      });
      this.displayTopicCategory = this.displayTopicCategory.filter(item => item.category1List.length>0)

      console.log('[debug] before send', this.displayTopicCategory);

      for(let item in this.displayTopicCategory) {
        // for mapMainCategoryToSubCategoryConfig
        if(this.mapMainCategoryToSubCategoryConfig[this.displayTopicCategory[item].selectedTopic]) {
          // automatically selected subCategory
          const selectedAllCategorySet = new Set();
          const data = this.mapMainCategoryToSubCategoryConfig[this.displayTopicCategory[item].selectedTopic].data;
          for(let each in data) {
            if(data[each].category1) {
              selectedAllCategorySet.add(data[each].category1);
            }
          }
          for(let each of selectedAllCategorySet) {
            this.displayTopicCategory[item].category1List.push(each);
          }
          // reduce duplicates
          this.displayTopicCategory[item].category1List = Array.from(new Set(this.displayTopicCategory[item].category1List));
          // change hasCategory if it has
          if(this.displayTopicCategory[item].category1List && this.displayTopicCategory[item].category1List.length>0) {
            this.displayTopicCategory[item].hasCategory = true;
          }
        } else if(this.displayTopicCategory[item].selectedTopic === "村庄基本信息") {
          const selectedAllCategorySet = new Set();
          for(let each in this.currentTopicData.data) {
            // [TODO] delete subcategories which in mapMainCategoryToSubCategoryConfig
            if(this.currentTopicData.data[each].category1) {
              selectedAllCategorySet.add(this.currentTopicData.data[each].category1);
            }
          }
          for(let each of selectedAllCategorySet) {
            this.displayTopicCategory[item].category1List.push(each);
          }
          // reduce duplicates
          this.displayTopicCategory[item].category1List = Array.from(new Set(this.displayTopicCategory[item].category1List))
          // change hasCategory if it has
          if(this.displayTopicCategory[item].category1List && this.displayTopicCategory[item].category1List.length>0) {
            this.displayTopicCategory[item].hasCategory = true;
          }
        }
        this.finalPostTopicList.push(this.middleTabsMap.get(this.displayTopicCategory[item].selectedTopic))
      }
      
      // console.log("this.finalPostTopicList" , this.finalPostTopicList)
      
      if(this.finalPostTopicList.length === 0 && this.checked_year_only.length === 0) {
        //convert to get request backend
        this.loading = true;
        await this.processRequest();
        this.loading = false;
      }
      else {
        this.loading = true;
        await this.postFinalRequest();
        this.loading = false;
      }

      if(this.displayTopicCategory.length < 1) {
        for(let i = 0; i < this.defaultTopics_InCh.length; i++) {    
        this.displayTopicCategory.push({
        selectedTopic: this.defaultTopics_InCh[i],
        hasCategory: false
        }) 
      }

      }

      const userValue = this.storeUserSelection();
      const userList = this.storeUserList();
      let baseUrl = window.location.href.replace(this.router.url, '');
      const url = baseUrl + this.router.createUrlTree(['/multi-village-search-result']);
      console.log('[debug] url:', baseUrl, url);
      window.open(url, '_blank');
      // this.router.navigate(["/multi-village-search-result"]);
    }
  checkAllTopics() {

    if(this.allTopicChecked) {
      // uncheck all
      this.category1Selection([]);
      this.allTopicChecked = false;
      this.category1Selected = [];
    }
    else {
      this.category1Selection(this.category1Set);
      this.category1Selected = Array.from(this.category1Set);
      this.allTopicChecked = true;
      this.displayTopicCategory.push({
        selectedTopic: this.currentSelectedTopic,
        category1List: Array.from(this.category1Set),
        hasCategory: true
      })
      this.displayTopicCategory = this.removeDuplicates(this.displayTopicCategory, "selectedTopic");
    }

  }
  // options: MatListOption[] - call multi-times
  category1Selection(selectedCategory1List) {
    this.category2Set.clear();

    // generate targetSet
    const targetSet = new Set();
    for(let key in this.mapMainCategoryToSubCategoryConfig){
      targetSet.add(this.mapMainCategoryToSubCategoryConfig[key].target)
    }

    // console.log('[debug] current data', this.currentTopicData);
    for(let i = 0; i < selectedCategory1List.length; i++) {
      for(let item in this.currentTopicData.data) {
        if(this.currentTopicData.data[item].category1 && this.currentTopicData.data[item].category1 === selectedCategory1List[i]) {
        //   if(this.currentTopicData.data[item].category2 && this.currentTopicData.data[item].category2 !== "null") {
        //   this.category2Set.add(this.currentTopicData.data[item].category2);
        // }
        }
      }
    }
    if(this.currentSelectedTopic in targetSet) {
      for(let category1 of selectedCategory1List) {
        let key = this.middleTabsEng2ChineseMap.get(category1);
        if(this.mapMainCategoryToSubCategoryConfig[key] && this.mapMainCategoryToSubCategoryConfig[key].data && this.mapMainCategoryToSubCategoryConfig[key].data.length>=0) {
          this.displayTopicCategory.push({
            selectedTopic: key,
            category1List: [],
            hasCategory: false,
          })
        }
      }
    }
      this.displayTopicCategory.push({
        selectedTopic: this.currentSelectedTopic,
        category1List: selectedCategory1List,
        hasCategory: true
      })  
      this.displayTopicCategory = this.removeDuplicates(this.displayTopicCategory, "selectedTopic");




    // let results_c1 = [];
    // //BUG
    // // this.displayTopicCategory = [];
    // for(let index in this.topicCategory) {
    //   for(let item in checkedList) { 
    //     checkedList[item].isSelected = true; 
    //     if(results_c1.indexOf(checkedList[item].category1) === -1) {
    //       results_c1.push(checkedList[item].category1);
    //     }
    //   }
    //   this.category2_checkedList = checkedList;
    // }

    // if(results_c1.length > 0) {
    //   this.displayTopicCategory.push({
    //     selectedTopic: this.currentSelectedTopic,
    //     selectedCategoryList: results_c1
    //   })  
    // }

    // this.displayTopicCategory = this.removeDuplicates(this.displayTopicCategory, "selectedTopic");
    //     // console.log("topic select",this.displayTopicCategory);

  }

  storeUserSelection(): string {
    const userSelection = JSON.stringify(this.displayTopicCategory);
    // [TODO] encode userValue and return it
    // const userValueEncoded = encode(userValue);
    window.localStorage.setItem("userSelection", userSelection);
    return userSelection;
  }
  storeUserList() {
    const userListString = JSON.stringify(this.userList);
    // [TODO] encode userListString and return it
    // const userListStringEncoded = encode(userListString);
    window.localStorage.setItem("userList", userListString);
    return userListString;
  }

  category2Selection(selectedCategory2List) {
    this.category3Set.clear();
    for(let i = 0; i < selectedCategory2List.length; i++) {
      for(let item in this.currentTopicData.data) {
        if(this.currentTopicData.data[item].category2 && this.currentTopicData.data[item].category2 === selectedCategory2List[i]) {
          if(this.currentTopicData.data[item].category3 && this.currentTopicData.data[item].category3 !== "null") {
          this.category3Set.add(this.currentTopicData.data[item].category3);
        }}
      }
    }
    // window.localStorage.setItem("user selection", JSON.stringify(this.displayTopicCategory));

    // this.displayTopicCategory.push({
    //   selectedTopic: this.currentSelectedTopic,
    //   category1List: selectedCategory1List
    // })  
    // this.displayTopicCategory = this.removeDuplicates(this.displayTopicCategory, "selectedTopic");

    // console.log("this.category3Set",this.category3Set)
  }
    //TODO  use dynamic db data - Later
    middleCheckBox(event: MatCheckboxChange) {
      const selectedText = event.source._elementRef.nativeElement.innerText;
      // console.log(this.responseData);
    }

  //       //TODO
  //       //   const getChineseWordCategory1 = item.category1
  //       //     .split('')
  //       //     .filter((char) => /\p{Script=Han}/u.test(char))
  //       //     .join('');
  //       //   if (
  //       //     this.middleBoxCategory1.indexOf(getChineseWordCategory1) == -1
  //       //   ) {
  //       //     this.middleBoxCategory1.push(getChineseWordCategory1);
  //       //     this.cat1Cat2Map.set(item.category1, item.category2);
  //       //     // console.log('trigger');
  //       //     // this.category1Map.set(element.id, this.middleBoxCategory1);
  //       //   }
  //       // });
  //       // console.log('filter category 1 result ', this.middleBoxCategory1);
  //     })
  //     .catch(function (error) {
  //       console.log(error);
  //     });
  // }

  //TODO
  onCreatePost(postData: { villageid: any; topic: any }) {
    // Send Http request
    this.httpService.post("advancesearch", postData).then((responseData) => {
        // console.log("responseData", responseData);
      });
  }

  // onInputStartYearField(event: any) {
  //   this.startYearInput = event.target.value;
  //   console.log(event.target.value);
  // }
  // onInputEndYearField(event: any) {
  //   this.endYearInput = event.target.value;
  //   // console.log(event.target.value);
  // }

  async onPostInputYear() {
    // console.log("value", this.value1)
    this.inputed_year_range = [
      parseInt(this.startYearInput),
      parseInt(this.endYearInput),
    ];
  }

  rightTopYearCheckBox(event: MatCheckboxChange) {
    this.singleYearSelected = event.source._elementRef.nativeElement.innerText;
    //IMPORTANT
    if (event.checked) {
      // m;
      // console.log("singleYearSelected", this.singleYearSelected);
      this.postYearData.year.push(parseInt(this.singleYearSelected));
      this.rightToptempcheckItems.push(event.source.name);
    } else {
      //remove the uncheck single year
      this.postYearData.year.splice(
        this.postYearData.year.indexOf(this.singleYearSelected)
      );
      var index = this.rightToptempcheckItems.indexOf(event.source.name);
      if (index > -1) {
        this.rightToptempcheckItems.splice(index, 1);
      }
      // this.checkItems.delete(element.id);
    }

    //do post to year serve
    this.onPostInputYear();

    // console.log(this.postYearData);
  }

  resetAll() {
    location.reload()
    //left
    this.masterSelected = false;
    this.multiVillages_checkedList = []
    this.userList = [];
    //middle
    this.allTopicChecked = false;
    this.category1Set.clear();
    this.topicCategory = [];
    this.category2_checkedList = [];
    this.displayTopicCategory = [];

  }
}