import { Village } from './../services/village-name.service';
import { Component, OnDestroy, OnInit } from '@angular/core';
import {
  BasicGazetteerInformation,
  BasicVillageInformation,
  FirstAvalabilityOrPurchase,
  LastName,
  NaturalDisaster,
  NaturalEnvironment,
  OneLevelResult,
  TableData,
  ThreeLevelResult,
  TwoLevelResult,
} from '../services/village-name.service';
import { StateServiceService } from '../services/state-service.service';
import { MatCheckboxChange } from '@angular/material/checkbox';
import { SingleVillageSearchResultService } from '../services/single-village-search-result.service';
import { ActivatedRoute } from '@angular/router';
import { HttpServiceService } from "../services/http-service.service";
import { environment} from "../../environments/environment";

@Component({
  selector: 'app-single-village-search-result',
  templateUrl: './single-village-search-result.component.html',
  styleUrls: ['./single-village-search-result.component.css'],
})
export class SingleVillageSearchResultComponent implements OnInit {
  searchResult: TableData[];
  selectedTable: TableData;

  //for download
  checkedTables: string[];
  downloadLink: any;

  choose;

  tables = [
    {
      // table1
      tableNameChinese: '村庄基本信息',
      columnsName: [
        {
          columnsHeaderChinese: '村志代码',
          columnsDef: 'gazetteerId',
          cell: (row: BasicVillageInformation) => `${row.gazetteerId}`,
        },
        {
          columnsHeaderChinese: '村志书名',
          columnsDef: 'gazetteerName',
          cell: (row: BasicVillageInformation) => `${row.gazetteerName}`,
        },
        {
          columnsHeaderChinese: '村庄代码',
          columnsDef: 'villageId',
          cell: (row: BasicVillageInformation) => `${row.villageId}`,
        },
        {
          columnsHeaderChinese: '村庄名字',
          columnsDef: 'villageName',
          cell: (row: BasicVillageInformation) => `${row.villageName}`,
        },
        {
          columnsHeaderChinese: '省',
          columnsDef: 'province',
          cell: (row: BasicVillageInformation) => `${row.province}`,
        },
        {
          columnsHeaderChinese: '市',
          columnsDef: 'city',
          cell: (row: BasicVillageInformation) => `${row.city}`,
        },
        {
          columnsHeaderChinese: '县',
          columnsDef: 'county',
          cell: (row: BasicVillageInformation) => `${row.county}`,
        },
        {
          columnsHeaderChinese: '分类',
          columnsDef: 'category1',
          cell: (row: BasicVillageInformation) => `${row.category1}`,
        },
        {
          columnsHeaderChinese: '数据',
          columnsDef: 'data',
          cell: (row: BasicVillageInformation) => `${row.data}`,
        },
        {
          columnsHeaderChinese: '单位',
          columnsDef: 'unit',
          cell: (row: BasicVillageInformation) => `${row.unit}`,
        },
      ],
      field: [
        'gazetteerId',
        'gazetteerName',
        'villageId',
        'villageName',
        'province',
        'city',
        'county',
        'category1',
        'data',
        'unit',
      ],
      data: [],
      treeFilter: {},
      //download
      topic: 'village',
      id: '1',
    }, // end of table 1
    {
      //table 2
      tableNameChinese: '村志基本信息',
      columnsName: [
        {
          columnsHeaderChinese: '村庄代码',
          columnsDef: 'villageId',
          cell: (row: BasicGazetteerInformation) => `${row.villageId}`,
        },
        {
          columnsHeaderChinese: '村庄名字',
          columnsDef: 'villageName',
          cell: (row: BasicGazetteerInformation) => `${row.villageName}`,
        },
        {
          columnsHeaderChinese: '村志代码',
          columnsDef: 'gazetteerId',
          cell: (row: BasicGazetteerInformation) => `${row.gazetteerId}`,
        },
        {
          columnsHeaderChinese: '村志书名',
          columnsDef: 'gazetteerName',
          cell: (row: BasicGazetteerInformation) => `${row.gazetteerName}`,
        },
        {
          columnsHeaderChinese: '出版年份',
          columnsDef: 'publishYear',
          cell: (row: BasicGazetteerInformation) => `${row.publishYear}`,
        },
        {
          columnsHeaderChinese: '出版类型',
          columnsDef: 'publishType',
          cell: (row: BasicGazetteerInformation) => `${row.publishType}`,
        },
      ],
      field: [
        'villageId',
        'villageName',
        'gazetteerId',
        'gazetteerName',
        'publishYear',
        'publishType',
      ],
      data: [],
      treeFilter: {},
      topic: 'gazetteerinformation',
      id: '2',
    }, // end of table 2
    {
      // table 3
      tableNameChinese: '自然灾害',
      columnsName: [
        {
          columnsHeaderChinese: '村志书名',
          columnsDef: 'gazetteerName',
          cell: (row: NaturalDisaster) => `${row.gazetteerName}`,
        },
        {
          columnsHeaderChinese: '村志代码',
          columnsDef: 'gazetteerId',
          cell: (row: NaturalDisaster) => `${row.gazetteerId}`,
        },
        {
          columnsHeaderChinese: '年份',
          columnsDef: 'year',
          cell: (row: NaturalDisaster) => `${row.year}`,
        },
        {
          columnsHeaderChinese: '类别',
          columnsDef: 'category1',
          cell: (row: NaturalDisaster) => `${row.category1}`,
        },
      ],
      field: ['gazetteerName', 'gazetteerId', 'year', 'category1'],
      data: [],
      treeFilter: {},
      topic: 'naturaldisasters',
      id: '3',
      // filters2
    }, // end of table 3
    {
      tableNameChinese: '自然环境',
      columnsName: [
        {
          columnsHeaderChinese: '村志书名',
          columnsDef: 'gazetteerName',
          cell: (row: NaturalEnvironment) => `${row.gazetteerName}`,
        },
        {
          columnsHeaderChinese: '村志代码',
          columnsDef: 'gazetteerId',
          cell: (row: NaturalEnvironment) => `${row.gazetteerId}`,
        },
        {
          columnsHeaderChinese: '类别',
          columnsDef: 'category1',
          cell: (row: NaturalEnvironment) => `${row.category1}`,
        },
        {
          columnsHeaderChinese: '数据',
          columnsDef: 'data',
          cell: (row: NaturalEnvironment) => `${row.data}`,
        },
        {
          columnsHeaderChinese: '单位',
          columnsDef: 'unit',
          cell: (row: NaturalEnvironment) => `${row.unit}`,
        },
      ],
      field: ['gazetteerName', 'gazetteerId', 'category1', 'data', 'unit'],
      data: [],
      treeFilter: {},
      topic: 'naturalenvironment',
      id: '4',
    },
    {
      tableNameChinese: '军事政治',
      columnsName: [
        {
          columnsHeaderChinese: '村志书名',
          columnsDef: 'gazetteerName',
          cell: (row: TwoLevelResult) => `${row.gazetteerName}`,
        },
        {
          columnsHeaderChinese: '村志代码',
          columnsDef: 'gazetteerId',
          cell: (row: TwoLevelResult) => `${row.gazetteerId}`,
        },
        {
          columnsHeaderChinese: '类别1',
          columnsDef: 'category1',
          cell: (row: TwoLevelResult) => `${row.category1}`,
        },
        {
          columnsHeaderChinese: '类别2',
          columnsDef: 'category2',
          cell: (row: TwoLevelResult) => `${row.category2}`,
        },
        {
          columnsHeaderChinese: '开始年份',
          columnsDef: 'startYear',
          cell: (row: TwoLevelResult) => `${row.startYear}`,
        },
        {
          columnsHeaderChinese: '结束年份',
          columnsDef: 'endYear',
          cell: (row: TwoLevelResult) => `${row.endYear}`,
        },
        {
          columnsHeaderChinese: '数据',
          columnsDef: 'data',
          cell: (row: TwoLevelResult) => `${row.data}`,
        },
        {
          columnsHeaderChinese: '单位',
          columnsDef: 'unit',
          cell: (row: TwoLevelResult) => `${row.unit}`,
        },
      ],
      field: [
        'gazetteerName',
        'gazetteerId',
        'category1',
        'category2',
        'startYear',
        'endYear',
        'data',
        'unit',
      ],
      data: [],
      treeFilter: {},
      topic: 'military',
      id: '5',
    },
    {
      tableNameChinese: '教育',
      columnsName: [
        {
          columnsHeaderChinese: '村志书名',
          columnsDef: 'gazetteerName',
          cell: (row: ThreeLevelResult) => `${row.gazetteerName}`,
        },
        {
          columnsHeaderChinese: '村志代码',
          columnsDef: 'gazetteerId',
          cell: (row: ThreeLevelResult) => `${row.gazetteerId}`,
        },
        {
          columnsHeaderChinese: '类别1',
          columnsDef: 'category1',
          cell: (row: ThreeLevelResult) => `${row.category1}`,
        },
        {
          columnsHeaderChinese: '类别2',
          columnsDef: 'category2',
          cell: (row: ThreeLevelResult) => `${row.category2}`,
        },
        {
          columnsHeaderChinese: '开始年份',
          columnsDef: 'startYear',
          cell: (row: ThreeLevelResult) => `${row.startYear}`,
        },
        {
          columnsHeaderChinese: '结束年份',
          columnsDef: 'endYear',
          cell: (row: ThreeLevelResult) => `${row.endYear}`,
        },
        {
          columnsHeaderChinese: '数据',
          columnsDef: 'data',
          cell: (row: ThreeLevelResult) => `${row.data}`,
        },
        {
          columnsHeaderChinese: '单位',
          columnsDef: 'unit',
          cell: (row: ThreeLevelResult) => `${row.unit}`,
        },
      ],
      field: [
        'gazetteerName',
        'gazetteerId',
        'category1',
        'category2',
        'startYear',
        'endYear',
        'data',
        'unit',
      ],
      data: [],
      treeFilter: {},
      topic: 'education',
      id: '5',
    },
    {
      tableNameChinese: '经济',
      columnsName: [
        {
          columnsHeaderChinese: '村志书名',
          columnsDef: 'gazetteerName',
          cell: (row: ThreeLevelResult) => `${row.gazetteerName}`,
        },
        {
          columnsHeaderChinese: '村志代码',
          columnsDef: 'gazetteerId',
          cell: (row: ThreeLevelResult) => `${row.gazetteerId}`,
        },
        {
          columnsHeaderChinese: '类别1',
          columnsDef: 'category1',
          cell: (row: ThreeLevelResult) => `${row.category1}`,
        },
        {
          columnsHeaderChinese: '类别2',
          columnsDef: 'category2',
          cell: (row: ThreeLevelResult) => `${row.category2}`,
        },
        {
          columnsHeaderChinese: '类别3',
          columnsDef: 'category3',
          cell: (row: ThreeLevelResult) => `${row.category3}`,
        },
        {
          columnsHeaderChinese: '开始年份',
          columnsDef: 'startYear',
          cell: (row: ThreeLevelResult) => `${row.startYear}`,
        },
        {
          columnsHeaderChinese: '结束年份',
          columnsDef: 'endYear',
          cell: (row: ThreeLevelResult) => `${row.endYear}`,
        },
        {
          columnsHeaderChinese: '数据',
          columnsDef: 'data',
          cell: (row: ThreeLevelResult) => `${row.data}`,
        },
        {
          columnsHeaderChinese: '单位',
          columnsDef: 'unit',
          cell: (row: ThreeLevelResult) => `${row.unit}`,
        },
      ],
      field: [
        'gazetteerName',
        'gazetteerId',
        'category1',
        'category2',
        'category3',
        'startYear',
        'endYear',
        'data',
        'unit',
      ],
      data: [],
      treeFilter: {},
      topic: 'economy',
      id: '6',
    },
    {
      tableNameChinese: '计划生育',
      columnsName: [
        {
          columnsHeaderChinese: '村志书名',
          columnsDef: 'gazetteerName',
          cell: (row: OneLevelResult) => `${row.gazetteerName}`,
        },
        {
          columnsHeaderChinese: '村志代码',
          columnsDef: 'gazetteerId',
          cell: (row: OneLevelResult) => `${row.gazetteerId}`,
        },
        {
          columnsHeaderChinese: '类别',
          columnsDef: 'category1',
          cell: (row: OneLevelResult) => `${row.category1}`,
        },
        {
          columnsHeaderChinese: '开始年份',
          columnsDef: 'startYear',
          cell: (row: OneLevelResult) => `${row.startYear}`,
        },
        {
          columnsHeaderChinese: '结束年份',
          columnsDef: 'endYear',
          cell: (row: OneLevelResult) => `${row.endYear}`,
        },
        {
          columnsHeaderChinese: '数据',
          columnsDef: 'data',
          cell: (row: OneLevelResult) => `${row.data}`,
        },
        {
          columnsHeaderChinese: '单位',
          columnsDef: 'unit',
          cell: (row: OneLevelResult) => `${row.unit}`,
        },
      ],
      field: [
        'gazetteerName',
        'gazetteerId',
        'category1',
        'startYear',
        'endYear',
        'data',
        'unit',
      ],
      data: [],
      treeFilter: {},
      topic: 'familyplanning',
      id: '7',
    },
    {
      tableNameChinese: '人口',
      columnsName: [
        {
          columnsHeaderChinese: '村志书名',
          columnsDef: 'gazetteerName',
          cell: (row: TwoLevelResult) => `${row.gazetteerName}`,
        },
        {
          columnsHeaderChinese: '村志代码',
          columnsDef: 'gazetteerId',
          cell: (row: TwoLevelResult) => `${row.gazetteerId}`,
        },
        {
          columnsHeaderChinese: '类别1',
          columnsDef: 'category1',
          cell: (row: TwoLevelResult) => `${row.category1}`,
        },
        {
          columnsHeaderChinese: '类别2',
          columnsDef: 'category2',
          cell: (row: TwoLevelResult) => `${row.category2}`,
        },
        {
          columnsHeaderChinese: '开始年份',
          columnsDef: 'startYear',
          cell: (row: TwoLevelResult) => `${row.startYear}`,
        },
        {
          columnsHeaderChinese: '结束年份',
          columnsDef: 'endYear',
          cell: (row: TwoLevelResult) => `${row.endYear}`,
        },
        {
          columnsHeaderChinese: '数据',
          columnsDef: 'data',
          cell: (row: TwoLevelResult) => `${row.data}`,
        },
        {
          columnsHeaderChinese: '单位',
          columnsDef: 'unit',
          cell: (row: TwoLevelResult) => `${row.unit}`,
        },
      ],
      field: [
        'gazetteerName',
        'gazetteerId',
        'category1',
        'category2',
        'startYear',
        'endYear',
        'data',
        'unit',
      ],
      data: [],
      treeFilter: {},
      topic: 'population',
      id: '8',
    },
    {
      tableNameChinese: '民族',
      columnsName: [
        {
          columnsHeaderChinese: '村志书名',
          columnsDef: 'gazetteerName',
          cell: (row: OneLevelResult) => `${row.gazetteerName}`,
        },
        {
          columnsHeaderChinese: '村志代码',
          columnsDef: 'gazetteerId',
          cell: (row: OneLevelResult) => `${row.gazetteerId}`,
        },
        {
          columnsHeaderChinese: '类别',
          columnsDef: 'category1',
          cell: (row: OneLevelResult) => `${row.category1}`,
        },
        {
          columnsHeaderChinese: '开始年份',
          columnsDef: 'startYear',
          cell: (row: OneLevelResult) => `${row.startYear}`,
        },
        {
          columnsHeaderChinese: '结束年份',
          columnsDef: 'endYear',
          cell: (row: OneLevelResult) => `${row.endYear}`,
        },
        {
          columnsHeaderChinese: '数据',
          columnsDef: 'data',
          cell: (row: OneLevelResult) => `${row.data}`,
        },
        {
          columnsHeaderChinese: '单位',
          columnsDef: 'unit',
          cell: (row: OneLevelResult) => `${row.unit}`,
        },
      ],
      field: [
        'gazetteerName',
        'gazetteerId',
        'category1',
        'startYear',
        'endYear',
        'data',
        'unit',
      ],
      data: [],
      treeFilter: {},
      // multiselectFilter: [ '汉族', '壮族', '回族', '满族', '维吾尔族', '苗族', '彝族', '土家族', '藏族',
      //   '蒙古族', '侗族', '布依族', '瑶族', '白族', '朝鲜族', '哈尼族', '黎族', '哈萨克族', '傣族', '畲族',
      //   '傈僳族', '东乡族', '仡佬族', '拉祜族', '佤族', '水族', '纳西族', '羌族', '土族', '仫佬族',
      //   '锡伯族', '柯尔克孜族', '景颇族', '达斡尔族', '撒拉族', '布朗族', '毛南族', '塔吉克族', '普米族', '阿昌族',
      //   '怒族', '鄂温克族', '京族', '基诺族', '德昂族', '保安族', '俄罗斯族', '裕固族', '乌兹别克族', '门巴族',
      //   '鄂伦春族', '独龙族', '赫哲族', '高山族', '珞巴族', '塔塔尔族', '少数民族 (总)' ],
      topic: 'ethnicgroups',
      id: '9',
    },
    {
      tableNameChinese: '姓氏',
      columnsName: [
        {
          columnsHeaderChinese: '村志书名',
          columnsDef: 'gazetteerName',
          cell: (row: LastName) => `${row.gazetteerName}`,
        },
        {
          columnsHeaderChinese: '村志代码',
          columnsDef: 'gazetteerId',
          cell: (row: LastName) => `${row.gazetteerId}`,
        },
        {
          columnsHeaderChinese: '第一大姓',
          columnsDef: 'firstLastNameId',
          cell: (row: LastName) => `${row.firstLastNameId}`,
        },
        {
          columnsHeaderChinese: '第二大姓',
          columnsDef: 'secondLastNameId',
          cell: (row: LastName) => `${row.secondLastNameId}`,
        },
        {
          columnsHeaderChinese: '第三大姓',
          columnsDef: 'thirdLastNameId',
          cell: (row: LastName) => `${row.thirdLastNameId}`,
        },
        {
          columnsHeaderChinese: '第四大姓',
          columnsDef: 'fourthLastNameId',
          cell: (row: LastName) => `${row.fourthLastNameId}`,
        },
        {
          columnsHeaderChinese: '第五大姓',
          columnsDef: 'fifthLastNameId',
          cell: (row: LastName) => `${row.fifthLastNameId}`,
        },
        {
          columnsHeaderChinese: '姓氏总数',
          columnsDef: 'totalNumberOfLastNameInVillage',
          cell: (row: LastName) => `${row.totalNumberOfLastNameInVillage}`,
        },
      ],
      field: [
        'gazetteerName',
        'gazetteerId',
        'firstLastNameId',
        'secondLastNameId',
        'thirdLastNameId',
        'fourthLastNameId',
        'fifthLastNameId',
        'totalNumberOfLastNameInVillage',
      ],
      data: [],
      treeFilter: {},
      //lastnameFilter:['杨', '邓', '叶', '赵', '孙'],
      topic: 'fourthlastNames',
      id: '9',
    },
    {
      tableNameChinese: '第一次购买或拥有年份',
      columnsName: [
        {
          columnsHeaderChinese: '村志书名',
          columnsDef: 'gazetteerName',
          cell: (row: FirstAvalabilityOrPurchase) => `${row.gazetteerName}`,
        },
        {
          columnsHeaderChinese: '村志代码',
          columnsDef: 'gazetteerId',
          cell: (row: FirstAvalabilityOrPurchase) => `${row.gazetteerId}`,
        },
        {
          columnsHeaderChinese: '类别',
          columnsDef: 'category',
          cell: (row: FirstAvalabilityOrPurchase) => `${row.category1}`,
        },
        {
          columnsHeaderChinese: '年份',
          columnsDef: 'year',
          cell: (row: FirstAvalabilityOrPurchase) => `${row.year}`,
        },
      ],
      field: ['gazetteerName', 'gazetteerId', 'category', 'year'],
      data: [],
      treeFilter: {},
      topic: 'firstavailabilityorpurchase',
      id: '10',
    },
  ];

  constructor(
    private stateService: StateServiceService,
    private villageSearchResultService: SingleVillageSearchResultService,
    private activatedRoute: ActivatedRoute,
    private httpService: HttpServiceService
  ) {}
  // ngOnDestroy(): void {
  //   console.log('destroy');
  //   this.stateService.data = this.searchResult;
  // }

  ngOnInit(): void {
    // this.choose = this.activatedRoute.snapshot.paramMap.get('choose');
    // console.log(this.choose);
    // console.log('hello');

    // this.searchResult = this.stateService.data;
    // console.log('🛋 this.searchResult' + this.searchResult);

    // !!! BUG local storage - solve CORS policy issue
    // this.searchResult = JSON.parse(window.localStorage.getItem('result'));
    // console.log(
    //   `here are the fake date for search result 👌🏻 : \n ${JSON.stringify(
    //     this.searchResult
    //   )}`
    // );
    // console.log(typeof this.selectedTable);
    //this.selectedTable = this.searchResult[0];

    // mock data
    // this.searchResult = this.tables;

    //BUG comment out the follow two lines
    this.choose = JSON.parse(window.localStorage.getItem('choose'));
    // console.log();
    this.search(this.choose);
  }

  search(choose: Village) {
    this.villageSearchResultService.searchEncap(choose).then((res) => {
      this.searchResult = res.tables;
      // console.log(this.searchResult);
      this.selectedTable = this.searchResult[0];
    });
  }

  onSelect(table: TableData) {
    this.selectedTable = table;
    console.log('selected table: ', this.selectedTable);
    //past code
    // this.downloadLink =
    //   'http://ngrok.luozm.me:8395/ccvg/download' +
    //   '/' +
    //   this.selectedTable.id +
    //   '_' +
    //   this.selectedTable.topic +
    //   '.csv';

    // for deploy
    this.downloadLink = `${environment.API_ROOT}/download/${this.selectedTable.id}_${this.selectedTable.topic}.csv`;


    //future improve
    // this.httpService.get(`download/${this.selectedTable.id}_${this.selectedTable.topic}.csv`);


  }

  checktables(topic: string, checked: MatCheckboxChange) {
    console.log('1235435', checked);

    if (checked) {
      this.checkedTables.push(topic);
    } else {
      const index = this.checkedTables.indexOf(topic, 0);
      if (index > -1) {
        this.checkedTables.splice(index, 1);
      }
    }
    console.log('this.checkedTables', this.checkedTables);
  }
}
