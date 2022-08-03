import { Component } from '@angular/core';
import { NaturalDisaster } from './services/village-name.service';



@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css'],
})
export class AppComponent {
  title = 'CCVGproject';
  // tables = {   // table 3
  //   tableNameChinese: '自然灾害',
  //   columnsName: [
  //     {columnsHeaderChinese:'村志书名',columnsDef:'gazetteerName',cell: (row: NaturalDisaster) => `${row.gazetteerName}`},
  //     {columnsHeaderChinese:'村志代码',columnsDef:'gazetteerId', cell: (row: NaturalDisaster) => `${row.gazetteerId}`},
  //     {columnsHeaderChinese:'年份',columnsDef:'year',cell: (row: NaturalDisaster) => `${row.year}`},
  //     {columnsHeaderChinese:'类别',columnsDef:'category',cell: (row: NaturalDisaster) => `${row.category}`},
  //   ],
  //   field: ['gazetteerName','gazetteerId','year','category'],
  //   data: [
  //     {gazetteerName: '叶店村志', gazetteerId: 2, year: 1954, category: '水灾 Flood'},
  //     {gazetteerName: '叶店村志', gazetteerId: 2, year: 1983, category: '水灾 Flood'},
  //     {gazetteerName: '叶店村志', gazetteerId: 2, year: 1998, category: '水灾 Flood'},
  //     {gazetteerName: '叶店村志', gazetteerId: 2, year: 1956, category: '虫害 Pestilence'},
  //     {gazetteerName: '叶店村志', gazetteerId: 2, year: 1980, category: '风灾 Windstorm'},
  //     {gazetteerName: '叶店村志', gazetteerId: 2, year: 1980, category: '龙卷风 Tornado'},
  //   ],
  //   filterFormGroupName: 'naturalDisasterFormGroup',
  //   filters:[
  //     {description: '风灾', type: 'windstorm'},
  //     {description: '水灾', type: 'flood'},
  //     {description: '虫害', type: 'pestilence'},
  //     {description: '龙卷风', type: 'tornado'},
  //   ],
  // };

}
