import { Component, ViewEncapsulation, OnInit } from '@angular/core';
import { VillageNameService} from "./../services/village-name.service";



interface Lang {
  value: number;
  text: string;
}


@Component({
  selector: 'app-top-bar',
  templateUrl: './top-bar.component.html',
  styleUrls: ['./top-bar.component.css'],
  encapsulation: ViewEncapsulation.None,
})
export class TopBarComponent implements OnInit {
  Langs: Lang[] = [
    {value: 0, text: "zhongwen"},
    {value: 1, text: "English"}
  ];
  constructor(private villageNameService: VillageNameService) { }

  ngOnInit(): void {
  }

  changeLanguage(data: any) {
    this.villageNameService.sendMessage(data);
  }
  jumpToCSVLink(){
    let res = confirm('这是一个官方外部链接，对于打开原始CSV文件但是不能正常显示中文的用户可以尝试转换成Excel之后正常显示中文文字;\n' +
        '确定跳转？\n'+
        'This is an official external link. For users who open the original CSV file but cannot display Chinese normally, ' +
        'you can try to convert it to Excel and display Chinese text normally;\n' +
        'Are you sure to redirect?'
    );
    if(res) {

        window.open('https://convertio.co/csv-xlsx/','_blank');

    }
  }

}
