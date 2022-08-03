import { Injectable } from "@angular/core";
import { HttpServiceService } from "./http-service.service";
import { HttpClient } from "@angular/common/http";
import {
  Village,
  VillageSearchResult,
  TableData,
} from "./village-name.service";
import { HttpErrorResponse } from "@angular/common/http";
@Injectable({
  providedIn: "root",
})
export class MultiVillageFilterService {
  getResults: VillageSearchResult;
  getUserList: any = {};//{ villageid: ["1"], topic: ["population"] }; // debug
  constructor(
    private httpService: HttpServiceService,
    private http: HttpClient
  ) {}

  async filterSelectedTopics(choose: Village): Promise<any> {
    const selectedTopic = "economy";

    //TODO
    console.log("my choose id: ", choose.id);

    //TODO
    const myObj = {
      villageid: ["1", "3", "2"],
      topic: ["economy", "population"],
    };
    let response = await this.httpService
      .post("advancesearch", {
        myObj,
        // villageid: choose.id,
        // topic: selectedTopic,
      })
      .catch((err: HttpErrorResponse) => {
        console.log("error for request", err);
        return { data: [] };
      });

    console.log("i got the response", response);

    return response;
  }

  async onPostMultiVillages(postData: { villageid: any; topic: any; year? : any; year_range?: any }) {
    if(postData.year && postData.year.length == 0){
      delete postData.year;
    }
    if(postData.year_range && postData.year_range.length == 0){
      delete postData.year_range;
    }

    console.log("post", postData);
    // this.getYearBySelectedVillagesAndTopics(postData);
    let response = await this.httpService
      .post("advancesearch", postData)
      .catch((err: HttpErrorResponse) => {
        console.log("error for request", err);
        return { data: [], error: err.message };
      });
    return response;
  }

  async postYearMultiVillages(postYearData: {
    villageid: any;
    topic: any;
    year: any;
    year_range: any;
  }) {
    let yearResponse = await this.httpService
      .post("advancesearch", postYearData)
      .catch((err: HttpErrorResponse) => {
        console.log("cannot get response data from year filter", err);
        return { data: [] };
      });

    return yearResponse;
  }
}
