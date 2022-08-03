import { Injectable } from '@angular/core';
import {environment} from '../../environments/environment';
import {HttpClient} from '@angular/common/http';
import { Observable, throwError } from 'rxjs';
import { catchError, retry } from 'rxjs/operators';

const API_ROOT = environment.API_ROOT;
@Injectable({
  providedIn: 'root'
})
export class HttpServiceService {

  constructor(private httpClient: HttpClient) {

  }

  get<T>(url:string,params:{[key:string]:any} = {}):Promise<T>{
    return this.httpClient.get<T>(`${API_ROOT}${url}`,{
      params:params
    }).toPromise();
  }

  post<T>(url:string,body:{[key:string]:any} = {}):Promise<T>{
    return this.httpClient.post<T>(`${API_ROOT}${url}`,body).toPromise()
  }

}
