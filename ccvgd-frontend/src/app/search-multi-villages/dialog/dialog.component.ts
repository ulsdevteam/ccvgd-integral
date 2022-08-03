import { Component, EventEmitter, Inject, OnInit, Output } from '@angular/core';
import {MatDialog, MatDialogRef, MAT_DIALOG_DATA} from '@angular/material/dialog'
import { SearchMultiVillagesComponent } from "../search-multi-villages.component"

@Component({
  selector: 'app-dialog',
  templateUrl: './dialog.component.html',
  styleUrls: ['./dialog.component.css']
})
export class DialogComponent implements OnInit {
  @Output() okIsClick: EventEmitter<boolean> = new EventEmitter();

  constructor(public dialogRef: MatDialogRef<DialogComponent>) { }

  ngOnInit(): void {
  }
  onNoClick(): void {
    this.dialogRef.close();
  }

  // okIsClicked() {
  //   this.okIsClick.emit(true);
  // }
}
