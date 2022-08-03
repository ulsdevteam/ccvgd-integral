import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';
import { RouterModule } from '@angular/router';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';


import { AppComponent } from './app.component';
import { TopBarComponent } from './top-bar/top-bar.component';
import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatSliderModule } from '@angular/material/slider';
import { MatOptionModule } from '@angular/material/core';
import { MatAutocompleteModule } from '@angular/material/autocomplete';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { A11yModule } from '@angular/cdk/a11y';
import { ClipboardModule } from '@angular/cdk/clipboard';
import { DragDropModule } from '@angular/cdk/drag-drop';
import { PortalModule } from '@angular/cdk/portal';
import { ScrollingModule } from '@angular/cdk/scrolling';
import { CdkStepperModule } from '@angular/cdk/stepper';
import { CdkTableModule } from '@angular/cdk/table';
import { CdkTreeModule } from '@angular/cdk/tree';
import { MatBadgeModule } from '@angular/material/badge';
import { MatBottomSheetModule } from '@angular/material/bottom-sheet';
import { MatButtonModule } from '@angular/material/button';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { MatCardModule } from '@angular/material/card';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { MatChipsModule } from '@angular/material/chips';
import { MatStepperModule } from '@angular/material/stepper';
import { MatDatepickerModule } from '@angular/material/datepicker';
import { MatDialogModule } from '@angular/material/dialog';
import { MatDividerModule } from '@angular/material/divider';
import { MatExpansionModule } from '@angular/material/expansion';
import { MatGridListModule } from '@angular/material/grid-list';
import { MatIconModule } from '@angular/material/icon';
import { MatListModule } from '@angular/material/list';
import { MatMenuModule } from '@angular/material/menu';
import { MatNativeDateModule, MatRippleModule } from '@angular/material/core';
import { MatPaginatorModule } from '@angular/material/paginator';
import { MatProgressBarModule } from '@angular/material/progress-bar';
import { MatProgressSpinnerModule } from '@angular/material/progress-spinner';
import { MatRadioModule } from '@angular/material/radio';
import { MatSelectModule } from '@angular/material/select';
import { MatSidenavModule } from '@angular/material/sidenav';
import { MatSlideToggleModule } from '@angular/material/slide-toggle';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { MatSortModule } from '@angular/material/sort';
import { MatTableModule } from '@angular/material/table';
import { MatTabsModule } from '@angular/material/tabs';
import { MatToolbarModule } from '@angular/material/toolbar';
import { MatTooltipModule } from '@angular/material/tooltip';
import { MatTreeModule } from '@angular/material/tree';
import { OverlayModule } from '@angular/cdk/overlay';

import { VillageNameService } from './services/village-name.service';
import { SingleVillageSearchComponent } from './single-village-search/single-village-search.component';
import { TableDisplayComponent } from './table-display/table-display.component';
import { TableDisplayV2Component } from './table-display-V2/table-display-V2.component';
import { TableDisplayV22Component } from './table-display-v22/table-display-v22.component';
import { TableDisplayV23Component } from './table-display-v23/table-display-v23.component';
import { HttpClient, HttpClientModule } from '@angular/common/http';
import { SingleVillageSearchV2Component } from './single-village-search-v2/single-village-search-v2.component';
import { TableDetailComponent } from './table-detail/table-detail.component';
import { SingleVillageSearchResultComponent } from './single-village-search-result/single-village-search-result.component';
import { SearchMultiVillagesComponent } from './search-multi-villages/search-multi-villages.component';
import { VillagesearchPipe } from './search-multi-villages/villagesearch.pipe';
import { MuitiVillageResultsComponent } from './muiti-village-results/muiti-village-results.component';
import { DialogComponent } from './search-multi-villages/dialog/dialog.component';

@NgModule({
  declarations: [
    AppComponent,
    TopBarComponent,
    SingleVillageSearchComponent,
    TableDisplayComponent,
    TableDisplayV2Component,
    TableDisplayV22Component,
    TableDisplayV23Component,
    SingleVillageSearchV2Component,
    TableDetailComponent,
    SingleVillageSearchResultComponent,
    SearchMultiVillagesComponent,
    VillagesearchPipe,
    MuitiVillageResultsComponent,
    DialogComponent,
  ],
  imports: [
    HttpClientModule,
    MatCardModule,
    BrowserModule,
    BrowserAnimationsModule,

    FormsModule,
    ReactiveFormsModule,
// =========禁用首页==========
    // RouterModule.forRoot([
    //   {
    //     path: 'single-village-search',
    //     component: SingleVillageSearchV2Component,
    //   },
    //   {
    //     path: 'single-village-search-result',
    //     component: SingleVillageSearchResultComponent,
    //   },
    //   { path: '', redirectTo: '/single-village-search', pathMatch: 'full' },
    // ]),
    RouterModule.forRoot([
      {
        path: 'multi-village-search',
        component: SearchMultiVillagesComponent,
      },
      {
        path: 'multi-village-search-result',
        component: MuitiVillageResultsComponent,
      },
      { path: '', redirectTo: '/multi-village-search', pathMatch: 'full' },
    ]),

    A11yModule,
    ClipboardModule,
    CdkStepperModule,
    CdkTableModule,
    CdkTreeModule,
    DragDropModule,
    MatAutocompleteModule,
    MatBadgeModule,
    MatBottomSheetModule,
    MatButtonModule,
    MatButtonToggleModule,
    MatCardModule,
    MatCheckboxModule,
    MatChipsModule,
    MatStepperModule,
    MatDatepickerModule,
    MatDialogModule,
    MatDividerModule,
    MatExpansionModule,
    MatGridListModule,
    MatIconModule,
    MatInputModule,
    MatListModule,
    MatMenuModule,
    MatNativeDateModule,
    MatPaginatorModule,
    MatProgressBarModule,
    MatProgressSpinnerModule,
    MatRadioModule,
    MatRippleModule,
    MatSelectModule,
    MatSidenavModule,
    MatSliderModule,
    MatSlideToggleModule,
    MatSnackBarModule,
    MatSortModule,
    MatTableModule,
    MatTabsModule,
    MatToolbarModule,
    MatTooltipModule,
    MatTreeModule,
    OverlayModule,
    PortalModule,
    MatAutocompleteModule,
    ScrollingModule,
    MatOptionModule,
    MatFormFieldModule,
  ],
  providers: [VillageNameService],
  bootstrap: [AppComponent],
})
export class AppModule {}
