import { Routes } from '@angular/router';

import {HomePageComponent} from './home-page/home-page.component';
import {SearchResultsPageComponent} from './search-results-page/search-results-page.component';
import {DataViewerPageComponent} from './data-viewer-page/data-viewer-page.component';

export const routes: Routes = [
    { path: '', component: HomePageComponent },
    { path: 'search', component: SearchResultsPageComponent },
    { path: 'viewer', component: DataViewerPageComponent },
];
