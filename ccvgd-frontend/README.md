# CCVGproject

This project was generated with [Angular CLI](https://github.com/angular/angular-cli) version 11.0.5.

## Production server

This application is a web front end for the [Flask backend API](https://github.com/ulsdevteam/ccvgd-backend/), which interfaces to the [CCVGD Database](https://github.com/ulsdevteam/ccvgd-database/)

To build the production server, first [build the database via the instructions](https://github.com/ulsdevteam/ccvgd-database/tree/master/pythonScript)

Then, [deploy the Flask backend API](https://github.com/ulsdevteam/ccvgd-backend/) as a daemon.

Configure [src/environments/environment.prod.ts](https://github.com/ulsdevteam/ccvgd-frontend/blob/frontend_master/src/environments/environment.prod.ts) to point to the Flask backend API, and then build the Angular application with `ng build --prod`. Deploy the resulting build from dist/CCVGproject to your public application root.

Since this application expects to be deployed in the webroot, which will reference a path, e,g: [http://localhost/ccvgd/]

### Angular use of `base-href` and `deploy-url` for building options :

if using `/ccvgd/` as the application base for the router, do this command line with 
```
ng build --prod --base-href /ccvgd/
```
go to `dist/CCVGproject/index.html` on line #6 will updated with the webroot: `<base href="/ccvgd/">`

for updating base of assets:

```
ng build -prod --base-href <href value for router> --deploy-url <href value for assets>
```

## Development server

### Prereqs

- Install Angular Packages:

```
npm install
```

- Run Server:

```
ng serve
```

- Navigate to:

```
http://localhost:4200/
```

Run `ng serve` for a dev server. Navigate to `http://localhost:4200/`. The app will automatically reload if you change any of the source files.

## Code scaffolding

Run `ng generate component component-name` to generate a new component. You can also use `ng generate directive|pipe|service|class|guard|interface|enum|module`.

## Build

Run `ng build` to build the project. The build artifacts will be stored in the `dist/` directory. Use the `--prod` flag for a production build.

## Running unit tests

Run `ng test` to execute the unit tests via [Karma](https://karma-runner.github.io).

## Running end-to-end tests

Run `ng e2e` to execute the end-to-end tests via [Protractor](http://www.protractortest.org/).

## Further help

To get more help on the Angular CLI use `ng help` or go check out the [Angular CLI Overview and Command Reference](https://angular.io/cli) page.
