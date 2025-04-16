# Mote


# TODO

DONE * update all package versions `npm outdated`
DONE * get custom-webpack installed 
DONE * fix size of popup and replace contents
DONE * get basic background service working
DONE * finish setting up browser extension https://www.justjeb.com/post/chrome-extension-with-angular-from-zero-to-a-little-hero

* debug service worker
  DONE * demonstrate fetch working to localhost
  * demonstrate XMLHttpRequest polyfill working to localhost
  * demonstrate hello world gRPC request
* agent interface
  * display popup when receiving notifications 
  * open agent interface in a tab
  * agent settings interface
    * agent URL

* agent backend
  * open connection to agent at startup
  * send page open events to agent
  * support push notifications from agent
  * support loading agent url from config somehow
  * 



# HOWTO

This project was generated with [Angular CLI](https://github.com/angular/angular-cli) version 17.3.8.

## Development server

Run `ng serve` for a dev server. Navigate to `http://localhost:4200/`. The application will automatically reload if you change any of the source files.

## Code scaffolding

Run `ng generate component component-name` to generate a new component. You can also use `ng generate directive|pipe|service|class|guard|interface|enum|module`.

##

run `npm run generate-proto` to generate protoc scaffolding for running (todo: get rid of this requirement)

## Build

Run `ng build` to build the project. The build artifacts will be stored in the `dist/` directory.

## Running unit tests

Run `ng test` to execute the unit tests via [Karma](https://karma-runner.github.io).

## Running end-to-end tests

Run `ng e2e` to execute the end-to-end tests via a platform of your choice. To use this command, you need to first add a package that implements end-to-end testing capabilities.

## Further help

To get more help on the Angular CLI use `ng help` or go check out the [Angular CLI Overview and Command Reference](https://angular.io/cli) page.
