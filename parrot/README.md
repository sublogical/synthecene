## HOWTO

### Setup CLASP
```
sudo npm install -g @google/clasp
clasp login
```

## Design Questions
### How much unique code really needs to exist for AppScript/Google?

### What other authoring environments should we target?
* Google Sheets
* Google Slides
* VSCode



## TODO

* Setup Project
  * setup project with https://github.com/danthareja/node-google-apps-script
  * download existing files from https://script.google.com/u/0/home/projects/1jkZ_0XNaK-p9au0gDd6bjUzK1VQhuHJbBoz86kRKF0UI74yW5DTzrvGt/settings

* Setup Environment
  * Publicly expose ports to here
  * Route ports to my linux box
  * Open ports on my linux box
  * Demonstrate ability to call APIs on Dexter

* CX
  * Get Vue and Vuetify working: https://ramblings.mcpher.com/vuejs-apps-script-add-ons/
  * ~~Get selected text~~
  * ~~Trigger actions when selected text changes~~
  * Sidebar
    * ~~Task action by button~~
    * ~~Display preview~~
    * ~~Insert at cursor~~
    * Display indication of work occurring
    * Enable/Disable actions based on selection state
    * Report user edits to candidate
    * Support copying to clipboard
    * Report inserts, copy
  * Context Menu CX
    * PoC ability to add items to context menu
    * Copy Summary
    * Insert Summary
  * Chat CX
    * PoC ability to share chat CX with Pilot
    * Add suggested actions bubbles in Chat CX
  * Related Topics CX
* AI Actions
  * Suggest possible actions
  * ~~Summarize/Simplify~~
  * Rephrase
  * Find Relevant Topics
  * Save Highlight
  * Research Pros & Cons
  * Summarize Link
  * Add definition for term

* Information
  * Save Highlights