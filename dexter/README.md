

## APIS

### Tasks

* POST   /task - create a new task
* GET    /task - list available tasks
* GET    /task/{task}
* DELETE /task/{task}
* POST   /task/{task}/subtask
* GET    /task/{task}/subtask
* POST   /task/{task}/subtask/__take - take an assignment for a subtask
* GET    /task/{task}/subtask/{subtask} - get info about a subtask
* PUT    /task/{task}/subtask/{subtask} - set info about a subtask
* PUT    /task/{task}/subtask/{subtask}/status - set update/completion status for a subtask



### CHAT

* POST   /chat - create new session
* GET    /chat - list sessions   
* GET    /chat/{session} - get a session (e.g. list of messages)
* DELETE /chat/{session} - end a session
* POST   /chat/{session}/message
* GET    /chat/{session}/message/{message}
* DELETE /chat/{session}/message/{message}

  * POST /chat 
  * GET  /chat        - list sessions
  * GET  /chat/{session} - list messages



### TODO

* Ergonomics
  * ~~print info on where it's running~~
  * ~~structured logs / telemetry~~
  * print info on routes
* Security
  * support oauth
* Chat API
  * ~~CRUD routes~~
  * WS interface
