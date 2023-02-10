

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


### TODO: Actor System

* Base Actor System
  * ~~typed messages and actors~~
  * ~~tell pattern~~
  * ~~ask pattern~~
  * behavior
    * ~~base pattern~~
    * stopped state
    * signal support
  * child pattern
    * context.spawn
    * child signals
  * pipe-to-self pattern
  * supervision pattern
  * persistence
    * persistence signals
  * clustering
    * gossip discovery / state synchronization
    * distributed data
    
* Unruly Goats
  * settable node
  * lambda node
  * meta node

### TODO

* Inference
  * Hugging Face
    * Text
      * fill-mask task
      * ~~summarization task~~
      * ~~question-answering task~~
      * table question task
      * sentence similarity task
      * text classification task
      * text generation task
      * text-to-text task
      * token classification task
      * translation task
      * zero-shot classification task
      * conversational task
      * feature extraction task
    * Image
      * image classification task
      * object detection task
      * image segmentation task
      * image generation task
    * Audio
      * ASR task
      * audio classification task
      * TTS task
  * OpenAI
    * Embedding

  * SageMaker
  * TFLite (local)
* Retrieval
  * FAISS
  * ElasticSearch
  * SCAN
* Ergonomics
  * ~~print info on where it's running~~
  * ~~structured logs / telemetry~~
  * print info on routes
* Security
  * support oauth
* Chat API
  * ~~CRUD route design~~
  * ~~CRUD route implementation~~
  * ~~State object support~~
  * ~~Create message~~
  * ~~List message~~
  * WS interface
* Store
  * ~~Fake state object: ```HashMap<Vec<_>>```~~
  * persist chats in mongodb
  * subscribe to change stream in mongodb
* Orchestration Graph
* Agent
  * design agent architecture
  * get(response)
  * get(intent)
