# TODO List
* productionalization
    * ~~add trace logging to all APIs~~
    * add rate limiting to all APIs
    * add metrics to all APIs
    * add documentation to all APIs
    * add error handling to all APIs
    * add tests to all APIs
    * create docker image
* statefulness
    * add state interface
    * add mongodb implementation of state
* agent
    * ~~add support for an agent template parameter~~
    * ~~setup agent template registry~~
        * ~~default channel definition~~
        * ~~default properties definition~~
    * ~~set override properties on agent creation~~
    * read and write properties from/to agent
    * list channels for an agent
    * list properties for an agent
    * get all running agents

* channel
    * implement faked stream
    * create channel actor
    * support posting messages to channel actor
    * support subscribing to channel actor
    * use subscription to send messages to stream
    * use subscription to send messages to AI
    * persist channel state
    * support on-demand retrieval of channel state



# Reference
