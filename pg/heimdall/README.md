# Heimdall

Heimdall is a REST API gateway for the Glimmer agent service. It provides HTTP endpoints to manage agents, their properties, and channels.

# TODO List
* productionalization
    * add trace logging to all APIs
    * add OAuth2 authentication to all APIs
    * add rate limiting to all APIs
    * add metrics to all APIs
    * add documentation to all APIs
    * add error handling to all APIs
    * add tests to all APIs
    * create docker image


# Reference

## REST API

| HTTP Method | Endpoint                | Description                               |
|-------------|-------------------------|-------------------------------------------|
| POST        | /agent                 | Create a new agent                        |
| GET         | /agent/:id             | Retrieve an agent by ID                   |
| DELETE      | /agent/:id             | Delete an agent by ID                     |
| GET         | /health                | Check the health status of the service    |
| POST        | /agent/:id/property     | Set a property for an agent               |
| GET         | /agent/:id/property     | Get all properties for an agent           |
| GET         | /agent/:id/property/:key| Get a specific property for an agent      |
| DELETE      | /agent/:id/property/:key| Delete a specific property for an agent   |
| POST        | /agent/:id/start        | Start an agent                            |
| POST        | /agent/:id/pause        | Pause an agent                            |
| POST        | /agent/:id/stop         | Stop an agent                             |
| POST        | /agent/:id/channel       | Create a channel for an agent             |
| GET         | /agent/:id/channel/:name | Get a specific channel for an agent       |
| DELETE      | /agent/:id/channel/:name | Delete a specific channel for an agent    |
| GET         | /agent/:id/channel/:name/stream | Stream messages from a channel for an agent |

### Create an agent

Method: POST
Path: /agent
Parameters:
- `id`: The ID of the agent.
- `name`: The name of the agent.

Usage:
```
curl -X POST http://localhost:50053/agent \
  -H "Content-Type: application/json" \
  -d '{"id": "my-agent", "name": "My Agent", "template_uri": "std/chatbot/1.0.0", "override_parameters": {"my-param": "my-value"}}'
```

### Get an agent

Method: GET
Path: /agent/:id
Parameters:
- `id`: The ID of the agent.

Usage:
```
curl http://localhost:50053/agent/my-agent
```

### Delete an agent

Method: DELETE
Path: /agent/:id
Parameters:
- `id`: The ID of the agent.

Usage:
```
curl -X DELETE http://localhost:50053/agent/my-agent
```

### Get the health status of the service

Method: GET 
Path: /health

Usage:
```
curl http://localhost:50053/health
```

### Set a property for an agent

Method: POST
Path: /agent/:id/property
Parameters:
- `id`: The ID of the agent.
- `key`: The key of the property.
- `value`: The value of the property.

Usage:
```
curl -X POST http://localhost:50053/agent/my-agent/property \
  -H "Content-Type: application/json" \
  -d '{"key": "my-property", "value": "my-value"}'
```

### Get all properties for an agent

Method: GET
Path: /agent/:id/property
Parameters:
- `id`: The ID of the agent.

Usage:
```
curl http://localhost:50053/agent/my-agent/property
```

### Get a specific property for an agent

Method: GET
Path: /agent/:id/property/:key
Parameters:
- `id`: The ID of the agent.
- `key`: The key of the property.

Usage:
```
curl http://localhost:50053/agent/my-agent/property/my-property
```

### Delete a specific property for an agent

Method: DELETE
Path: /agent/:id/property/:key
Parameters:
- `id`: The ID of the agent.
- `key`: The key of the property.

Usage:
```
curl -X DELETE http://localhost:50053/agent/my-agent/property/my-property
``` 

### Start an agent

Method: POST
Path: /agent/:id/start
Parameters:
- `id`: The ID of the agent.

Wakes a hibernating agent.

Usage:
```
curl -X POST http://localhost:50053/agent/my-agent/start
```

### Pause an agent

Method: POST
Path: /agent/:id/pause
Parameters:
- `id`: The ID of the agent.

Sends an agent to hibernation state.

Usage:
```
curl -X POST http://localhost:50053/agent/my-agent/pause
```

### Stop an agent

Method: POST
Path: /agent/:id/stop
Parameters:
- `id`: The ID of the agent.

Stops an agent.

Usage:
```
curl -X POST http://localhost:50053/agent/my-agent/stop 
```

### Create a channel for an agent

Method: POST
Path: /agent/:id/channel
Parameters:
- `id`: The ID of the agent.
- `name`: The name of the channel.

Usage:
```
curl -X POST http://localhost:50053/agent/my-agent/channel \
  -H "Content-Type: application/json" \
  -d '{"name": "my-channel"}'
```

### Get a specific channel for an agent

Method: GET
Path: /agent/:id/channel/:name
Parameters:
- `id`: The ID of the agent.
- `name`: The name of the channel.  

Usage:
```
curl http://localhost:50053/agent/my-agent/channel/my-channel
```

### Delete a specific channel for an agent

Method: DELETE
Path: /agent/:id/channel/:name
Parameters:
- `id`: The ID of the agent.
- `name`: The name of the channel. 

Usage:
```
curl -X DELETE http://localhost:50053/agent/my-agent/channel/my-channel
```

### Stream messages from a channel for an agent

Method: GET
Path: /agent/:id/channel/:name/stream
Parameters:
- `id`: The ID of the agent.
- `name`: The name of the channel.

Usage:
```
curl http://localhost:50053/agent/my-agent/channel/my-channel/stream
```


