---
title: AMQP
weight: ????
---
# amqp
This trigger provides your flogo application the ability to start a flow via AMQP


## Installation

```bash
flogo install https://github.com/mtorre-iot/flogo-contrib/trigger/amqp
```

## Schema
Settings, Outputs and Endpoint:

```json
{
  "settings":[
    {
      "name": "requestHostName",
      "type": "string",
      "required": true
    },
    {
      "name": "requestPort",
      "type": "string",
      "required": true
    },
    {
      "name": "requestExchangeName",
      "type": "string",
      "required": true
    },
    {
      "name": "requestExchangeType",
      "type": "string",
      "required": true
    },
    {
      "name": "requestRoutingKey",
      "type": "string",
      "required": true
    },
    {
      "name": "requestUser",
      "type": "string",
      "required": true
    },    
    {
      "name": "requestPassword",
      "type": "string",
      "required": true
    },
    {
      "name": "requestDurable",
      "type": "string",
      "required": false
    },
    {
      "name": "requestAutoDelete",
      "type": "string",
      "required": false
    },
    {
      "name": "requestReliable",
      "type": "string",
      "required": false
    },
    {
      "name": "responseHostName",
      "type": "string",
      "required": false
    },
    {
      "name": "responsePort",
      "type": "string",
      "required": false
    },
    {
      "name": "responseExchangeName",
      "type": "string",
      "required": false
    },
    {
      "name": "responseExchangeType",
      "type": "string",
      "required": false
    },
    {
      "name": "responseRoutingKey",
      "type": "string",
      "required": false
    },
    {
      "name": "responseUser",
      "type": "string",
      "required": false
    },    
    {
      "name": "responsePassword",
      "type": "string",
      "required": false
    },
    {
      "name": "responseDurable",
      "type": "string",
      "required": false
    },
    {
      "name": "responseAutoDelete",
      "type": "string",
      "required": false
    },
    {
      "name": "responseReliable",
      "type": "string",
      "required": false
    }
  ],
  "output": [
    {
      "name": "message",
      "type": "string"
    }
  ],
  "reply": [
    {
      "name": "data",
      "type": "object"
    }
  ],
  "handler": {
    "settings": [
      {
        "name": "topic",
        "type": "string",
        "required": true
      }
    ]
  }
}
```

## Example Configurations

Triggers are configured via the triggers.json of your application. The following are some example configuration of the AMQP Trigger.

### Start a flow
Configure the Trigger to start "myflow". "settings" "topic" is the topic it uses to listen for incoming messages. So in this case the "endpoints" "settings" "topic" is "test_start" will start "myflow" flow. The incoming message payload has to define "replyTo" which is the the topic used to reply on.

```json
{
  "triggers": [
    {
      "name": "flogo-amqp",
      "settings": {
        "topic": "flogo/#",
        "broker": "tcp://192.168.1.12:1883",
        "id": "flogo",
        "user": "",
        "password": "",
        "store": "",
        "qos": "0",
        "cleansess": "false"
      },
      "endpoints": [
        {
          "actionType": "flow",
          "actionURI": "embedded://myflow",
          "settings": {
            "topic": "test_start"
          }
        }
      ]
    }
  ]
}
```
