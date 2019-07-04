---
title: kxupdatefilter
weight: 4615
---

# Log
This activity allows you decode knox Update messagess based on specific tagnames.

## Installation
### Flogo Web
This activity is part of the knox system
### Flogo CLI
```bash
flogo add activity github.com/mtorre-iot/flogo-contrib/activity/kxupdatefilter
```

## Schema
Inputs and Outputs:

```json
{
"input":[
    {
      "name": "message",
      "type": "string",
      "value": "",
      "required": true
    },
    {
      "name": "RTDBFile",
      "type": "string",
      "value": "",
      "required": true
    },
    {
      "name": "triggerTag",
      "type": "string",
      "value": "",
      "required": true
    },
    {
      "name": "inputTags",
      "type": "params",
      "required": true
    },
    {
      "name": "functionName",
      "type": "string",
      "value": "",
      "required": true
    }

  ],
  "output": [
    {
      "name": "outputStream",
      "type": "string"
    }
  ]
}
```
## Settings
## Examples
```json
{
}
```