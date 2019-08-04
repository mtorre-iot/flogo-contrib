---
title: kxreadrtdb
weight: 4615
---

# Log
This activity allows you to get realtime data from RTDB of specific tagnames.

## Installation
### Flogo Web
This activity is part of the knox system
### Flogo CLI
```bash
flogo add activity github.com/mtorre-iot/flogo-contrib/activity/kxreadrtdb
```

## Schema
Inputs and Outputs:

```json
{
"input":[
    {
      "name": "RTDBFile",
      "type": "string",
      "value": "",
      "required": true
    }
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