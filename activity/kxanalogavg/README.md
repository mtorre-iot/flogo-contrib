---
title: kxanalogavg
weight: 4615
---

# Log
This activity allows you to calculate the analog averages of specific tagnames.

## Installation
### Flogo Web
This activity is part of the knox system
### Flogo CLI
```bash
flogo add activity github.com/mtorre-iot/flogo-contrib/activity/kxanalogavg
```

## Schema
Inputs and Outputs:

```json
{
"input":[
    {
      "name": "TSDB",
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
      "name": "outputTags",
      "type": "params",
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