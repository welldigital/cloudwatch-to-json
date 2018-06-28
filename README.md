# cw/filter

Filters CloudWatch exports to just the JSON data from your logs so that they can be easily imported into other tools.

Example usage:

```
filter -data=logs > output.json
gzip output.json
```