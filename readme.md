# SQL2JSON
Converts SQL dump file to json files. Generates one json file per table.

Example SQL:
```sql
INSERT INTO `articles` (`id`, `timestamp`, `content`, `status`) VALUES (100, 21321312, `This is article content`, `published`);
```

JSON Output (articles.json):
```json
[{"id": 100, "timestamp": 21321312, "content": "This is article content", "status": "published"}]
```

## Setup
Install pip dependencies
```
# pipenv install --ignore-pipfile
```

## Usage

```
# python3 sql2json.py <sqlfilepath>
```

### Options
Command line options
```
--tables=<csv>
Comma separated list of tables to convert. Can be literal table names or regular expression patterns. Defaults to all tables in dump file
```
```
--output_dir=<path>
Output target directory (json files go here)
```
```
--config=<jsonfilepath>
Json configuration file path.
```
```
--flush_batch_size=<number>
Number of records to queue before writing to file
```
### Advanced Configuration
The JSON configuration file supports all of the options listed above in addition to some additional options that cannot be expressed through the command line.

Example:
```json
{
  "output_dir": "./json",
  "flush_batch_size": 50000,
  "tables": ["example_table", "example_\\d+"],
  "parsers": {
    "example_table": {
      "meta_value": ["json", "phps"]
    },
    "example_\\d+": {
      "data": ["json"]
    }
  }
}
```

### Column value parsers
SQL2JSON supports parsing JSON and PHP serialized data which is converted to nested json. Enable this behavior by defining the "parsers" dictionary in the config json file.

Supported parsers: json, phps

```json
{
  "parsers": {
    "<table_name or table_regex>": {
      "<column_name>": ["json", "phps"]
    }
  }
}
```


This should output the following JSON array:

```json
[
  {"id": "1", "name": "John", "age": "30"},
  {"id": "2", "name": "Jane", "age": "25"},
  {"id": "3", "name": "Bob", "age": "40"}
]
