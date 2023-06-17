```
# SQL2JSON
Converts SQL dump file to json files.

## Setup
```
pipenv install --ignore-pipfile
```

## Usage

```
> python3 sql2json.py <sqlfilepath>
```

### Options

--tables=<csv>
Comma separated list of tables to convert. Defaults to all tables in dump file

--output_dir=<path>
Path to

This should output the following JSON array:

```json
[
  {"id": "1", "name": "John", "age": "30"},
  {"id": "2", "name": "Jane", "age": "25"},
  {"id": "3", "name": "Bob", "age": "40"}
]
