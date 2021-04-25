# SQLizer.io Client for Python

A client library for the [SQLizer.io](https://sqlizer.io) file conversion service. Use this library to convert CSV, JSON, Excel or XML files to SQL tables within your python projects.

## Getting Started

Install the library using pip, by typing at the command line:

```bash
pip install sqlizer-io-client
```

To use the converter, import the sqlizer module, then set your API Key value. You can find your API keys on the [SQLizer.io Account Page](https://sqlizer.io/account). Create a `sqlizer.File` object passing in the conversion parameters. Finally, call `convert()`. For example:

```python
import sqlizer

sqlizer.config.API_KEY = 'your-api-key'

with open('example.xlsx', mode='rb') as file_content:
    converter = sqlizer.File(file_content, sqlizer.DatabaseType.MySQL, sqlizer.FileType.XLSX, 'example.xlsx', 'my_table')
    converter.convert(wait=True)
```
