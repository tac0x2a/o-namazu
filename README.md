# o-namazu
![](https://github.com/tac0x2a/o-namazu/workflows/Python%20Build/badge.svg)

Oh Namazu (Catfish) in datalake.

# What is o-namazu ?
o-namazu is data collector that traverse specified directories.
You can be target of traverse just place `.onamazu` file.

### Supported format and protocol
+ `csv` and multi-line `text`.
+ Send via `mqtt` protocol.

Please see `mqtt: Dict` parameter.


# Setup
```
pip install -r requirements.txt
```

if you faced `No module named '_bz2'` error, please re install python environment.
```
sudo apt-get install liblzma-dev libbz2-dev
pyenv install 3.7.3 # your python version
 ```


# Parameters
Parameter should be write YAML format as `.onamazu` file. It should be placed for each directories that be observed.

## `pattern: String`
Pattern of filename. It should be arong unix shell file pattern. Please see [fnmatch document](https://docs.python.org/3/library/fnmatch.html)

## `min_mod_interval: Numeric`
Minimum modification interval [sec].
Modified events will be ignored if it inside of between previous modified and after `min_mod_interval` seconds.

Default value is 1. It means all events will be ignored in term of 1 second since last modified.


## `callback_delay: Numeric`
Delay of callback from last modification detect [sec]
Often, modification events are received several times in continuous writing the file.
The event will be ignored that is received inside of between previous modified and after `callback_delay` seconds.
After "callback_delay" seconds from received last modification event, the callback is ececution.


## `db_file: String`
File name of status file of the directory.
It contains current read position,last time of read, and so on.

In default, db_file contains following.
+ `watching: Dict` is map of file name to status of the file. The status contains following in default.
  +  `last_modified: Numeric` is time of last modified the file as epoch time.


## `ttl: Numeric`
Time to archive the file [sec]
When expired ttl seconds since last detected at by o-namazu, the file will be moved into archive directory.
o-namazu will traverse directories each minutes to judge the file should be archived or not.
If the value is -1, the file is never archive. (Default)

## `archive: Dict`
Destination of ttl expired files [Dict]

### `type: String`
Archive action type be applied to the file that expired ttl. `type` have to be `directory`, `zip` or `delete`.
+ `directory`: move the file into directory.
+ `zip`: compress the file into zip file.
+ `delete`: delete the file.

### `name: String`
`name` is name of directory or zip as the destination. This is ignored when use "delete" type


## `mqtt: Dict`
If this parameter is defined, o-namazu try to read as ascii data, and sent to MQTT Broker.
when put a file into directory, o-namazu read all data and will send. If some rows append to the file, o-namazu will send appended rows only.

`mqtt` will write last read position at db_file as `read_completed_pos: Numeric` into each file entry under `watching` dict.

**Example**
```yaml
mqtt:
  host: localhost
  port: 1883
  topic: csv/sample
  format: csv
```

### `host: String`
MQTT Broker host or IP address.

### `port: Numeric`
MQTT Broker port.

### `topic: String`
Topic of published mqtt message.

### `format: String`
The file format `csv` or `text`.
If use `csv`, when some rows append to the file, o-namazu will send header and appended rows only. When use `text`, just will send appended lines.
Default value is `text`.


# Parameter inheritance of effects on observing directory
Parameters are inherited from parent directory.

#### Example
There are 2 directories under root directory. All directries has `.onamazu` file. (i.e. there are obseved).

+ `root_dir/.onamazu`
  ```yaml
  pattern: "*.csv"
  ```

  It effects follow:
  ```yaml
  pattern: "*.csv"
  min_mod_interval: 1
  ...
  ```

  `min_mod_interval: 1` is one of the default values. It effects even if not write explicit.

+ `root_dir/mario/.onamazu`
  ```yaml
  pattern: "*.json"
  ```

  It effects follow:
  ```yaml
  pattern: "*.json"
  min_mod_interval: 1
  ...
  ```

  `pattern` is overwritten.


+ `root_dir/luigi/.onamazu`
  ```yaml
  min_mod_interval: 10
  ```

  It effects follow:
  ```yaml
  pattern: "*.csv"
  min_mod_interval: 10
  ...
  ```
  `min_mod_interval` is overwritten. But `pattern` is same value of parent directory because it's not overwrtten in current directory.
