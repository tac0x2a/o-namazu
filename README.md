# o-namazu
![](https://github.com/tac0x2a/o-namazu/workflows/Python%20Build/badge.svg)

Oh Namazu (Catfish) in datalake

# Parameters of observing directory
Parameter should be write YAML format as `.onamazu` file. It should be placed for each directories that be observed.

## Inheritance of effects
Parameters are inherited from parent directory.

#### Example
There are 2 directories under root directory. All directries has `.onamazu` file. (i.e. there are obseved).

+ `root_dir/.onamazu`
  ```
  pattern: "*.csv"
  ```

  It effects follow:
  ```
  pattern: "*.csv"
  min_mod_interval: 1
  ...
  ```

  `min_mod_interval: 1` is one of the default values. It effects even if not write explicit.

+ `root_dir/mario/.onamazu`
  ```
  pattern: "*.json"
  ```

  It effects follow:
  ```
  pattern: "*.json"
  min_mod_interval: 1
  ...
  ```

  `pattern` is overwritten.


+ `root_dir/luigi/.onamazu`
  ```
  min_mod_interval: 10
  ```

  It effects follow:
  ```
  pattern: "*.csv"
  min_mod_interval: 10
  ...
  ```
  `min_mod_interval` is overwritten. But `pattern` is same value of parent directory because it's not overwrtten in current directory.



## Parameters
### `pattern` `[String]`
Pattern of filename. It should be arong unix shell file pattern. Please see [fnmatch document](https://docs.python.org/3/library/fnmatch.html)

### `min_mod_interval` `[Numeric]`
Minimum modification interval [sec].
Modified events will be ignored if it inside of between previous modified and after `min_mod_interval` seconds.

Default value is 1. It means all events will be ignored in term of 1 second since last modified.
