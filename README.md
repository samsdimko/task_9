# Alert project task
This is a project for intern alert system. For current version alert messages are sent in terminal.

## Architecture
There are 4 main classes that implement behaviour of alerting.
* ```Pattern``` is an abstract class that implements a base behaviour to proceed 
data for alerting. By default, it has such arguments:
  * ```window_size``` that controls the time window for searching in log file;
  * ```message``` for alert;
  * ```use_cols``` that determines using columns in matching for better performance.

  ```Pattern``` should be inherited to implement specific behaviour.
* ```PatternAggregator``` orchestrates all pattern to use them all efficiently in single data load.
* ```DataLoader``` is a generator that implements loading data from log files in chunks.
* ```Executor``` orchestrates DataLoader and PatternAggregator.

Also, there is a ```DataPreparer``` class that implements custom merge sort algorithm 
that prepares data for moving window by sorting raw data by date.

## Usage:

### Start
To start project write ```python main.py [sorce_directory_name]``` 
in terminal in project directory. 
The command starts an executor that sensors 
the source directory for log .csv files and creates alert messages. 

### Implementation of custom search patterns
To implement your own search pattern you need to inherit 
your own class in ```patterns.by``` from abstract ```Pattern``` class
and implement ```match``` method in it.

By default, parent ```Pattern``` class implements 
```window_size```, ```message```, ```use_cols``` parameters.

To use arguments you need to add your class in ```config.json``` 
or set your default values in method ```__init__``` (not recommended).
To use multiply instances of single class with different arguments 
use changing ```config.json``` way
and add values as list of dicts for your class name.
