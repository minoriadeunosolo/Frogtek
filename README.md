# Technical Tests

## 1 Transform a poorly formatted TSV File

```sh
python3 massage_file.py -i data.tsv -o output.tsv -n 15 -verbose
```

-i | --inputfile  : The file to be reading
-o | --outputfile : Output file
-n | --num_process: Number of parallel process to used
-v | --verbose: Show information about the slices created and doesn't delete
the partial temporary files.

The main idea is to detect every field with a Regular Expression. Only if
5 consecutive fields are "well detected" a record is created.

It's a parallel algorithm. A file is cut in n pieces (initial_position, length)
and processed independently. Every process has 3 returned values:

- Initial garbage found at the beginning, before a set of consecutive well formed records.
- A file that contains the Records
- Final garbage found at the end that isn't part of a well formed record.

```mermaid
graph LR
A[Initial File] -- 1st Slice --> B((1st Process))
A -- 2nd Slice --> C((2nd Process))
A -- ... Slice --> D((nth Process))
A -- Last Slice --> E((Last Process))
B --> F{Final Combination}
C --> F
D --> F
E --> F
F --> G[Output File]

## 2 Generate core business events

```sh
python3 generate-event.py -o ./mydir -n 10000 -b 100 -i 1
```
-o | --output-directory   : The directory where the files will be created.
-n | --number-of-orders   : Number of orders to be created.
-b | --batch-size         : Batch size of events per file
-i | --interval           : Interval in seconds between each file being created


## 3 Monitor App
```sh
python3 watchdog.py -d ./mydir -n 10000 -i 2
```
-d | --directory_to_watch : Directory to be watched for new files
-i | --interval           : Interval in seconds

Uses the pyinotify library:
```sh
pip3 install pyinotify
```
