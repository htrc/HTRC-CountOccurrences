# HTRC-Tools-CountOccurrences
Given a set of terms and a set of HT volume IDs, this tool counts the number of times
these terms appear in the specified volumes, and outputs the result as CSV. The first
column of the CSV represents the volume ID, while the rest of the columns represent
the given search term counts. The (x,y) entry in the table represents the number of times
term 'y' was found in volume 'x'.

Each term is defined through a regular expression for added flexibility. The terms are
read from a file provided by the user. Each line of the file contains two columns,
separated by a tab (`\t`) character. The first column is the term whose occurrence count
will be reported in the CSV results, and the second column contains the regular expression
that defines the search criteria for the term. All matches of the regular expression in
the text are counted, and the total count assigned to the term.

# Build
* To generate an executable package that can be invoked via a shell script, run:  
  `sbt stage`  
  then find the result in `target/universal/stage/` folder.

# Run
The following command line arguments are available:
```
count-occurrences
HathiTrust Research Center
  -k, --keywords  <FILE>      The path to the file containing the keyword
                              patterns (one per line) to count
  -l, --log-level  <LEVEL>    The application log level; one of INFO, DEBUG, OFF
  -n, --num-partitions  <N>   The number of partitions to split the input set of
                              HT IDs into, for increased parallelism
  -o, --output  <DIR>         Write the output to DIR
  -p, --pairtree  <DIR>       The path to the paitree root hierarchy to process
      --spark-log  <FILE>     Where to write logging output from Spark to
      --help                  Show help message
      --version               Show version of this program

 trailing arguments:
  htids (not required)   The file containing the HT IDs to be searched (if not
                         provided, will read from stdin)
```
