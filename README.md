# HTRC-CountOccurrences
Given a set of terms and a set of HT volume IDs, this tool counts the number of times
these terms appear in the specified volumes, and outputs the result as CSV. The first
column of the CSV represents the volume ID, while the rest of the columns represent
the given search terms. The (x,y) entry in the table represents the number of times
term 'y' was found in volume 'x'.

# Build
* To generate a "fat" executable JAR, run:  
  `sbt assembly`  
  then look for it in `target/scala-2.11/` folder.

  *Note:* you can run the JAR via the usual: `java -jar JARFILE`

* To generate a package that can be invoked via a shell script, run:  
  `sbt stage`  
  then find the result in `target/universal/stage/` folder.
  
# Run
```
count-occurrences
HathiTrust Research Center
  -k, --keywords  <FILE>      The path to the file containing the keywords (one
                              per line) to count
  -n, --num-partitions  <N>   The number of partitions to split the input set of
                              HT IDs into, for increased parallelism
  -o, --output  <DIR>         Write the output to DIR
  -p, --pairtree  <DIR>       The path to the paitree root hierarchy to process
      --help                  Show help message
      --version               Show version of this program

 trailing arguments:
  htids (not required)   The file containing the HT IDs to be searched (if not
                         provided, will read from stdin)
```
