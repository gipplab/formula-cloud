# NII Setup

After many failures the following setup worked for me on NII 15 (RAM 1TB, 128 Cores)

### Startup:
Please use the following options to start `tfidf-calculator.jar`:
* `-Xmx500g` take care of the RAM usage. You should take at least 100GB
* `-in` the main folder of the harvest files
* `-out` define the output folder
* `--threads` set the number of threads to the number of databases in basex
* `-minTF` minimum term frequency per document. You should avoid set this value to 1. Arxiv (warning+no-problem) with `-minTF 2` generated over 11 million distinguished math formulae.
* `-defCli` set the number of BaseXClients per BaseXServer (1 is recommended)
* `-numOutF` set the number of output files. You can also set it to 1, but this file might be multiple GB large which might be difficult to handle later on.

```
andreg-p@csisv15:~/formulacloud/arxiv$ java -Xmx500g -jar tfidf-calculator.jar -in /home/andreg-p/arxmliv/math-basex-arxiv/ -out /home/andreg-p/arxmliv/math-stats/tfidf/ --threads 24 -minTF 2 -defCli 1 -numOutF 16
...
Finished 100.00% [=================================================>] 841006/841008 [empty: 11948, BXC: 24, BXS: 24]
...
Time Elapsed: 27919835ms
```

### conf/flink-conf.yaml

```yaml
# The heap size for the JobManager JVM
jobmanager.heap.size: 4096m
slot.idle.timeout: 60000
slot.request.timeout: 900000

# The heap size for the TaskManager JVM
taskmanager.heap.size: 4096m
task.cancellation.interval: 120000
task.cancellation.timeout: 320000
task.cancellation.timers.timeout: 15000

# Configure network buffers. See:
# https://ci.apache.org/projects/flink/flink-docs-stable/ops/config.html#jobmanager
# It might be necessary to just increase the number of network buffers
# be careful increasing the min over 800mb -> increase the max instead
taskmanager.network.memory.fraction: 0.15
taskmanager.network.memory.min: 128mb
taskmanager.network.memory.max: 4gb
# the next value strongly depends on the size of your RAM and how you start the program. In my case I used -Xmx500g and a fraction of 0.3. The default fraction of 0.7 was way to big to pre allocate. In the end the program needs about 100GB of RAM
taskmanager.memory.fraction: 0.3
taskmanager.memory.segment-size: 256k
taskmanager.memory.preallocate: true

akka.watch.heartbeat.interval: 800 s
akka.watch.heartbeat.pause: 1200 s
```

### .basex
Note that you have to change your basex datapath
```
# General Options
DBPATH = /opt/basex/data/
LOGPATH = /opt/arxmliv/logs/basex/

# Local Options
ADDCACHE = true
SKIPCORRUPT = false
STRIPNS = true
INTPARSE = true
ATTRINCLUDE = data-set,data-doc-id,data-major-collection,data-minor-collection,data-finer-collection,url
```

### Useful comments
Counting all lines in all files (make sure parallel is installed):
```bash
find . -name "*" | parallel 'wc -l {}' 2>/dev/null | awk '{print $1}' | paste -sd+ - | bc
```
