package mir.formulacloud.tfidf;

import com.beust.jcommander.JCommander;
import mir.formulacloud.beans.MathElement;
import mir.formulacloud.util.TFIDFConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @author Andre Greiner-Petter
 */
public class TFIDFMerger {
    private static final Logger LOG = LogManager.getLogger(TFIDFMerger.class.getName());

    private TFIDFConfig config;
    private volatile HashMap<String, MathElement> memory;

    private TFIDFMerger(TFIDFConfig config) {
        this.config = config;
        memory = new HashMap<>();

        if ( Files.notExists(Paths.get(config.getOutputF())) ){
            LOG.error("Output directory does not exist.");
            return;
        }
    }

    public void merge(Path... files) {
        if ( files.length < 2 ){
            LOG.info("Nothing todo here... you only wanted to merge one or less files.");
        }

        LOG.info("Loading reference to cache.");
        // get reference
        Path ref = files[0];
        try (Stream<String> lines = Files.lines(ref)) {
            loadToCache(lines).forEach( e -> memory.put( e.getExpression(), e ));
            double heapSize = Runtime.getRuntime().totalMemory()/Math.pow(1024,2);
            LOG.info("Loaded TF-IDF math elements from " + ref.toString() + " [#" + memory.size() + "; Mem: "+heapSize+" MB]");
        } catch ( IOException ioe ) {
            LOG.fatal("Cannot read TF-IDF file " + ref);
            return;
        }

        LOG.info("Reference loaded. Start merging.");
        for ( int i = 1; i < files.length; i++ ) {
            Path m = files[i];
            LOG.info("Loading " + m.toString());
            try ( Stream<String> lines = Files.lines(m) ) {
                loadToCache(lines)
                        .forEach( element -> {
                            MathElement refEl = memory.get(element.getExpression());
                            if ( refEl == null ) { // new element, just grow memory
                                memory.put(element.getExpression(), element);
                            } else { // otherwise, update element
                                refEl.add(element);
                            }
                        });
                double heapSize = Runtime.getRuntime().totalMemory()/Math.pow(1024,2);
                LOG.info("Merged TF-IDF math elements from " + m.toString() + " [#" + memory.size() + "; Mem: "+heapSize+" MB]");
            } catch (IOException ioe) {
                LOG.fatal("Cannot read merging files " + m + "; Continue with other files.");
            }
        }

        LOG.info("Done, succesffully merged all files into one mapping.");
    }

    public void storeMemory() throws IOException {
        Path outputBase = Paths.get(config.getOutputF());
        ForkJoinPool writerPool = new ForkJoinPool(config.getNumOfOutputFiles());
        BlockingQueue<MathElement> writingQueue = new LinkedBlockingQueue<>();

        LOG.info("Kick-off Writers.");
        for ( int i = 1; i <= config.getNumOfOutputFiles(); i++ ){
            Path outF = outputBase.resolve(i+"");
            Writer writer = new Writer(outF, writingQueue);
            writerPool.submit( writer );
        }

        memory.values()
                .parallelStream()
                .forEach( writingQueue::add );

        for ( int i = 1; i <= config.getNumOfOutputFiles(); i++ ){
            MathElement stopper = new MathElement();
            stopper.markAsStopper();
            writingQueue.add(stopper);
        }

        LOG.info("Await writers finish writing.");
        writerPool.shutdown();
        try {
            writerPool.awaitTermination(8, TimeUnit.HOURS);
            LOG.info("Done, finished all successfully.");
        } catch (InterruptedException e) {
            LOG.fatal("Interrupted writing process.", e);
        }
    }

    private Stream<MathElement> loadToCache(Stream<String> lines) {
        return lines.map( l -> l.split(";(?=(\"[^\"]*\")*[^\"]*$)") )
                .map( TFIDFMerger::stripParentheses )
                .map( arr -> new MathElement(
                        arr[0],
                        Short.parseShort(arr[1]),
                        Integer.parseInt(arr[2]),
                        Integer.parseInt(arr[3])
                ));
    }

    private static String[] stripParentheses(String[] a){
        for(int i = 0; i<a.length; i++)
            if(a[i].startsWith("\""))
                a[i] = a[i].substring(1,a[i].length()-1);
        return a;
    }

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        TFIDFConfig config = new TFIDFConfig();

        // parse config
        JCommander jcommander = JCommander
                .newBuilder()
                .addObject(config)
                .build();

        jcommander.parse(args);

        if (config.isHelp()){
            jcommander.usage();
            return;
        }

        ArrayList<Path> files = new ArrayList<>();

        Files
                .walk(Paths.get(config.getDataset()))
                .sequential()
                .filter( Files::isRegularFile )
                .forEach( files::add );

        Path[] paths = new Path[files.size()];
        paths = files.toArray(paths);

        TFIDFMerger merger = new TFIDFMerger(config);
        merger.merge(paths);
        merger.storeMemory();

        long stop = System.currentTimeMillis() - start;
        LOG.info("Time Elapsed: " + stop + "ms");
        System.out.println();
        System.out.println("Done");

        String format = String.format("%02d:%02d",
                TimeUnit.MILLISECONDS.toMinutes(stop),
                TimeUnit.MILLISECONDS.toSeconds(stop)%60
        );
        System.out.println("Time Elapsed: " + format);
    }
}
