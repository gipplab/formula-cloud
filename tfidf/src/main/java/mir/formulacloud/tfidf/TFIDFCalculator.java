package mir.formulacloud.tfidf;

import com.beust.jcommander.JCommander;
import mir.formulacloud.beans.Document;
import mir.formulacloud.beans.MathElement;
import mir.formulacloud.util.TFIDFConfig;
import mir.formulacloud.util.XQueryLoader;
import org.apache.commons.io.FilenameUtils;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.basex.query.func.math.MathE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Andre Greiner-Petter
 */
public class TFIDFCalculator {

    private static final Logger LOG = LogManager.getLogger(TFIDFCalculator.class.getName());

    private final TFIDFConfig config;
    private ExecutionEnvironment environment;

    public static int NUM_OF_FILES = 0;
    public static int PROCESSED = 0;

    public TFIDFCalculator (TFIDFConfig config) {
        this.config = config;
    }

    public LinkedList<Path> retrieveFiles() throws Exception {
        LinkedList<Path> files = new LinkedList<>();

        Files
                .walk(Paths.get(config.getDataset()))
                .sequential()
                .filter( Files::isRegularFile )
                .forEach( files::add );

        NUM_OF_FILES = files.size();

        return files;
    }

    /**
     * Builds the execution plan for Flink.
     */
    public void execute(LinkedList<Path> set) throws Exception {

        ForkJoinPool outerPool = new ForkJoinPool(config.getParallelism());
        ForkJoinPool writingPool = new ForkJoinPool(config.getNumOfOutputFiles());

        BlockingQueue<MathElement> writingQueue = new LinkedBlockingQueue<>();
        Path outputBase = Paths.get(config.getOutputF());

        if ( Files.notExists(outputBase) ){
            LOG.error("Output directory does not exist.");
            return;
        }

        LOG.info("Start writer threads.");
        for ( int i = 1; i <= config.getNumOfOutputFiles(); i++ ){
            Path outF = outputBase.resolve(i+"");
            Writer writer = new Writer(outF, writingQueue);
            writingPool.submit( writer );
        }

        LOG.info("Start processing...");
        outerPool.submit(
                () -> {
                    set.parallelStream()
                            .flatMap( path -> {
                                LinkedList<MathElement> elements = new LinkedList<>();
                                LOG.info("Load file " + path.toString());
                                Document doc = Document.parseDocument(path);

                                LinkedList<String> expressions = doc.getExpressions();
                                LinkedList<Short> freqs = doc.getTermFrequencies();
                                LinkedList<Short> depths = doc.getDepths();
                                int counter = 0;

                                while ( !expressions.isEmpty() ){
                                    MathElement entry = new MathElement(
                                            expressions.pop(),
                                            depths.pop(),
                                            (int)freqs.pop(),
                                            1
                                    );

                                    elements.add(entry);
                                    counter++;
                                }

                                LOG.info("Successfully extracted " + counter + " lines from " + path.toString());

                                TFIDFCalculator.PROCESSED++;
                                TFIDFCalculator.update();
                                return elements.stream();
                            })
                            .collect(
                                    Collectors.groupingBy(
                                        MathElement::getExpression,
                                        Collectors.reducing( MathElement::add )
                                    )
                            ).values().forEach( opt -> {
                                if ( opt.isPresent() ){
                                    writingQueue.add(opt.get());
                                    update(writingQueue.size());
                                }
                            });
                }
        );

        outerPool.shutdown();
        outerPool.awaitTermination(42, TimeUnit.HOURS);

        LOG.info("Finished filling up writing queues. Inform writers.");
        for ( int i = 1; i <= config.getNumOfOutputFiles(); i++ ){
            MathElement stopper = new MathElement();
            stopper.markAsStopper();
            writingQueue.add(stopper);
        }

        LOG.info("Await termination of writing process.");
        writingPool.shutdown();
        writingPool.awaitTermination(2, TimeUnit.HOURS);


//        Configuration flinkConfig = GlobalConfiguration.loadConfiguration("conf");
//        environment = ExecutionEnvironment.createLocalEnvironment(flinkConfig);
//
//        DataSet<Path> source = environment.fromCollection(set);
//
//        source
//                .flatMap( new BaseXRequestMapper() )
//                .groupBy(Document.IDX_EXPR-1) // Expression
//                .sum(Document.IDX_FREQ-1)   // TF
//                .andSum(3)                    // DF
//                .setParallelism(config.getParallelism())
//                .writeAsCsv(
//                        config.getOutputF(),
//                        "\n",
//                        ";",
//                        FileSystem.WriteMode.OVERWRITE
//                )
//                .setParallelism(config.getNumOfOutputFiles());
//
//        LOG.info("Done planning Flink schedule.");
    }

//    public void execute() throws Exception {
//        LOG.info("Done! Trigger Flink execution. Write files to specified output.");
//        environment.execute();
//    }

    private static long startTimer = -1;
    private static int oldLength = -1;

    public static void update() {
        update(-1, false);
    }

    public static void update(int length) {
        update(length, false);
    }

    public static void updateMerger() {
        update(-1, true);
    }

    private static int mergeCounter = 0;
    private static final String[] symbs = new String[]{ "|", "/", "-", "\\" };

    public static void update(int queueLength, boolean merger){
        if ( startTimer < 0 ) startTimer = System.currentTimeMillis();

        double perc = (double)PROCESSED/NUM_OF_FILES;
        int n = (int)(perc*50);

        long timeSpan = System.currentTimeMillis() - startTimer; // time span until now
        long estimatedRestTime = (long)(timeSpan/perc) - timeSpan;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++){
            sb.append("=");
        }
        if (n < 50) sb.append(">");

        if ( queueLength < 0 ) {
            queueLength = oldLength;
        } else {
            oldLength = queueLength;
        }

        String i;
        if ( merger ) {
            mergeCounter = (mergeCounter+1) % 4;
            i = String.format(
                    "\rFinished %05.2f%% [%-50s] %06d/%06d - Estimated Rest Time: %02d:%02d:%02d  [QL: %06d] - MERGING ( %s )",
                    perc*100,
                    sb.toString(),
                    PROCESSED,
                    NUM_OF_FILES,
                    TimeUnit.MILLISECONDS.toHours(estimatedRestTime),
                    TimeUnit.MILLISECONDS.toMinutes(estimatedRestTime) % 60,
                    TimeUnit.MILLISECONDS.toSeconds(estimatedRestTime) % 60,
                    queueLength,
                    symbs[mergeCounter]
            );
        } else {
            i = String.format(
                    "\rFinished %05.2f%% [%-50s] %06d/%06d - Estimated Rest Time: %02d:%02d:%02d  [QL: %06d]                 ",
                    perc*100,
                    sb.toString(),
                    PROCESSED,
                    NUM_OF_FILES,
                    TimeUnit.MILLISECONDS.toHours(estimatedRestTime),
                    TimeUnit.MILLISECONDS.toMinutes(estimatedRestTime) % 60,
                    TimeUnit.MILLISECONDS.toSeconds(estimatedRestTime) % 60,
                    queueLength
            );
        }

        System.out.print(i);
    }

    public static void main(String[] args) throws Exception {
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

        // create and init calculator
        TFIDFCalculator calculator = new TFIDFCalculator(config);
        LinkedList<Path> fileList = calculator.retrieveFiles();
        calculator.execute(fileList);
//        if ( !config.getOutputF().isEmpty() )
//            calculator.execute();

//        BaseXController.closeAllClients();
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
