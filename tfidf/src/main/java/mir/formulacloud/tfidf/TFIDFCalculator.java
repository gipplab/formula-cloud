package mir.formulacloud.tfidf;

import com.beust.jcommander.JCommander;
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

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

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
    public void initExecutionPlan(LinkedList<Path> set) throws Exception {
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration("conf");

        environment = ExecutionEnvironment.createLocalEnvironment(flinkConfig);

        DataSet<Path> source = environment.fromCollection(set);

        source
                .flatMap( new BaseXRequestMapper() )
                .groupBy(0) // Expression
                .sum(1)     // TF
                .andSum(3)
                .setParallelism(config.getParallelism())
                .writeAsCsv(
                        config.getOutputF(),
                        "\n",
                        ";",
                        FileSystem.WriteMode.OVERWRITE
                )
                .setParallelism(config.getNumOfOutputFiles());

        LOG.info("Done planning Flink schedule.");
    }

    public void execute() throws Exception {
        LOG.info("Done! Trigger Flink execution. Write files to specified output.");
        environment.execute();
    }

    private static long startTimer = -1;

    public static void update(){
        if ( startTimer < 0 ) startTimer = System.currentTimeMillis();

        double perc = (double)PROCESSED/NUM_OF_FILES;
        int n = (int)(perc*50);

        long timeSpan = System.currentTimeMillis() - startTimer; // time span until now
        long estimatedRestTime = timeSpan - (long)(timeSpan/perc);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++){
            sb.append("=");
        }
        if (n < 50) sb.append(">");

        String i = String.format(
                "\rFinished %05.2f%% [%-50s] %06d/%06d - Estimated Rest Time: %02d:%02d:%02d",
                perc*100,
                sb.toString(),
                PROCESSED,
                NUM_OF_FILES,
                TimeUnit.MILLISECONDS.toHours(estimatedRestTime),
                TimeUnit.MILLISECONDS.toMinutes(estimatedRestTime) % 60,
                TimeUnit.MILLISECONDS.toSeconds(estimatedRestTime) % 60
        );
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
        calculator.initExecutionPlan(fileList);
        if ( !config.getOutputF().isEmpty() )
            calculator.execute();

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
