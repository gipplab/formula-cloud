package mir.formulacloud.tfidf;

import com.beust.jcommander.JCommander;
import mir.formulacloud.beans.MathElement;
import mir.formulacloud.util.TFIDFConfig;
import mir.formulacloud.util.XQueryLoader;
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

import java.util.LinkedList;

/**
 * @author Andre Greiner-Petter
 */
public class TFIDFCalculator {

    private static final Logger LOG = LogManager.getLogger(TFIDFCalculator.class.getName());

    private final TFIDFConfig config;
    private ExecutionEnvironment environment;

    public static int NUM_OF_FILES = 0;
    public static int EMPTY_FILES = 0;
    public static int PROCESSED = 0;

    public TFIDFCalculator (TFIDFConfig config) {
        this.config = config;
    }

    public LinkedList<String> initPreExecutionPlan() throws Exception {
        XQueryLoader.initMinTermFrequency(config.getMinTermFrequency());
        DatastructureAnalyzer dataAnalyzer = new DatastructureAnalyzer(config);

        try {
            NUM_OF_FILES = dataAnalyzer.init();
            return dataAnalyzer.getEvenProcessingOrder();
        } catch (Exception e){
            BaseXController.closeAllClients();
            throw e;
        }
    }

    /**
     * Builds the execution plan for Flink.
     */
    public void initExecutionPlan(LinkedList<String> set) throws Exception {
        Configuration flinkConfig = GlobalConfiguration.loadConfiguration("conf");

        environment = ExecutionEnvironment.createLocalEnvironment(flinkConfig);

        DataSet<String> source = environment.fromCollection(set);

        // Tuple4: ExpressionString, Depth, Term Frequency, Document Frequency
        DataSet<Tuple4<String, Short, Integer, Integer>> mathElements = source
                .flatMap(new BaseXRequestMapper())
                .groupBy(0) // group on strings
                .sum(2)     // sum up TF
                .andSum(3); // sum up DF

//        DataSet<MathElement> mathElements = source
//                .flatMap(new BaseXRequestMapper())
//                .groupBy("expression")
//                .reduce(new MathElementMerger())
//                .setParallelism(config.getParallelism()*4);

//                .setCombineHint(ReduceOperatorBase.CombineHint.HASH)
//                .setParallelism(config.getParallelism()*4);

        if ( !config.getOutputF().isEmpty() ){
            mathElements
                    .writeAsCsv(
                            config.getOutputF(),
                            "\n",
                            ";",
                            FileSystem.WriteMode.OVERWRITE
                    )
                    .setParallelism(config.getNumOfOutputFiles());
        }
        else {
            LOG.info("Done! Trigger Flink execution. Write the output directly to console.");
            mathElements.print();
        }
    }

    public void execute() throws Exception {
        LOG.info("Done! Trigger Flink execution. Write files to specified output.");
        environment.execute();
    }

    public static void update(){
        double perc = (double)PROCESSED/NUM_OF_FILES;
        int n = (int)(perc*50);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++){
            sb.append("=");
        }
        if (n < 50) sb.append(">");

        String i = String.format(
                "\rFinished %05.2f%% [%-50s] %06d/%06d [empty: %d, BXC: %d, BXS: %d]",
                perc*100,
                sb.toString(),
                PROCESSED,
                NUM_OF_FILES,
                EMPTY_FILES,
                BaseXController.CLIENT_COUNTER,
                BaseXController.SERVER_COUNTER
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
        LinkedList<String> fileList = calculator.initPreExecutionPlan();
        calculator.initExecutionPlan(fileList);
        if ( !config.getOutputF().isEmpty() )
            calculator.execute();

        BaseXController.closeAllClients();
        long stop = System.currentTimeMillis() - start;
        LOG.info("Time Elapsed: " + stop + "ms");
        System.out.println("Done");
        System.out.println("Time Elapsed: " + stop + "ms");
    }
}
