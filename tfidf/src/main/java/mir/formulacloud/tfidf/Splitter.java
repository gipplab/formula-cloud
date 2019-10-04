package mir.formulacloud.tfidf;

import com.beust.jcommander.JCommander;
import mir.formulacloud.beans.Document;
import mir.formulacloud.util.TFIDFConfig;
import mir.formulacloud.util.XQueryLoader;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.*;
import java.util.LinkedList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

/**
 * @author Andre Greiner-Petter
 */
public class Splitter {
    private static final Logger LOG = LogManager.getLogger(Splitter.class.getName());

    private final TFIDFConfig config;
    private ExecutionEnvironment environment;

    public static int NUM_OF_FILES = 0;
    public static int EMPTY_FILES = 0;
    public static int PROCESSED = 0;

    public Splitter( TFIDFConfig config ) {
        this.config = config;
    }

    public LinkedList<String> initPreExecutionPlan() throws Exception {
//        LinkedList<String> dump = new LinkedList<>();
//        dump.addFirst("1000474.xml");
//        return dump;

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

    public void startProcessing(LinkedList<String> set) {
//        Configuration flinkConfig = GlobalConfiguration.loadConfiguration("conf");
//        environment = ExecutionEnvironment.createLocalEnvironment(flinkConfig);

//        DataSet<String> source = environment.fromCollection(set);

        ForkJoinPool outerPool = new ForkJoinPool(config.getParallelism());

        Path basePath = Paths.get(config.getOutputF());

        outerPool.submit(
                () -> set
                        .stream()
                        .parallel()
                        .map( BaseXRequestMapper::getDocument )
                        .filter( Document::isNotEmpty )
                        .forEach( doc -> {
                            Path outF = basePath.resolve(doc.getDB() + "/");
                            if ( !Files.exists(outF) ){
                                LOG.info("DB folder does not exist. Create it now.");
                                try {
                                    Files.createDirectory(outF);
                                } catch (IOException e) {
                                    LOG.fatal("Cannot create directory, ", e);
                                }
                            }
                            outF = outF.resolve(doc.getFilename());
                            try {
                                LOG.info("Writing " + doc.getDB() + "/" + doc.getFilename());
                                Files.write(outF, doc.toString().getBytes(), StandardOpenOption.CREATE);
                                LOG.debug("Finished writing " + doc.getDB() + "/" + doc.getFilename());
                            } catch (IOException e) {
                                LOG.error("Cannot write file " + doc.getFilename() + " in " + doc.getDB());
                            }
                        })
        );

        outerPool.shutdown();
        try {
            outerPool.awaitTermination(42, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            LOG.fatal("Cannot stop processing...", e);
            e.printStackTrace();
        }
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

        // create and init calculator
        Splitter splitter = new Splitter(config);

        try {
            LinkedList<String> fileList = splitter.initPreExecutionPlan();
            splitter.startProcessing(fileList);
        } catch (Exception e) {
            LOG.fatal("Cannot init execution.", e);
            e.printStackTrace();
        }

        BaseXController.closeAllClients();
        long stop = System.currentTimeMillis() - start;
        LOG.info("Time Elapsed: " + stop + "ms");
        System.out.println("Done");
        System.out.println("Time Elapsed: " + stop + "ms");
    }

    public class FilterEmptyDocuments implements FilterFunction<Document> {
        @Override
        public boolean filter(Document document) {
            // false for values to be filtered out
            return !(document.isNull() || document.isEmpty());
        }
    }
}
