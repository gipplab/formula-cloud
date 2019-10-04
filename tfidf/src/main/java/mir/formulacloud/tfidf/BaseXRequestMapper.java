package mir.formulacloud.tfidf;

import com.formulasearchengine.mathosphere.basex.BaseXClient;
import mir.formulacloud.beans.Document;
import mir.formulacloud.util.Constants;
import mir.formulacloud.util.XQueryLoader;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.regex.Matcher;

/**
 * @author Andre Greiner-Petter
 */
public class BaseXRequestMapper implements FlatMapFunction<Path, Tuple3<String, Integer, Short>> {
    private static final Logger LOG = LogManager.getLogger(BaseXRequestMapper.class.getName());

    public BaseXRequestMapper() {}

    @Override
    public void flatMap(Path path, Collector<Tuple3<String, Integer, Short>> collector) {
        try {
            Document doc = Document.parseDocument(path);

            LinkedList<String> expressions = doc.getExpressions();
            LinkedList<Short> freqs = doc.getTermFrequencies();
            LinkedList<Short> depths = doc.getDepths();
            int counter = 0;

            while ( !expressions.isEmpty() ){
                Tuple3<String, Integer, Short> entry = new Tuple3<>(
                        expressions.pop(),
                        (int)freqs.pop(),
                        depths.pop()
                );

                collector.collect(entry);
                counter++;
            }

//            TFIDFCalculator.PROCESSED++;
//            String msg = String.format(
//                    "Finished %10s (%5s); Contained %3d math expressions; Processed: %6d / %d",
//                    docID,
//                    db,
//                    counter,
//                    Splitter.PROCESSED,
//                    Splitter.NUM_OF_FILES
//            );
        } catch (IOException e) {
            LOG.error("Cannot parse document " + path.toString(), e);
        }
    }

    public static Document getDocument(String docID, Path outputPath){
        String query = XQueryLoader.getScript(docID);
        String db = BaseXController.getDBFromDocID(docID);

        Path p = outputPath.resolve(db).resolve(docID);
        if (Files.exists(p)){
            // file already exist -> skip it
            LOG.info("File already processed. Skip it. " + p.toString());

            // empty document
            Splitter.PROCESSED++;
            Splitter.update();
            return new Document();
        }

        BaseXClient client = BaseXController.getBaseXClient(docID);

        // check if document is empty or exists
        if ( db == null || client == null ){
            // empty document
            Splitter.PROCESSED++;
            Splitter.EMPTY_FILES++;
            String msg = String.format(
                    "Finished %10s (%5s); Contained %3d math expressions; Processed: %6d / %d",
                    docID,
                    db,
                    0,
                    Splitter.PROCESSED,
                    Splitter.NUM_OF_FILES
            );
            LOG.warn(msg);
            Splitter.update();
            return new Document();
        }

        // doc exists -> proceed
        LOG.info("Requesting math in " + docID + " (" + db + ")");

        // lets measure time
        long start = System.currentTimeMillis();
        Document doc = new Document(db, docID);

        try {
            // execute extraction script
            String results = client.execute("XQUERY " + query);
            // stop time
            start = System.currentTimeMillis() - start;

            LOG.debug("Received result from BaseX for " + docID + " (" + db + ") - it took " + start + "ms");

            int counter = 0;
            Matcher matcher = Constants.BASEX_ELEMENT_PATTERN.matcher(results);

            // go through all hits
            while (matcher.find()) {
                doc.addFormula(
                        matcher.group(3),                   // expression
                        Short.parseShort(matcher.group(2)), // frequency
                        Short.parseShort(matcher.group(1))  // depth
                );

                counter++;
            }

            Splitter.PROCESSED++;
            String msg = String.format(
                    "Finished %10s (%5s); Contained %3d math expressions; Processed: %6d / %d",
                    docID,
                    db,
                    counter,
                    Splitter.PROCESSED,
                    Splitter.NUM_OF_FILES
            );

            if (counter < 2){
                if ( counter == 0 ){
                    Splitter.EMPTY_FILES++;
                }
                LOG.warn(msg);
            }
            else LOG.info(msg);
        } catch (IOException | NullPointerException e) {
            LOG.error("Cannot execute script to retrieve math (docID: " + docID + ")", e);
        } finally {
            BaseXController.returnBaseXClient(docID, client);
            Splitter.update();
        }

        return doc;
    }


//
//    @Override
//    public void flatMap(String docID, Collector<Tuple4<String, Short, Integer, Integer>> collector) {
//        String query = XQueryLoader.getScript(docID);
//
//        String db = BaseXController.getDBFromDocID(docID);
//        BaseXClient client = BaseXController.getBaseXClient(docID);
//
//        if (db == null || client == null){
//            // empty document
//            TFIDFCalculator.PROCESSED++;
//            TFIDFCalculator.EMPTY_FILES++;
//            String msg = String.format(
//                    "Finished %10s (%5s); Contained %3d math expressions; Processed: %6d / %d",
//                    docID,
//                    db,
//                    0,
//                    TFIDFCalculator.PROCESSED,
//                    TFIDFCalculator.NUM_OF_FILES
//            );
//            LOG.warn(msg);
//            TFIDFCalculator.update();
//            return;
//        }
//
//        LOG.info("Requesting math in " + docID + " (" + db + ")");
//
//        long start = System.currentTimeMillis();
//        try {
//            String results = client.execute("XQUERY " + query);
//
//            start = System.currentTimeMillis() - start;
//            LOG.debug("Received result from BaseX for " + docID + " (" + db + ") - it took " + start + "ms");
//
//            int counter = 0;
//            Matcher matcher = Constants.BASEX_ELEMENT_PATTERN.matcher(results);
//            while (matcher.find()) {
//                // found new line of math element
//                Tuple4<String, Short, Integer, Integer> element = new Tuple4<>(
//                        matcher.group(3),
//                        Short.parseShort(matcher.group(2)), // depth
//                        Integer.parseInt(matcher.group(1)), // TF
//                        1 // DF
//                );
//
////                MathElement element = new MathElement(
////                        matcher.group(3),
////                        Integer.parseInt(matcher.group(2)),
////                        Integer.parseInt(matcher.group(1)),
////                        1
////                );
//
//                collector.collect(element);
//                counter++;
//            }
//
//            TFIDFCalculator.PROCESSED++;
//            String msg = String.format(
//                    "Finished %10s (%5s); Contained %3d math expressions; Processed: %6d / %d",
//                    docID,
//                    db,
//                    counter,
//                    TFIDFCalculator.PROCESSED,
//                    TFIDFCalculator.NUM_OF_FILES
//            );
//            if (counter < 10){
//                if ( counter == 0 ){
//                    TFIDFCalculator.EMPTY_FILES++;
//                }
//                LOG.warn(msg);
//            }
//            else LOG.info(msg);
//        } catch (IOException e) {
//            LOG.error("Cannot execute script to retrieve math (docID: " + docID + ")", e);
//        } finally {
//            BaseXController.returnBaseXClient(docID, client);
//            TFIDFCalculator.update();
//        }
//    }
}
