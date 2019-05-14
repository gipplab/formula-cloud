package mir.formulacloud.tfidf;

import com.formulasearchengine.mathosphere.basex.BaseXClient;
import mir.formulacloud.beans.MathElement;
import mir.formulacloud.util.Constants;
import mir.formulacloud.util.XQueryLoader;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Andre Greiner-Petter
 */
public class BaseXRequestMapper implements FlatMapFunction<String, MathElement> {
    private static final Logger LOG = LogManager.getLogger(BaseXRequestMapper.class.getName());

    public BaseXRequestMapper() {}

    @Override
    public void flatMap(String docID, Collector<MathElement> collector) {
        String query = XQueryLoader.getScript(docID);

        String db = BaseXController.getDBFromDocID(docID);
        BaseXClient client = BaseXController.getBaseXClient(docID);

        if (db == null || client == null){
            // empty document
            TFIDFCalculator.PROCESSED++;
            TFIDFCalculator.EMPTY_FILES++;
            String msg = String.format(
                    "Finished %10s (%5s); Contained %3d math expressions; Processed: %6d / %d",
                    docID,
                    db,
                    0,
                    TFIDFCalculator.PROCESSED,
                    TFIDFCalculator.NUM_OF_FILES
            );
            LOG.warn(msg);
            TFIDFCalculator.update();
            return;
        }

        LOG.info("Requesting math in " + docID + " (" + db + ")");

        long start = System.currentTimeMillis();
        try {
            String results = client.execute("XQUERY " + query);

            start = System.currentTimeMillis() - start;
            LOG.debug("Received result from BaseX for " + docID + " (" + db + ") - it took " + start + "ms");

            int counter = 0;
            Matcher matcher = Constants.BASEX_ELEMENT_PATTERN.matcher(results);
            while (matcher.find()) {
                // found new line of math element
                MathElement element = new MathElement(
                        matcher.group(3),
                        Integer.parseInt(matcher.group(2)),
                        Integer.parseInt(matcher.group(1)),
                        1
                );
                collector.collect(element);
                counter++;
            }

            TFIDFCalculator.PROCESSED++;
            String msg = String.format(
                    "Finished %10s (%5s); Contained %3d math expressions; Processed: %6d / %d",
                    docID,
                    db,
                    counter,
                    TFIDFCalculator.PROCESSED,
                    TFIDFCalculator.NUM_OF_FILES
            );
            if (counter < 10){
                if ( counter == 0 ){
                    TFIDFCalculator.EMPTY_FILES++;
                }
                LOG.warn(msg);
            }
            else LOG.info(msg);
        } catch (IOException e) {
            LOG.error("Cannot execute script to retrieve math (docID: " + docID + ")", e);
        } finally {
            BaseXController.returnBaseXClient(docID, client);
            TFIDFCalculator.update();
        }
    }
}
