package mir.formulacloud.tfidf;

import mir.formulacloud.beans.MathElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;

/**
 * @author Andre Greiner-Petter
 */
public class Writer implements Runnable {

    private static final Logger LOG = LogManager.getLogger(Writer.class.getName());
    private static final String NL = System.lineSeparator();

    private BlockingQueue<MathElement> queue;
    private Path outputFile;

    private int batchSize = 100_000;

    private LinkedList<MathElement> cache;

    private boolean run = true;

    public Writer (Path outputFile, BlockingQueue<MathElement> queue) throws IOException {
        Files.deleteIfExists(outputFile);
        Files.createFile(outputFile);

        this.outputFile = outputFile;
        this.queue = queue;
        this.cache = new LinkedList<>();
    }

    @Override
    public void run() {
        LOG.info("Writer starts " + outputFile);

        while ( run ) {
            // filling up cache
            LOG.info("Filling up cache...");
            while ( cache.size() < batchSize ){
                try {
                    MathElement element = queue.take();

                    // trigger writing process
                    if ( element.isStopper() ) {
                        LOG.info("Retrieved stopper -> kick writing process.");
                        run = false;
                        break; // breaks out inner while -> fill cache
                    }

                    cache.add(element);
                } catch (InterruptedException e) {
                    LOG.info("Retrieved interrupt. Writing cache and stop.");
                    run = false;
                    break;
                }
            }

            // writing cache
            LOG.info("Start writing current batch.");
            StringBuffer bf = new StringBuffer();
            while ( !cache.isEmpty() ) {
                bf.append(cache.removeFirst()).append(NL);
            }
            try {
                Files.write(outputFile, bf.toString().getBytes(), StandardOpenOption.APPEND);
                LOG.debug("Writing batch successfully finished.");
            } catch (IOException e) {
                run = false;
                LOG.fatal("Cannot write results to output file " + outputFile.toString(), e);
            }
        }

        LOG.info("Writer finished task. Written to " + outputFile.toString());
    }
}
