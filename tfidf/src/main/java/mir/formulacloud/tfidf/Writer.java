package mir.formulacloud.tfidf;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;

/**
 * @author Andre Greiner-Petter
 */
public class Writer<T> implements Runnable {

    private static final Logger LOG = LogManager.getLogger(Writer.class.getName());
    private static final String NL = System.lineSeparator();

    private BlockingQueue<T> queue;
    private Path outputFile;

    private String writerID;

    public Writer (Path outputFile, BlockingQueue<T> queue) throws IOException {
        Files.deleteIfExists(outputFile);
        Files.createFile(outputFile);

        this.outputFile = outputFile;
        this.queue = queue;
        this.writerID = outputFile.getFileName().toString();
    }

    @Override
    public void run() {
        LOG.info("["+writerID+"] Writer starts " + outputFile);
        LOG.info("["+writerID+"] Await first element...");

        try {
            T element = queue.take();

            // writing cache
            LOG.info("["+writerID+"] Retrieved first element. Kick-off writing process.");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile.toAbsolutePath().toString()))) {
                while( element != null ){
                    writer.write(element.toString()+NL);
                    element = queue.take();
                }
                LOG.info("["+writerID+"] Found stop signal. Finish writing.");
            } catch ( IOException ioe ){
                LOG.fatal("["+writerID+"] Cannot write to output file " + outputFile.toString(), ioe);
            }

        } catch (InterruptedException e) {
            LOG.warn("["+writerID+"] Received interruption signal. Stop writing...");
        }

        LOG.info("["+writerID+"] Writer finished task. Written to " + outputFile.toString());
    }
}
