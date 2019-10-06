package mir.formulacloud.tfidf;

import mir.formulacloud.beans.MathElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
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

    private String writerID;

    public Writer (Path outputFile, BlockingQueue<MathElement> queue) throws IOException {
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
            MathElement element = queue.take();

            // writing cache
            LOG.info("["+writerID+"] Retrieved first element. Kick-off writing process.");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile.toAbsolutePath().toString()))) {
                while( !element.isStopper() ){
                    writer.append(element.toString());
                    writer.append(NL);
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
