package mir.formulacloud.util;

import mir.formulacloud.tfidf.Writer;
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
public class BasexDataWriter implements Runnable {

    private static final Logger LOG = LogManager.getLogger(Writer.class.getName());
    private static final String NL = System.lineSeparator();

    private BlockingQueue<String> queue;
    private Path outputFile;

    private String writerID;
    private String[] outerTags;

    public BasexDataWriter (Path outputFile, BlockingQueue<String> queue) throws IOException {
        Files.deleteIfExists(outputFile);
        Files.createFile(outputFile);

        this.outputFile = outputFile;
        this.queue = queue;
        this.writerID = outputFile.getFileName().toString();
        this.outerTags = SimpleMMLConverter.outerTags();
    }

    @Override
    public void run() {
        LOG.info("["+writerID+"] Writer starts " + outputFile);
        LOG.info("["+writerID+"] Await first element...");

        try {
            String str = queue.take();

            // writing cache
            LOG.info("["+writerID+"] Retrieved first element. Kick-off writing process.");
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile.toAbsolutePath().toString()))) {
                writer.write(outerTags[0]);
                while( !str.equals("STOP") ){
                    writer.write(str+NL);
                    str = queue.take();
                }
                writer.write(outerTags[1]);
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
