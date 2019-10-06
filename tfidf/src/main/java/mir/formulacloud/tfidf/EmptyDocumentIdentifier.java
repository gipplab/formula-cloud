package mir.formulacloud.tfidf;

import com.beust.jcommander.JCommander;
import com.formulasearchengine.mathosphere.basex.BaseXClient;
import mir.formulacloud.beans.BaseXServerInstances;
import mir.formulacloud.util.TFIDFConfig;
import mir.formulacloud.util.XQueryLoader;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * @author Andre Greiner-Petter
 */
public class EmptyDocumentIdentifier {

    private static final Logger LOG = LogManager.getLogger(EmptyDocumentIdentifier.class.getName());
    private static final String NL = System.lineSeparator();

    private final TFIDFConfig config;
    private String script;
    private Set<String> serverNames;

    public EmptyDocumentIdentifier ( TFIDFConfig config ){
        this.config = config;
    }

    public void init() throws IOException {
        LOG.info("Initialize server connections.");

        this.script = "XQUERY " + XQueryLoader.getIdentifyEmptyDocIDScript();
        HashMap<String, BaseXServerInstances> servers = new HashMap<>();

        Files
                .walk(Paths.get(config.getDataset()))
                .sequential() // mandatory, otherwise the hashmaps may vary in sizes and entries
                .filter( p -> !Files.isRegularFile(p))
                .forEach( p -> {
                    if ( !config.getDataset().contains(p.getFileName().toString()) )
                        servers.put(p.getFileName().toString(), null);
                });

        serverNames = servers.keySet();

        BaseXController.initBaseXServers( servers, null, config );
        LOG.info("Initialization finished successful.");
    }

    public void run() throws IOException {
        StringBuffer sb = new StringBuffer();

        serverNames.parallelStream()
                .flatMap( s -> {
                    BaseXClient client = BaseXController.getBaseXClientByDatabase(s);
                    try {
                        String result = client.execute(script);
                        return Arrays.stream(result.split("[\\r\\n]+"));
                    } catch (IOException e) {
                        LOG.fatal("Cannot retrieve docs.", e);
                        return Arrays.stream(new String[]{});
                    }
                })
                .forEach( s -> {
                    sb.append(s);
                    sb.append(NL);
                } );

        LOG.info("Done retrieving list from server. Start writing.");
        Path out = Paths.get(config.getOutputF());
        Files.write(out, sb.toString().getBytes(), StandardOpenOption.CREATE);
    }

    public static void main(String[] args){
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

        EmptyDocumentIdentifier prog = new EmptyDocumentIdentifier(config);
        try {
            prog.init();
            prog.run();
        } catch (IOException e) {
            LOG.fatal("Something went wrong.", e);
        } finally {
            BaseXController.closeAllClients();
        }
    }

}
