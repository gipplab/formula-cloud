package mir.formulacloud.tfidf;

import mir.formulacloud.beans.BaseXServerInstances;
import mir.formulacloud.util.TFIDFConfig;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * @author Andre Greiner-Petter
 */
public class DatastructureAnalyzer {

    private static final Logger LOG = LogManager.getLogger(DatastructureAnalyzer.class.getName());

    private TFIDFConfig config;

    private HashMap<String, BaseXServerInstances> basexServers;
    private HashMap<String, String> fileMapping;
    private HashMap<String, LinkedList<String>> groupedFileMapping;

    private int numFiles;

    public DatastructureAnalyzer(TFIDFConfig config){
        this.config = config;
        this.basexServers = new HashMap<>();
        this.fileMapping = new HashMap<>();
        this.groupedFileMapping = new HashMap<>();
    }

    public int init() throws IOException {
        LOG.info("Collecting all files from directory (including subdirectories)");
        final String baseP = Paths.get(config.getDataset()).getFileName().toString();

        Files
                .walk(Paths.get(config.getDataset()))
                .sequential() // mandatory, otherwise the hashmaps may vary in sizes and entries
                .filter( p -> {
                    if (!Files.isRegularFile(p)){
                        String folder = p.getFileName().toString();
                        if ( !folder.equals(baseP) ){
                            basexServers.put(folder, null);
                            groupedFileMapping.put(folder, new LinkedList<>());
                        }
                    }
                    return Files.isRegularFile(p);
                })
                .forEach( p -> {
                    String fileName = FilenameUtils.removeExtension(p.getFileName().toString());
                    String folderName = p.getParent().getFileName().toString();
                    fileMapping.put(fileName, folderName);
                    groupedFileMapping.get(folderName).push(fileName);
                });

        LOG.info("Done collecting the files. Start init flink plan.");
        numFiles = fileMapping.keySet().size();
        LOG.info("Total number of files: " + numFiles);
        BaseXController.initBaseXServers(basexServers, fileMapping, config);
        return numFiles;
    }

    public LinkedList<String> getEvenProcessingOrder(){
        LinkedList<String> set = new LinkedList<>();

        LOG.info("Create ordered list of files evenly distributed between file topics.");
        Collection<LinkedList<String>> lists = groupedFileMapping.values();

        while(!lists.isEmpty()){
            for (Iterator<LinkedList<String>> iterator = lists.iterator(); iterator.hasNext(); /**/) {
                LinkedList<String> list = iterator.next(); // cannot be empty by default
                if(list.isEmpty()){
                    iterator.remove(); // removes list from the iterator
                    continue;
                }
                set.add(list.pop());
            }
        }

        LOG.info("Done creating distributional file list.");
        return set;
    }

}
