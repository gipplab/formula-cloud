package mir.formulacloud.searcher;

import com.formulasearchengine.mathmltools.mml.elements.MathDoc;
import mir.formulacloud.beans.*;
import mir.formulacloud.elasticsearch.ElasticSearchConnector;
import mir.formulacloud.tfidf.BaseXController;
import mir.formulacloud.util.Helper;
import mir.formulacloud.util.TFIDFConfig;
import mir.formulacloud.util.TFIDFLoader;
import mir.formulacloud.util.XQueryLoader;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Andre Greiner-Petter
 */
public class SearcherService {
    private static final Logger LOG = LogManager.getLogger(SearcherService.class.getName());

    private final SearcherConfig config;

    private ElasticSearchConnector elasticsearch;

    public SearcherService(SearcherConfig config) {
        this.config = config;
    }

    public void init(){
        initBaseXServers();
        initElasticSearch();
        initTFIDFTables();
    }

    protected void initBaseXServers(){
        LOG.info("Init BaseXServer and BaseXClients.");
        HashMap<String, BaseXServerInstances> serversMap = new HashMap<>();
        Path dbFolder = Paths.get(config.getDatabaseParentFolder());
        String baseFolderName = dbFolder.getFileName().toString();
        try {
            Files.walk(dbFolder, 1)
                    .forEach(p -> {
                        String folder = p.getFileName().toString();
                        if ( !folder.equals(baseFolderName) )
                            serversMap.put(p.getFileName().toString(), null);
                    });

            BaseXController.initBaseXServers(serversMap, null, new TFIDFConfig());
            LOG.info("Successfully initialized BaseX connections.");
        } catch (IOException e) {
            LOG.fatal("Cannot create list of BaseXServers", e);
            System.exit(1);
        }
        XQueryLoader.initMinTermFrequency(config.getMinTermFrequency());
    }

    protected void initBaseXServers(String... databases) {
        HashMap<String, BaseXServerInstances> serversMap = new HashMap<>();
        for (String db : databases)
            serversMap.put(db, null);
        BaseXController.initBaseXServers(serversMap, null, new TFIDFConfig());
    }

    protected void initElasticSearch(){
        LOG.info("Init Elasticsearch connection.");
        elasticsearch = new ElasticSearchConnector(config);
        elasticsearch.start();
        LOG.info("Successfully initialized Elasticsearch connections.");
    }

    protected void initTFIDFTables(){
        LOG.info("Init TF-IDF cache.");
        TFIDFLoader.initTFIDFLoader(config.getTfidfData());
        LOG.info("Successfully loaded TF-IDF cache.");
    }

    protected void shutdown(){
        BaseXController.closeAllClients();
        elasticsearch.stop();
    }

    public long getNumberOfDocuments(String index){
        return elasticsearch.numberOfDocuments(index);
    }

    public SearchHits getSearchResults(String searchQuery, @NotNull String... indices){
        LOG.info("Collection documents from Elasticsearch for query: " + searchQuery);
        return elasticsearch.search(searchQuery, indices);
    }

    public List<MathDocument> getMathResults(SearchHits results){
        LOG.info("Generate MathDocuments from Elasticsearch hits.");
        SearchHit[] hits = results.getHits();
        LinkedList<MathDocument> mathDocs = new LinkedList<>();

        for (SearchHit hit : hits){
            Map<String, Object> info = hit.getSourceAsMap();
            String id = info.get(MathDocument.F_ID).toString();
            Object db = info.get(MathDocument.F_DB);
            if (db == null || db.toString().isEmpty()){
                // this document doesn't have math
                LOG.info("Document " + id + " consists of no math!");
                continue;
            }

            MathDocument doc = new MathDocument(id, db.toString(), (double)hit.getScore());
            Object url = info.get(MathDocument.F_URL);
            if (url != null) doc.setArxivURL(url.toString());
            mathDocs.add(doc);
            LOG.info("Retrieved document " + doc.getDocID() +
                    " (prec. " + doc.getEsSearchPrecision() + ") " +
                    "[" + doc.getBasexDB() + "]");
        }

        return mathDocs;
    }

    public List<MathDocument> getMathResults(List<String> zbMathIDs){
        LinkedList<MathDocument> mathDocs = new LinkedList<>();
        for(String zbID : zbMathIDs){
            GetResponse res = elasticsearch.getID(zbID);
            Map<String, Object> info = res.getSource();
            if (info == null){
                LOG.info("Didn't find ID " + zbID);
                continue;
            }

            String id = info.get(MathDocument.F_ID).toString();
            Object db = info.get(MathDocument.F_DB);
            if (db == null || db.toString().isEmpty()){
                // this document doesn't have math
                LOG.info("Document " + id + " consists of no math!");
                continue;
            }

            MathDocument doc = new MathDocument(id, db.toString(), -1);
            Object url = info.get(MathDocument.F_URL);
            if (url != null) doc.setArxivURL(url.toString());
            mathDocs.add(doc);
            LOG.info("Retrieved document " + doc.getDocID() +
                    " (prec. " + doc.getEsSearchPrecision() + ") " +
                    "[" + doc.getBasexDB() + "]");
        }

        LOG.info("Done. Found " + mathDocs.size() + " docs.");

        return mathDocs;
    }

    public List<MathDocument> requestMath(List<MathDocument> documents){
        LOG.info("Collecting math for each document from BaseX.");
        for (MathDocument doc : documents){
            if (doc == null) continue;
            LOG.info("Requesting math for " + doc.getDocID());
            doc.requestMathFromBasex(config);
        }
//        documents
//                .stream()
//                .filter(Objects::nonNull)
//                .parallel()
//                .forEach(e -> e.requestMathFromBasex(config));
        return documents;
    }

    /**
     *
     * @param docs
     * @return the result may contain entries multiple times. Use {{@link #groupTFIDFElements(HashMap, MathMergeFunctions)}} to group the entries.
     */
    public HashMap<String, List<TFIDFMathElement>> mapMathDocsToTFIDFElements(
            List<MathDocument> docs,
            long totalDocs
    ){
        return mapMathDocsToTFIDFElements(docs, totalDocs, TFIDFOptions.getDefaultTFIDFOption());
    }

    public HashMap<String, List<TFIDFMathElement>> mapMathDocsToTFIDFElements(
            List<MathDocument> docs,
            long totalDocs,
            TFIDFOptions options
    ){
        HashMap<String, List<TFIDFMathElement>> map = new HashMap<>();
        for(MathDocument doc : docs) {
            HashMap<String, TFIDFMathElement> docElements =
                    doc.getDocumentTFIDF(
                            totalDocs,
                            config.getMinDocumentFrequency(),
                            config.getMaxDocumentFrequency(),
                            options,
                            config
                    );
            Helper.collect(docElements, map);
        }
        return map;
    }

    /**
     *
     * @param elements
     * @param mergeFunction
     * @return ordered linked list of TF-IDF math elements
     */
    public List<TFIDFMathElement> groupTFIDFElements(HashMap<String, List<TFIDFMathElement>> elements, MathMergeFunctions mergeFunction){
        return groupTFIDFElements(elements, mergeFunction, 1);
    }

    public List<TFIDFMathElement> groupTFIDFElements(HashMap<String, List<TFIDFMathElement>> elements, MathMergeFunctions mergeFunction, int minHitFrequency){
//        LinkedList<TFIDFMathElement> finalElements = new LinkedList<>();

        LOG.info("Start merging math elements in parallel...");
        List<TFIDFMathElement> finalElements = elements.values().stream()
                .parallel()
                .map(l -> {
                    if (l.size() < minHitFrequency) return null;

                    double[] scores = new double[l.size()];
                    long localfreq = 0;
                    for (int i = 0; i < scores.length; i++) {
                        scores[i] = l.get(i).getScore();
                        localfreq += l.get(i).getTotalFrequency();
                    }
                    double totalScore = mergeFunction.calculate(scores);
                    TFIDFMathElement tfidfElement = new TFIDFMathElement(l.get(0), totalScore);
                    tfidfElement.setTotalFrequency(localfreq);
                    tfidfElement.setDocFrequency(l.size());
                    return tfidfElement;
                })
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(TFIDFMathElement::getScore).reversed())
                .collect(Collectors.toList());

//        for( List<TFIDFMathElement> list : elements.values() ){
//            // skip if function does not appear in minimum number of hits
//            if ( list.size() < minHitFrequency ) continue;
//
//
//        }

        LOG.info("Successfully finished merging math elements.");

        return finalElements;
    }

    public static List<MathDocument> requestAllDocs(Path input) throws IOException {
        return Files
                .walk(input)
                .parallel()
                .filter(Files::isRegularFile)
                .map( p -> {
                    String fileName = FilenameUtils.removeExtension(p.getFileName().toString());
                    String folderName = p.getParent().getFileName().toString();
                    return new MathDocument(fileName, folderName, -1);
                })
                .collect(Collectors.toList());
    }

    public static void main(String[] args) {
        SearcherConfig config = SearcherConfig.loadConfig(args);
        CLISearcher cliSearcher = new CLISearcher(config);
        cliSearcher.init();

//        try {
////            cliSearcher.runZBMath("EigenvalueIDs");
//            cliSearcher.runAll();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        cliSearcher.start();
    }
}
