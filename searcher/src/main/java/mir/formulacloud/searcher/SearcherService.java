package mir.formulacloud.searcher;

import mir.formulacloud.beans.*;
import mir.formulacloud.elasticsearch.ElasticSearchConnector;
import mir.formulacloud.tfidf.BaseXController;
import mir.formulacloud.util.Helper;
import mir.formulacloud.util.TFIDFConfig;
import mir.formulacloud.util.TFIDFLoader;
import mir.formulacloud.util.XQueryLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

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

    public List<MathDocument> requestMath(List<MathDocument> documents){
        LOG.info("Collecting math for each document from BaseX.");
        documents.forEach(MathDocument::requestMathFromBasex);
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
                            options
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
    public LinkedList<TFIDFMathElement> groupTFIDFElements(HashMap<String, List<TFIDFMathElement>> elements, MathMergeFunctions mergeFunction){
        return groupTFIDFElements(elements, mergeFunction, 1);
    }

    public LinkedList<TFIDFMathElement> groupTFIDFElements(HashMap<String, List<TFIDFMathElement>> elements, MathMergeFunctions mergeFunction, int minHitFrequency){
        LinkedList<TFIDFMathElement> finalElements = new LinkedList<>();

        for( List<TFIDFMathElement> list : elements.values() ){
            // skip if function does not appear in minimum number of hits
            if ( list.size() < minHitFrequency ) continue;

            double[] scores = new double[list.size()];
            long localfreq = 0;
            for( int i = 0; i < scores.length; i++ ){
                scores[i] = list.get(i).getScore();
                localfreq += list.get(i).getTotalFrequency();
            }
            double totalScore = mergeFunction.calculate(scores);
            TFIDFMathElement tfidfElement = new TFIDFMathElement(list.get(0), totalScore);
            tfidfElement.setTotalFrequency(localfreq);
            tfidfElement.setDocFrequency(list.size());
            finalElements.add(tfidfElement);
        }

        finalElements.sort(Comparator.comparing(TFIDFMathElement::getScore).reversed());
        return finalElements;
    }

    public static void main(String[] args) {
        SearcherConfig config = SearcherConfig.loadConfig(args);
        CLISearcher cliSearcher = new CLISearcher(config);
        cliSearcher.init();
        cliSearcher.start();
    }
}