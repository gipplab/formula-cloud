package mir.formulacloud.searcher;

import com.formulasearchengine.mathmltools.mml.elements.MathDoc;
import mir.formulacloud.beans.MathDocument;
import mir.formulacloud.beans.MathElement;
import mir.formulacloud.beans.MathMergeFunctions;
import mir.formulacloud.beans.TFIDFMathElement;
import mir.formulacloud.util.SimpleMMLConverter;
import mir.formulacloud.util.XQueryLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.search.SearchHits;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Andre Greiner-Petter
 */
public class CLISearcher extends SearcherService {
    private static final Logger LOG = LogManager.getLogger(CLISearcher.class.getName());

    public static final String NL = System.lineSeparator();

    private static final int
            CMD_INDEX = 1,
            CMD_ESHITS = 2,
            CMD_DFMIN = 3,
            CMD_DFMAX = 4,
            CMD_TFMIN = 5,
            CMD_RES = 6,
            CMD_DEF = 7,
            CMD_MIN_ES_HITS = 8,
            CMD_EXPORT = 9;

    private static final Pattern SET_CMDS = Pattern.compile(
            "SET INDEX ([\\w-]+)|" +
            "SET ES HITS (\\d+)|" +
            "SET DF MIN (\\d+)|" +
            "SET DF MAX (\\d+)|" +
            "SET TF MIN (\\d+)|" +
            "SET NUM RESULTS (\\d+)|" +
            "SET (DEFAULT)|" +
            "SET MIN HITS (\\d+)|" +
            "EXPORT LAST (.+)"
    );

    private static final Pattern GET_CMDS = Pattern.compile(
            "GET (INDEX)|" +
            "GET (ES HITS)|" +
            "GET (DF MIN)|" +
            "GET (DF MAX)|" +
            "GET (TF MIN)|" +
            "GET (NUM RESULTS)|" +
            "GET(\\sALL|)|" +
            "GET (MIN HITS)|" +
            "GET TOTAL DOCS"
    );

    private static final Pattern SEARCH_CMD = Pattern.compile(
            "SEARCH\\s*(-expect ([^\\s]*))? (.*)$"
    );

    private String currentIndex = "arxiv";
    private int showNumberOfResults = 10;
    private int minEShits = 1;

    private SearcherConfig config;

    private List<TFIDFMathElement> lastResults = null;

    public CLISearcher(SearcherConfig config){
        super(config);
        this.config = config;
        currentIndex = config.getFixedIndex();
    }

    public void start(){
        System.out.println("Welcome to the searcher standalone program. Enter 'help' to see possible options.");
        System.out.print("> ");
        Scanner in = new Scanner(System.in);
        String input;

        while(!(input = in.nextLine()).matches("\\s*quit\\s*|\\s*exit\\s*")){
            interpretRequest(input);
            System.out.print("> ");
        }

        shutdown();
        System.out.println("Thanks for using the standalone version. Bye Bye");
    }

    private void interpretRequest(String input){
        Matcher setMatcher = SET_CMDS.matcher(input);
        if (setMatcher.matches()) {
            setter(setMatcher);
            return;
        }

        Matcher getMatcher = GET_CMDS.matcher(input);
        if (getMatcher.matches()) {
            getter(getMatcher);
            return;
        }

        Matcher searchMatcher = SEARCH_CMD.matcher(input);
        if (searchMatcher.matches()) {
            String expectedRegex = searchMatcher.group(2); // might be null
            String searchQuery = searchMatcher.group(3);
            run(searchQuery, expectedRegex);
            return;
        }

        printHelp();
    }

    private void run(String searchQuery, String expected){
        long numberOfDocs = getNumberOfDocuments(currentIndex);
        SearchHits hits = getSearchResults(searchQuery, currentIndex);
        List<MathDocument> mdocs = getMathResults(hits);
        mdocs = requestMath(mdocs);
        HashMap<String, List<TFIDFMathElement>> tfidfMath = mapMathDocsToTFIDFElements(mdocs, numberOfDocs);
        List<TFIDFMathElement> results = groupTFIDFElements(tfidfMath, MathMergeFunctions.MAX, minEShits);
        lastResults = results;
        if (expected == null || expected.isEmpty()){
            System.out.println("Total Hits: " + results.size());
            for (int i = 0; i < showNumberOfResults && i < results.size(); i++){
                System.out.println((i+1)+": " + results.get(i));
            }
        } else {
            checkHits(results, expected, showNumberOfResults);
        }
    }

    public void runZBMath(String collection) throws IOException {
        minEShits = 1;
        showNumberOfResults = 50;
        long numberOfDocs = getNumberOfDocuments("zbmath");
        Path p = Paths.get("data").resolve(collection);
        List<String> ids = Files.lines(p).collect(Collectors.toList());
        List<MathDocument> mdocs = getMathResults(ids);
        mdocs = requestMath(mdocs);
        HashMap<String, List<TFIDFMathElement>> tfidfMath = mapMathDocsToTFIDFElements(mdocs, numberOfDocs);
        List<TFIDFMathElement> results = groupTFIDFElements(tfidfMath, MathMergeFunctions.MAX, minEShits);
        lastResults = results;
        wirteResults(Paths.get("data").resolve(collection+"Results.txt"), results);
        shutdown();
    }

    public void runAll() throws IOException {
        minEShits = config.getMinDocumentFrequency();
        showNumberOfResults = 300;
        LOG.info("Requesting all files from folder.");
        List<MathDocument> mdocs = requestAllDocs(Paths.get(config.getDatabaseParentFolder()));
        long numberOfDocs = mdocs.size();
        LOG.info("Done. Total size of documents: " + numberOfDocs);
        LOG.info("Start requesting math from BaseX for all documents.");
        System.out.println("Total Docs: " + numberOfDocs);
        mdocs = requestMath(mdocs);
        LOG.info("Done requesting all math. Start calculating TF-IDF values.");
        HashMap<String, List<TFIDFMathElement>> tfidfMath = mapMathDocsToTFIDFElements(mdocs, numberOfDocs);
        LOG.info("Done calculating TF-IDF values. Merging entries and find MAX.");
        List<TFIDFMathElement> results = groupTFIDFElements(tfidfMath, MathMergeFunctions.MAX, minEShits);
        LOG.info("Done. Writing results to data/ZBMathTotalResults.txt");
        lastResults = results;
        wirteResults(Paths.get("data").resolve("ZBMathTotalResults.txt"), results);
        shutdown();
    }

    public static void checkHits(List<TFIDFMathElement> results, String regex, int maxEntries){
        LinkedList<TestHit> hits = new LinkedList<>();

        int idx = 1;
        Pattern p = Pattern.compile(regex);
        System.out.println("Test regex: " + regex);

        for ( TFIDFMathElement e : results ){
            Matcher m = p.matcher(e.getExpression());
            if (m.matches()){
                TestHit h = new TestHit(idx, e);
                hits.add(h);
            }

            if (idx <= maxEntries) {
                System.out.println(idx + ": " + e);
            }
            idx++;
        }
        System.out.println();
        System.out.println("Total: " + results.size());
        System.out.println("All hits:");
        for (TestHit hit : hits) {
            System.out.println(hit);
        }
    }

    private void printHelp(){
        StringBuilder sb = new StringBuilder();
        sb.append("quit or exit             - shutdown the service").append(NL);
        sb.append("SET/GET INDEX <w>        - ElasticSearch index").append(NL);
        sb.append("SET/GET ES HITS <d>      - number of document hits from elastic search").append(NL);
        sb.append("SET/GET MIN HITS <d>     - in how many document hits from ES should a formula appear").append(NL);
        sb.append("SET/GET DF MIN <d>       - minimum document frequency").append(NL);
        sb.append("SET/GET DF MAX <d>       - maximum document frequency").append(NL);
        sb.append("SET/GET TF MIN <d>       - minimum term frequency per document").append(NL);
        sb.append("SET/GET NUM RESULTS <d>  - set the number of top results that should be shown").append(NL);
        sb.append("SET DEFAULT              - set all parameter to default").append(NL);
        sb.append("GET ALL                  - returns all parameter").append(NL);
        sb.append("GET TOTAL DOCS           - returns number of total docs in current ES index").append(NL);
        sb.append("EXPORT LAST <s>          - export the last results with MathML to given path").append(NL);
        sb.append("SEARCH <s>               - runs the program for given search query").append(NL);
        sb.append("SEARCH -expect <s> <s>   - runs the program for given search query and a given expected value (as java regex)").append(NL);
        System.out.println(sb.toString());
    }

    private void printCurrentSettings(){
        StringBuilder sb = new StringBuilder();
        sb.append("Index: ").append(currentIndex).append(NL);
        sb.append("ES HITS: ").append(config.getElasticsearchMaxHits()).append(NL);
        sb.append("MIN HITS: ").append(minEShits).append(NL);
        sb.append("DF MIN: ").append(config.getMinDocumentFrequency()).append(NL);
        sb.append("DF MAX: ").append(config.getMaxDocumentFrequency()).append(NL);
        sb.append("TF MIN: ").append(config.getMinTermFrequency()).append(NL);
        sb.append("#Results: ").append(showNumberOfResults).append(NL);
        System.out.println("Current Settings are");
        System.out.println(sb.toString());
    }

    private void setter(Matcher match){
        if (match.group(CMD_INDEX) != null){
            currentIndex = match.group(CMD_INDEX);
            System.out.println("Set index to " + currentIndex);
        } else if (match.group(CMD_ESHITS) != null){
            int n = Integer.parseInt(match.group(CMD_ESHITS));
            config.setElasticsearchMaxHits(n);
            System.out.println("Set elastic search hits to " + n);
        } else if (match.group(CMD_DFMIN) != null){
            int n = Integer.parseInt(match.group(CMD_DFMIN));
            config.setMinDocumentFrequency(n);
            System.out.println("Set minimum document frequency to " + n);
        } else if (match.group(CMD_DFMAX) != null){
            int n = Integer.parseInt(match.group(CMD_DFMAX));
            config.setMaxDocumentFrequency(n);
            System.out.println("Set maximum document frequency to " + n);
        } else if (match.group(CMD_TFMIN) != null){
            int n = Integer.parseInt(match.group(CMD_TFMIN));
            config.setMinTermFrequency(n);
            XQueryLoader.initMinTermFrequency(n);
            System.out.println("Set minimum term frequency per document to " + n);
        } else if (match.group(CMD_RES) != null){
            showNumberOfResults = Integer.parseInt(match.group(CMD_RES));
            System.out.println("Set number of top results shown to " + showNumberOfResults);
        } else if (match.group(CMD_MIN_ES_HITS) != null){
            minEShits = Integer.parseInt(match.group(CMD_MIN_ES_HITS));
            System.out.println("Set number of minimum hits per search query to " + minEShits);
        } else if (match.group(CMD_EXPORT) != null){
            if (lastResults == null){
                System.out.println("There are no results in cache.");
                return;
            }
            Path out = Paths.get(match.group(CMD_EXPORT));
            try {
                wirteResults(out, lastResults);
            } catch (IOException e) {
                System.out.println("An error occurred during export: " + e.getMessage());
            }
        } else if (match.group(CMD_DEF) != null){
            currentIndex = "arxiv";
            config.setElasticsearchMaxHits(10);
            config.setMinDocumentFrequency(1);
            config.setMaxDocumentFrequency(100000);
            config.setMinTermFrequency(2);
            showNumberOfResults = 10;
            minEShits = 1;
            printCurrentSettings();
        }
    }

    private void getter(Matcher match){
        if (match.group(CMD_INDEX) != null){
            System.out.println(currentIndex);
        } else if (match.group(CMD_ESHITS) != null){
            System.out.println(config.getElasticsearchMaxHits());
        } else if (match.group(CMD_DFMIN) != null){
            System.out.println(config.getMinDocumentFrequency());
        } else if (match.group(CMD_DFMAX) != null){
            System.out.println(config.getMaxDocumentFrequency());
        } else if (match.group(CMD_TFMIN) != null){
            System.out.println(config.getMinTermFrequency());
        } else if (match.group(CMD_RES) != null){
            System.out.println(showNumberOfResults);
        } else if (match.group(CMD_DEF) != null){
            printCurrentSettings();
        } else if (match.group(CMD_MIN_ES_HITS) != null){
            System.out.println(minEShits);
        } else {
            long numOfDocs = getNumberOfDocuments(currentIndex);
            System.out.println("Index " + currentIndex + " has " + numOfDocs + " documents.");
        }
    }

    public void wirteResults(Path outputFile, List<TFIDFMathElement> results) throws IOException {
        Files.deleteIfExists(outputFile);

        try (BufferedWriter bw = Files.newBufferedWriter(outputFile, StandardOpenOption.CREATE_NEW)){
            results.forEach( e -> {
                try {
                    bw.write(""+e.getScore() + "," + SimpleMMLConverter.stringToMML(e.getExpression()));
                    bw.newLine();
                } catch (IOException ioe){
                    ioe.printStackTrace();
                }
            });
        } finally {
            System.out.println("Exported last results to " + outputFile);
        }
    }

    private static class TestHit {
        int position;
        TFIDFMathElement element;

        public TestHit(int pos, TFIDFMathElement element){
            this.position = pos;
            this.element = element;
        }

        @Override
        public String toString(){
            return position + ": " + element;
        }
    }

}
