package mir.formulacloud.searcher;

import mir.formulacloud.beans.*;
import org.elasticsearch.search.SearchHits;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Andre Greiner-Petter
 */
public class ArxivTests {

    private static final int DOCS = 135914;
    private static final String index = "arxiv-no-problem";

    private static SearcherConfig config;
    private static SearcherService service;
    private static List<MathDocument> mathDocs;

    // best settings: ESHits 20, MinDoc30, MinHit5, NORM_IDF
//    private static final String searchQuery = "Riemann Zeta Function";
//    private static final String expected = ".*mrow\\(mi:ζ,mo:ivt,mrow\\(mo:\\(,.*,mo:\\)\\)\\).*";
//    private static final String expected = ".*mi:ζ.*";

    // best setting: ESHits 20, MinDoc10, MinHit5, NORM_IDF
    private static final String searchQuery = "Gamma Function";
    private static final String expected = ".*mi:Γ.*";

    // best setting: ESHits 20, MinDoc30, MinHit5, NORM_IDF
//    private static final String searchQuery = "Bessel Function";
//    private static final String expected = ".*msub\\(mi:J,(mrow\\(mi:ν\\)|mi:ν)\\).*";

    private static MathMergeFunctions mergeF;

    @BeforeAll
    public static void init(){
        config = new SearcherConfig();
        config.setTfidfData("/opt/arxmliv/tfidf-results");
        config.setDatabaseParentFolder("/opt/arxmliv/empty-dump/");
        config.setElasticsearchMaxHits(20);

        config.setMaxDocumentFrequency(DOCS/4);
        mergeF = MathMergeFunctions.MAX;

        config.setMinTermFrequency(1);

        service = new SearcherService(config);
        service.init();

        SearchHits hits = service.getSearchResults(searchQuery, index);
        mathDocs = service.getMathResults(hits);
        mathDocs = service.requestMath(mathDocs);

        double mem = Runtime.getRuntime().totalMemory()/Math.pow(1024,2);
        System.out.println("Finish setup - Memory usage now: " + mem + " MB");
    }

    @ParameterizedTest
    @EnumSource(value = TFIDFs.class)
    public void testTFIDFOptionsMinDoc10MinHit1(TFIDFs options){
        config.setMinDocumentFrequency(10);
        System.out.println(options.msg);
        compute(options.getOptions(), 1);
    }

    @ParameterizedTest
    @EnumSource(value = TFIDFs.class)
    public void testTFIDFOptionsMinDoc30MinHit1(TFIDFs options){
        config.setMinDocumentFrequency(30);
        System.out.println(options.msg);
        compute(options.getOptions(), 1);
    }

    @ParameterizedTest
    @EnumSource(value = TFIDFs.class)
    public void testTFIDFOptionsMinDoc10MinHit5(TFIDFs options){
        config.setMinDocumentFrequency(10);
        System.out.println(options.msg);
        compute(options.getOptions(), 5);
    }

    @ParameterizedTest
    @EnumSource(value = TFIDFs.class)
    public void testTFIDFOptionsMinDoc30MinHit5(TFIDFs options){
        config.setMinDocumentFrequency(30);
        System.out.println(options.msg);
        compute(options.getOptions(), 5);
    }

    private static void compute(TFIDFOptions options, int minHitFrequency){
        HashMap<String, List<TFIDFMathElement>> tfidfMath =
                service.mapMathDocsToTFIDFElements(mathDocs, DOCS, options);
        List<TFIDFMathElement> results = service.groupTFIDFElements(tfidfMath, mergeF, minHitFrequency);

        testResults(results, expected);
    }

    @AfterAll
    public static void end(){
        service.shutdown();
    }

    public static void testResults(List<TFIDFMathElement> results, String regex){
        CLISearcher.checkHits(results, regex, 30);
    }

    @SuppressWarnings("unused")
    private enum TFIDFs {
        RAW_IDF("RAW * IDF", TermFrequencies.RAW, InverseDocumentFrequencies.IDF),
        REL_IDF("REL * IDF", TermFrequencies.RELATIVE, InverseDocumentFrequencies.IDF),
        LOG_IDF("LOG * IDF", TermFrequencies.LOG, InverseDocumentFrequencies.IDF),
        NORM_IDF("NORM * IDF", TermFrequencies.NORM, InverseDocumentFrequencies.IDF),
        RAW_PROP_IDF("RAW * PROP_IDF", TermFrequencies.RAW, InverseDocumentFrequencies.PROP_IDF),
        REL_PROP_IDF("REL * PROP_IDF", TermFrequencies.RELATIVE, InverseDocumentFrequencies.PROP_IDF),
        LOG_PROP_IDF("LOG * PROP_IDF", TermFrequencies.LOG, InverseDocumentFrequencies.PROP_IDF),
        NORM_PROP_IDF("NORM * PROP_IDF", TermFrequencies.NORM, InverseDocumentFrequencies.PROP_IDF);

        private String msg;
        private TermFrequencies tf;
        private InverseDocumentFrequencies idf;

        TFIDFs(String msg, TermFrequencies tf, InverseDocumentFrequencies idf){
            this.msg = msg;
            this.tf = tf;
            this.idf = idf;
        }

        TFIDFOptions getOptions(){
            return new TFIDFOptions(tf, idf);
        }
    }
}
