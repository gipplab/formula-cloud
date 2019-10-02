package mir.formulacloud.searcher;

import mir.formulacloud.beans.*;
import mir.formulacloud.util.SimpleMMLConverter;
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

//    private static final int DOCS = 135914;
    private static final int DOCS = 1_300_000;
//    private static final String index = "arxiv-no-problem";
    private static final String index = "zbmath";

    private static SearcherConfig config;
    private static SearcherService service;
    private static List<MathDocument> mathDocs;

    // best settings: ESHits 20, MinDoc30, MinHit5, NORM_IDF
    private static final String searchQuery = "Riemann Zeta Function";
    private static final String expected = ".*mrow\\(mi:ζ,mo:ivt,mrow\\(mo:\\(,.*,mo:\\)\\)\\).*";
//    private static final String expected = ".*mi:ζ.*";

    // best setting: ESHits 20, MinDoc10, MinHit5, NORM_IDF
//    private static final String searchQuery = "Beta Function";
//    private static final String expected = ".*mi:B.*";

    // best setting: ESHits 20, MinDoc30, MinHit5, NORM_IDF
//    private static final String searchQuery = "Jacobi Polynomial";
//    private static final String expected = ".*msubsup\\(mi:P.*\\).*|.*mo:&gt.*";

    private static MathMergeFunctions mergeF;

    @BeforeAll
    public static void init(){
        config = new SearcherConfig();
        config.setTfidfData("/opt/zbmath/tfidf");
        config.setDatabaseParentFolder("/opt/zbmath/empty-dump/");
        config.setElasticsearchMaxHits(200);

        config.setMaxDocumentFrequency(100_000);
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

//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc25MinHit1(TFIDFs options){
//        config.setMinDocumentFrequency(25);
//        System.out.println(options.msg);
//        compute(options.getOptions(), 1);
//    }

//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc50MinHit1(TFIDFs options){
//        config.setMinDocumentFrequency(50);
//        System.out.println(options.msg);
//        compute(options.getOptions(), 1);
//    }

    @ParameterizedTest
    @EnumSource(value = TFIDFs.class)
    public void testTFIDFOptionsMinDoc25MinHit9(TFIDFs options){
        config.setMinDocumentFrequency(25);
        System.out.println(options.msg);
        compute(options.getOptions(), 9);
    }

    @ParameterizedTest
    @EnumSource(value = TFIDFs.class)
    public void testTFIDFOptionsMinDoc50MinHit9(TFIDFs options){
        config.setMinDocumentFrequency(50);
        System.out.println(options.msg);
        compute(options.getOptions(), 9);
    }

//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc25MinHit10B95K2(TFIDFs options){
//        TFIDFOptions op = options.getOptions();
//        op.setB(0.95);
//        op.setK1(1.8);
//
//        config.setMinDocumentFrequency(25);
//        System.out.println(options.msg);
//        compute(op, 10);
//    }
//
//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc25MinHit10B75K2(TFIDFs options){
//        TFIDFOptions op = options.getOptions();
//        op.setB(0.85);
//        op.setK1(1.8);
//
//        config.setMinDocumentFrequency(25);
//        System.out.println(options.msg);
//        compute(op, 10);
//    }
//
//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc25MinHit10B5K2(TFIDFs options){
//        TFIDFOptions op = options.getOptions();
//        op.setB(0.75);
//        op.setK1(1.8);
//
//        config.setMinDocumentFrequency(25);
//        System.out.println(options.msg);
//        compute(op, 10);
//    }
//
//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc25MinHit10B25K2(TFIDFs options){
//        TFIDFOptions op = options.getOptions();
//        op.setB(0.7);
//        op.setK1(1.8);
//
//        config.setMinDocumentFrequency(25);
//        System.out.println(options.msg);
//        compute(op, 10);
//    }
//
//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc25MinHit10B95(TFIDFs options){
//        TFIDFOptions op = options.getOptions();
//        op.setB(0.95);
//        op.setK1(1.5);
//
//        config.setMinDocumentFrequency(25);
//        System.out.println(options.msg);
//        compute(op, 10);
//    }
//
//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc25MinHit10B75(TFIDFs options){
//        TFIDFOptions op = options.getOptions();
//        op.setB(0.85);
//        op.setK1(1.5);
//
//        config.setMinDocumentFrequency(25);
//        System.out.println(options.msg);
//        compute(op, 10);
//    }
//
//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc25MinHit10B5(TFIDFs options){
//        TFIDFOptions op = options.getOptions();
//        op.setB(0.75);
//        op.setK1(1.5);
//
//        config.setMinDocumentFrequency(25);
//        System.out.println(options.msg);
//        compute(op, 10);
//    }
//
//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc25MinHit10B25(TFIDFs options){
//        TFIDFOptions op = options.getOptions();
//        op.setB(0.7);
//        op.setK1(1.5);
//
//        config.setMinDocumentFrequency(25);
//        System.out.println(options.msg);
//        compute(op, 10);
//    }
//
//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc25MinHit10B95K05(TFIDFs options){
//        TFIDFOptions op = options.getOptions();
//        op.setB(0.95);
//        op.setK1(1.2);
//
//        config.setMinDocumentFrequency(25);
//        System.out.println(options.msg);
//        compute(op, 10);
//    }
//
//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc25MinHit10B75K05(TFIDFs options){
//        TFIDFOptions op = options.getOptions();
//        op.setB(0.85);
//        op.setK1(1.2);
//
//        config.setMinDocumentFrequency(25);
//        System.out.println(options.msg);
//        compute(op, 10);
//    }
//
//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc25MinHit10B5K05(TFIDFs options){
//        TFIDFOptions op = options.getOptions();
//        op.setB(0.75);
//        op.setK1(1.2);
//
//        config.setMinDocumentFrequency(25);
//        System.out.println(options.msg);
//        compute(op, 10);
//    }
//
//    @ParameterizedTest
//    @EnumSource(value = TFIDFs.class)
//    public void testTFIDFOptionsMinDoc25MinHit10B25K05(TFIDFs options){
//        TFIDFOptions op = options.getOptions();
//        op.setB(0.7);
//        op.setK1(1.2);
//
//        config.setMinDocumentFrequency(25);
//        System.out.println(options.msg);
//        compute(op, 10);
//    }

    private static void compute(TFIDFOptions options, int minHitFrequency){
        HashMap<String, List<TFIDFMathElement>> tfidfMath =
                service.mapMathDocsToTFIDFElements(mathDocs, DOCS, options);
        List<TFIDFMathElement> results = service.groupTFIDFElements(tfidfMath, mergeF, minHitFrequency);

        System.out.println(options.getB());
        System.out.println(options.getK1());

        testResults(results, expected);
        System.out.println();
        System.out.println("Top 50 MathML");
        printPretty(results);
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
        BM25_IDF("BM25 * IDF", TermFrequencies.BM25, InverseDocumentFrequencies.IDF),
        RAW_PROP_IDF("RAW * PROP_IDF", TermFrequencies.RAW, InverseDocumentFrequencies.PROP_IDF),
        REL_PROP_IDF("REL * PROP_IDF", TermFrequencies.RELATIVE, InverseDocumentFrequencies.PROP_IDF),
        LOG_PROP_IDF("LOG * PROP_IDF", TermFrequencies.LOG, InverseDocumentFrequencies.PROP_IDF),
        NORM_PROP_IDF("NORM * PROP_IDF", TermFrequencies.NORM, InverseDocumentFrequencies.PROP_IDF),
        BM25_PROP_IDF("BM25 * PROP_IDF", TermFrequencies.BM25, InverseDocumentFrequencies.PROP_IDF),
        RAW_BM25_IDF("RAW w BM25", TermFrequencies.RAW, InverseDocumentFrequencies.BM25_IDF),
        REL_BM25_IDF("REL w BM25", TermFrequencies.RELATIVE, InverseDocumentFrequencies.BM25_IDF),
        LOG_BM25_IDF("REL w BM25", TermFrequencies.LOG, InverseDocumentFrequencies.BM25_IDF),
        NORM_BM25_IDF("NORM w BM25", TermFrequencies.NORM, InverseDocumentFrequencies.BM25_IDF),
        BM25_BM25_IDF("BM25 w BM25", TermFrequencies.BM25, InverseDocumentFrequencies.BM25_IDF);

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

    private static void printPretty(List<TFIDFMathElement> results){
        System.out.println("Pretty Printed Top Results:");
        for (int i = 0; i < 50 && i < results.size(); i++){
            TFIDFMathElement e = results.get(i);
            System.out.println(e.getScore() + ", <math>" + SimpleMMLConverter.stringToMML(e.getExpression()) + "</math>");
        }
    }
}
