package mir.formulacloud.searcher;

import mir.formulacloud.beans.*;
import mir.formulacloud.tfidf.BaseXController;
import mir.formulacloud.util.Helper;
import mir.formulacloud.util.SimpleMMLConverter;
import mir.formulacloud.util.XQueryLoader;
import org.junit.Before;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Andre Greiner-Petter
 */
public class ZentralBlattTests {

    private static final int DOCS = 135914;
    private static final String DB = "harvest";

    private static SearcherConfig config;
    private static SearcherService service;
    private static List<MathDocument> mathDocs;

    private static final String collection = "Riemann zeta";
    private static final String expr = ".*mrow\\(mi:ζ,mo:ivt,mrow\\(mo:\\(,.*,mo:\\)\\)\\).*";
//    private static final String expr = "mrow(mo:(,msub(mi:λ,mn:1),mo:,,mi:…,mo:,,msub(mi:λ,mi:n),mo:))";


    @BeforeAll
    public static void init(){
        XQueryLoader.initMinTermFrequency(10);

        config = new SearcherConfig();
        config.setTfidfData("/opt/zbmath/tfidf");
        config.setMinDocumentFrequency(3);

        service = new SearcherService(config);
        service.initBaseXServers(DB);
        service.initTFIDFTables();

        mathDocs = new LinkedList<>();

        addMathDoc(collection);

        mathDocs = service.requestMath(mathDocs);
    }

    @Test
    public void relIDFTest(){
        System.out.println("Test RELATIVE * IDF");
        TFIDFOptions options = new TFIDFOptions(
                TermFrequencies.RELATIVE, InverseDocumentFrequencies.IDF
        );
        compute(options);
    }

    @Test
    public void rawIDFTest(){
        System.out.println("Test RAW * IDF");
        TFIDFOptions options = new TFIDFOptions(
                TermFrequencies.RAW, InverseDocumentFrequencies.IDF
        );
        compute(options);
    }

    @Test
    public void logIDFTest(){
        System.out.println("Test LOG * IDF");
        TFIDFOptions options = new TFIDFOptions(
                TermFrequencies.LOG, InverseDocumentFrequencies.IDF
        );
        compute(options);
    }

    @Test
    public void normIDFTest(){
        System.out.println("Test NORM * IDF");
        TFIDFOptions options = new TFIDFOptions(
                TermFrequencies.NORM, InverseDocumentFrequencies.IDF
        );
        compute(options);
    }

    @Test
    public void relPROPIDFTest(){
        System.out.println("Test RELATIVE * PROP_IDF");
        TFIDFOptions options = new TFIDFOptions(
                TermFrequencies.RELATIVE, InverseDocumentFrequencies.PROP_IDF
        );
        compute(options);
    }

    @Test
    public void rawPROPIDFTest(){
        System.out.println("Test RAW * PROP_IDF");
        TFIDFOptions options = new TFIDFOptions(
                TermFrequencies.RAW, InverseDocumentFrequencies.PROP_IDF
        );
        compute(options);
    }

    @Test
    public void logPROPIDFTest(){
        System.out.println("Test LOG * PROP_IDF");
        TFIDFOptions options = new TFIDFOptions(
                TermFrequencies.LOG, InverseDocumentFrequencies.PROP_IDF
        );
        compute(options);
    }

    @Test
    public void normPROPIDFTest(){
        System.out.println("Test NORM * PROP_IDF");
        TFIDFOptions options = new TFIDFOptions(
                TermFrequencies.NORM, InverseDocumentFrequencies.PROP_IDF
        );
        compute(options);
    }


    private static void compute(TFIDFOptions options){
        HashMap<String, List<TFIDFMathElement>> elements =
                service.mapMathDocsToTFIDFElements(mathDocs, DOCS, options);

        List<TFIDFMathElement> results = service.groupTFIDFElements(elements, MathMergeFunctions.MAX);

        ArxivTests.testResults(results, expr);

        System.out.println();
        printPretty(results);
    }

    private static void printPretty(List<TFIDFMathElement> results){
        System.out.println("Pretty Printed Top Results:");
        for (int i = 0; i < 50; i++){
            TFIDFMathElement e = results.get(i);
            System.out.println(e.getScore() + "," + SimpleMMLConverter.stringToMML(e.getExpression()));
        }
    }

    private static void addMathDoc(String collection){
        MathDocument md = new MathDocument(collection, DB);
        mathDocs.add(md);
    }

    @AfterAll
    public static void end(){
        BaseXController.closeAllClients();
    }



}
