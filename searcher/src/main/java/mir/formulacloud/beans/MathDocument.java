package mir.formulacloud.beans;

import com.formulasearchengine.mathosphere.basex.BaseXClient;
import mir.formulacloud.searcher.SearcherConfig;
import mir.formulacloud.tfidf.BaseXController;
import mir.formulacloud.tfidf.BaseXRequestMapper;
import mir.formulacloud.util.Constants;
import mir.formulacloud.util.TFIDFLoader;
import mir.formulacloud.util.XQueryLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.regex.Matcher;

/**
 * @author Andre Greiner-Petter
 */
public class MathDocument {
    public static int counter = 0;

    private static final Logger LOG = LogManager.getLogger(MathDocument.class.getName());

    public static final String F_ID = "title";
    public static final String F_DB = "database";
    public static final String F_URL = "arxiv";

    private String docID;
    private String basexDB;
    private String arxivURL;

    private String xQuery;

    private int documentLength = 0;

    private HashMap<String, MathElement> mathElements;

    private ArrayList<Integer> maxCountPerDepthTable;
    private int maxComplexity = 0;
    private double avgComplexity = 0;

    private double esSearchPrecision;

    public static final int ARXIV_DOCS   =   841_008;
    public static final int ZBMATH_DOCS  = 1_349_297;

    private static final int ARXIV_MATH  = 2_080_634_554;
    private static final int ZBMATH_MATH =    61_355_307;

    public static final double ARXIV_AVGC  = 4.59;
    public static final double ZBMATH_AVGC = 4.89;

    public static final double ARXIV_AVGDL  = ARXIV_MATH / (double) ARXIV_DOCS;
    public static final double ZBMATH_AVGDL = ZBMATH_MATH / (double) ZBMATH_DOCS;

    private static double AVGDL = ARXIV_AVGDL;
    private static double AVGC  = ARXIV_AVGC;

    public MathDocument(String docID, String basexDB, double elasticsearchPrecision){
        this.docID = docID;
        this.basexDB = basexDB;
        this.esSearchPrecision = elasticsearchPrecision;
        this.mathElements = new HashMap<>();
        this.xQuery = "XQUERY " + XQueryLoader.getScript(docID);
        this.maxCountPerDepthTable = new ArrayList<>();
    }

    public MathDocument(String collection, String basexDB){
        this.docID = collection;
        this.basexDB = basexDB;
        this.esSearchPrecision = 0;
        this.mathElements = new HashMap<>();
        this.xQuery = "XQUERY " + XQueryLoader.getZBScript(collection);
        this.maxCountPerDepthTable = new ArrayList<>();
    }

    public MathDocument(String collection, String basexDB, String docID){
        this.docID = docID;
        this.basexDB = basexDB;
        this.esSearchPrecision = 0;
        this.mathElements = new HashMap<>();
        this.xQuery = "XQuery " + XQueryLoader.getZBScriptForSingleDoc(collection, docID);
        this.maxCountPerDepthTable = new ArrayList<>();
    }

    public static void setArxivMode(){
        AVGDL = ARXIV_AVGDL;
        AVGC  = ARXIV_AVGC;
    }

    public static void setZBMATHMode(){
        AVGDL = ZBMATH_AVGDL;
        AVGC  = ZBMATH_AVGC;
    }

    public static double getCurrentAVGDL(){
        return AVGDL;
    }

    public static double getCurrentAVGC() {
        return AVGC;
    }

    public String getDocID() {
        return docID;
    }

    public String getBasexDB() {
        return basexDB;
    }

    public String getArxivURL() {
        return arxivURL;
    }

    public void setArxivURL(String arxivURL) {
        this.arxivURL = arxivURL;
    }

    public double getEsSearchPrecision() {
        return esSearchPrecision;
    }

    public void requestMathFromBasex(SearcherConfig config){
        this.mathElements = new HashMap<>();

        if (basexDB == null || basexDB.isEmpty()){
            // this document dont have math... only text
            LOG.debug("Cannot receive math for document " + docID + " because it doesn't have math.");
            counter++;
//            System.out.print("\r"+counter);
            return;
        }

        BaseXClient client = BaseXController.getBaseXClientByDatabase(basexDB);

        long start = System.currentTimeMillis(); // measure time elapsed
        try {
            String results = client.execute(xQuery);

            long stop = System.currentTimeMillis() - start;
            LOG.debug("Received results from BaseX [" + docID + "]. Time Elapsed: " + stop + "ms");

            BaseXController.returnBaseXClientByDatabase(basexDB, client);
            // don't use client anymore!
            client = null;

            int minD = config.getMinDepth();
            Matcher matcher = Constants.BASEX_ELEMENT_PATTERN.matcher(results);

            while(matcher.find()){
                MathElement element = new MathElement(
                        matcher.group(Constants.BX_IDX_EXPR),
                        Short.parseShort(matcher.group(Constants.BX_IDX_DEPTH)),
                        Integer.parseInt(matcher.group(Constants.BX_IDX_FREQ)),
                        1
                );

                this.documentLength += element.getTotalFrequency();
                this.avgComplexity += element.getDepth();

                if ( element.getDepth() >= minD ){
                    int d = element.getDepth();
                    while ( maxCountPerDepthTable.size() < d ){
                        maxCountPerDepthTable.add(0);
                    }

                    if ( maxComplexity < d ){
                        maxComplexity = d;
                    }

//                    maxCountPerDepthTable.set(
//                            d-1,
//                            maxCountPerDepthTable.get(d-1) + element.getTotalFrequency()
//                    );

                    if ( maxCountPerDepthTable.get(d-1) < element.getTotalFrequency() ){
                        maxCountPerDepthTable.set(d-1, (int)element.getTotalFrequency());
                    }

                    this.mathElements.put(element.getExpression(), element);
                }
            }

            this.avgComplexity = this.avgComplexity / (double)this.documentLength;

            LOG.info("Finished requests for document " + docID + " [math elements: " + mathElements.size() + "]");
            counter++;
//            System.out.print("\r"+counter);
        } catch (IOException e) {
            LOG.error("Not able to receive math from BaseX for Document " + docID, e);
            counter++;
//            System.out.print("\r"+counter);
        }
    }

    public HashMap<String, TFIDFMathElement> getDocumentTFIDF(int totalDocs, int minDocFrq, int maxDocFrq, SearcherConfig config){
        return getDocumentTFIDF(totalDocs, minDocFrq, maxDocFrq, TFIDFOptions.getDefaultTFIDFOption(), config);
    }

    public HashMap<String, TFIDFMathElement> getDocumentTFIDF(int totalDocs, int minDocFrq, int maxDocFrq, TFIDFOptions options, SearcherConfig config){
        if (mathElements == null){
            LOG.warn("Requested document TF-IDF values but did not request math from BaseX yet. Invoke requestMathFromBasex() first.");
            requestMathFromBasex(config);
        }

        if (mathElements.isEmpty()){
            // might be an empty document... so just return 0
            return new HashMap<>();
        }

        TFIDFLoader tfidfReg = TFIDFLoader.getLoaderInstance();
        TermFrequencies tfSetting = options.getTfOption();
        InverseDocumentFrequencies idfSetting = options.getIdfOption();

        // the total number of math elements in this document or max number of math of one type
        int total = tfSetting.equals(TermFrequencies.NORM) ?
                (int)getMaxFrequency(mathElements).getTotalFrequency() :
                getSumOfFrequencies(mathElements);

        boolean bm25 = false;
        if ( tfSetting.equals(TermFrequencies.BM25) ){
            bm25 = true;
        }

        HashMap<String, TFIDFMathElement> tfidfElements = new HashMap<>();
        for ( MathElement docMathElement : mathElements.values() ){
            MathElement tfidfReference = tfidfReg.getMathElement(docMathElement.getExpression());

            if ( tfidfReference == null ){
//                LOG.warn("Not able to find frequencies value for " + docMathElement.getExpression());
                continue;
            }

            // test minimum document frequency
            if ( tfidfReference.getDocFrequency() < minDocFrq || maxDocFrq < tfidfReference.getDocFrequency() )
                continue; // skip this entry

            // calculate TF-IDF
            // TF: raw(term,doc) and total=NumOfElements in Doc
            // DF: in how many docs it appear and total number of docs
//            int totalPerDepth = total;
//            try {
//                totalPerDepth = maxCountPerDepthTable.get(docMathElement.getDepth()-1);
//            } catch (Exception e){}

            double tf = tfSetting.calculate(docMathElement.getTotalFrequency(), total);

//            if ( bm25 ) {
//                total = documentLength;
//            }


            if (bm25){
                double k = options.getK1();
                double b = options.getB();

                total = maxCountPerDepthTable.get(docMathElement.getDepth()-1);
                tf = (docMathElement.getTotalFrequency() * (k + 1)) / (total + k*(1-b+b*((AVGDL)/(this.documentLength*this.avgComplexity))));
//                tf = (docMathElement.getTotalFrequency() * (k + 1)) / (total + k*(1-b+b*(AVGDL/this.documentLength)));

                double innerIDF = idfSetting.calculate(docMathElement.getTotalFrequency(), this.documentLength);
                tf *= innerIDF;
            }

            double idf = idfSetting.calculate(tfidfReference.getDocFrequency(), totalDocs);


            TFIDFMathElement e = new TFIDFMathElement(
                    docMathElement, tf*idf
            );
            tfidfElements.put(e.getExpression(), e);
        }

        return tfidfElements;
    }

    public static MathElement getMaxFrequency(HashMap<String, MathElement> elements){
        MathElement max = null;
        for ( MathElement e : elements.values() ){
            if ( max == null || max.getTotalFrequency() < e.getTotalFrequency() ){
                max = e;
            }
        }
        return max;
    }

    public static int getSumOfFrequencies(HashMap<String, MathElement> elements){
        int sum = 0;
        for ( MathElement e : elements.values() )
            sum += e.getTotalFrequency();
        return sum;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(docID).append("-[").append(basexDB).append(", ").append(esSearchPrecision);
        if (arxivURL != null)
            sb.append(", ").append(arxivURL);
        sb.append("]: Math Elements ");
        if (mathElements != null)
            sb.append(mathElements.size());
        return sb.toString();
    }
}
