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
import java.util.HashMap;
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

    private HashMap<String, MathElement> mathElements;

    private double esSearchPrecision;

    public MathDocument(String docID, String basexDB, double elasticsearchPrecision){
        this.docID = docID;
        this.basexDB = basexDB;
        this.esSearchPrecision = elasticsearchPrecision;
        this.mathElements = new HashMap<>();
        this.xQuery = "XQUERY " + XQueryLoader.getScript(docID);
    }

    public MathDocument(String collection, String basexDB){
        this.docID = collection;
        this.basexDB = basexDB;
        this.esSearchPrecision = 0;
        this.mathElements = new HashMap<>();
        this.xQuery = "XQUERY " + XQueryLoader.getZBScript(collection);
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
            System.out.print("\r"+counter);
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
                        matcher.group(3),
                        Integer.parseInt(matcher.group(2)),
                        Integer.parseInt(matcher.group(1)),
                        1
                );

                if ( element.getDepth() >= minD )
                    this.mathElements.put(element.expression, element);
            }

            LOG.info("Finished requests for document " + docID + " [math elements: " + mathElements.size() + "]");
            counter++;
            System.out.print("\r"+counter);
        } catch (IOException e) {
            LOG.error("Not able to receive math from BaseX for Document " + docID, e);
            counter++;
            System.out.print("\r"+counter);
        }
    }

    public HashMap<String, TFIDFMathElement> getDocumentTFIDF(long totalDocs, long minDocFrq, long maxDocFrq, SearcherConfig config){
        return getDocumentTFIDF(totalDocs, minDocFrq, maxDocFrq, TFIDFOptions.getDefaultTFIDFOption(), config);
    }

    public HashMap<String, TFIDFMathElement> getDocumentTFIDF(long totalDocs, long minDocFrq, long maxDocFrq, TFIDFOptions options, SearcherConfig config){
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
        long total = tfSetting.equals(TermFrequencies.NORM) ?
                getMaxFrequency(mathElements).getTotalFrequency() :
                getSumOfFrequencies(mathElements);

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
            double tf = tfSetting.calculate(docMathElement.getTotalFrequency(), total);
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

    public static long getSumOfFrequencies(HashMap<String, MathElement> elements){
        long sum = 0;
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
