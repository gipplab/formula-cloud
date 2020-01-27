package mir.formulacloud.searcher;

import mir.formulacloud.beans.MathDocument;
import mir.formulacloud.beans.MathMergeFunctions;
import mir.formulacloud.beans.TFIDFMathElement;
import org.elasticsearch.search.SearchHits;

import java.util.HashMap;
import java.util.List;

/**
 * @author Andre Greiner-Petter
 */
public class OpenAccessSearcher extends SearcherService {
    private SearcherConfig config;
    private int minimumESHits = 1;
    private int resultLimit = 10;

    public OpenAccessSearcher(SearcherConfig config) {
        super(config);
        this.config = config;
        if ( config.getFixedIndex().equals("zbmath") )
            MathDocument.setZBMATHMode();
    }

    public void init() {
        super.init();
    }

    public String search(String searchQuery) {
        String index = config.getFixedIndex();
        int numberOfDocs = getNumberOfDocuments(config.getFixedIndex());

        // first, get the hits
        SearchHits hits = getSearchResults(searchQuery, index);

        // convert to math documents and get math
        List<MathDocument> mathDocs = getMathResults(hits);
        mathDocs = requestMath(mathDocs);

        HashMap<String, List<TFIDFMathElement>> tfidfMath =
                mapMathDocsToTFIDFElements(mathDocs, numberOfDocs);
        List<TFIDFMathElement> results =
                groupTFIDFElements(tfidfMath, MathMergeFunctions.MAX, minimumESHits);

        StringBuilder sb = new StringBuilder("Results (top-").append(resultLimit).append("):\n");
        for ( int i = 0; i < results.size() && i < resultLimit; i++ ) {
            sb.append(results.get(i)).append("\n");
        }
        return sb.toString();
    }
}
