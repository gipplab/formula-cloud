package mir.formulacloud.util;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URL;

/**
 * @author Andre Greiner-Petter
 */
public class XQueryLoader {
    private static final Logger LOG = LogManager.getLogger(XQueryLoader.class.getName());

    private static final String FNAME_PATTERN = "$$DOCID$$";
    private static final String COLNAME_PATTERN = "$$DATACOL$$";
    private static final String MINFRQ_PATTERN = "$$MINFREQ$$";
    private static final String LIST_PATTERN = "$$LIST$$";

    private static String script, zbScript;

    private static final String NS_DECLARE =
            "declare namespace mws = \"http://search.mathweb.org/ns\";\n" +
            "declare default element namespace \"http://www.w3.org/1998/Math/MathML\";\n\n";

    private static final String ID_PREFIX = "@data-doc-id=";

    private static String DOC_PRE_CALLER =
            "declare variable $docid := \"$$DOCID$$\";\n" +
            "declare variable $minDocFreq := $$MINFREQ$$;\n\n";

    private static String DOC_PRE_COLLECTION_CALLER =
            "declare variable $dataCollection := \"$$DATACOL$$\";\n" +
            "declare variable $minDocFreq := $$MINFREQ$$;\n\n";

    private static String DOC_PRE_ID_CALLER =
            "declare variable $docID := \"$$DOCID$$\";\n";

    private static String POST_CALLER =
            "declare variable $doc := /mws:harvest[@data-doc-id=$docid];\n" +
            "if ($doc/*) then\n" +
            "  local:extractTerms($doc[1], $minDocFreq)";

    private static String POST_SET_CALLER =
            "declare variable $minDocFreq := $$MINFREQ$$;\n" +
            "declare variable $doc := /mws:mir.formulacloud.harvest[$$LIST$$];\n" +
                    "if ($doc/*) then\n" +
                    "  local:extractTerms($doc[1], $minDocFreq)";

    private static String IDENTIFY_EMPTY_DOC = NS_DECLARE +
            "mws:harvest[not(descendant::mi)]/@data-doc-id/string()";

    private static String TRIGGER = "\n" +
            "declare variable $docs := /mws:harvest[@data-collection=$dataCollection];\n" +
            "local:extractTerms($docs, $minDocFreq)";

    private static String TRIGGER_SINGLE_DOC = "\n" +
            "declare variable $docs := /mws:harvest[@data-collection=$dataCollection]/mws:expr[contains(@url, $docID)]/..;\n" +
            "local:extractTerms($docs, $minDocFreq)";

    static {
        try {
            script = getTermExtractorScript("termExtractor.xq");
            zbScript = getTermExtractorScript("ZBTermExtractor.xq");
        } catch (Exception e){
            System.err.println("Cannot read resource!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static String getTermExtractorScript(String filename) throws IOException {
        URL url = Resources.getResource(filename);
        return Resources.toString(url, Charsets.UTF_8);
    }

    public static void initMinTermFrequency(int minTermFrequency){
        DOC_PRE_CALLER = DOC_PRE_CALLER.replace(MINFRQ_PATTERN, ""+minTermFrequency);
        POST_SET_CALLER = POST_SET_CALLER.replace(MINFRQ_PATTERN, ""+minTermFrequency);
        DOC_PRE_COLLECTION_CALLER = DOC_PRE_COLLECTION_CALLER.replace(MINFRQ_PATTERN, ""+minTermFrequency);
    }

    public static String getScript(String docID){
        String caller = DOC_PRE_CALLER.replace(FNAME_PATTERN, docID);
        return NS_DECLARE + caller + script + POST_CALLER;
    }

    public static String getScript(String... docIDs){
        String command = ID_PREFIX + '"' + docIDs[0] + '"';
        for ( int i = 1; i < docIDs.length; i++ ){
            command = command + " or " + ID_PREFIX + '"' + docIDs[i] + '"';
        }
        return NS_DECLARE + script + POST_CALLER.replace(LIST_PATTERN, command);
    }

    public static String getZBScript(String collection){
        String caller = DOC_PRE_COLLECTION_CALLER.replace(COLNAME_PATTERN, collection);
        return NS_DECLARE + caller + zbScript + TRIGGER;
    }

    public static String getZBScriptForSingleDoc(String collection, String docID){
        String caller = DOC_PRE_COLLECTION_CALLER.replace(COLNAME_PATTERN, collection);
        caller += DOC_PRE_ID_CALLER.replace(FNAME_PATTERN, docID);
        return NS_DECLARE + caller + zbScript + TRIGGER_SINGLE_DOC;
    }

    public static String getIdentifyEmptyDocIDScript() {
        return IDENTIFY_EMPTY_DOC;
    }
}
