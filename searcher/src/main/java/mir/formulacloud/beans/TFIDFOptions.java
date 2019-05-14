package mir.formulacloud.beans;

/**
 * @author Andre Greiner-Petter
 */
public class TFIDFOptions {
    private final TermFrequencies tfOption;
    private final InverseDocumentFrequencies idfOption;

    public TFIDFOptions(TermFrequencies tf, InverseDocumentFrequencies idf){
        this.tfOption = tf;
        this.idfOption = idf;
    }

    public TermFrequencies getTfOption() {
        return tfOption;
    }

    public InverseDocumentFrequencies getIdfOption() {
        return idfOption;
    }

    public static TFIDFOptions getDefaultTFIDFOption(){
        return new TFIDFOptions(TermFrequencies.NORM, InverseDocumentFrequencies.IDF);
    }
}
