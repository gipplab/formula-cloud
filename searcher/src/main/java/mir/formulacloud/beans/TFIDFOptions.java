package mir.formulacloud.beans;

/**
 * @author Andre Greiner-Petter
 */
public class TFIDFOptions {
    private final TermFrequencies tfOption;
    private final InverseDocumentFrequencies idfOption;

    private double k1 = 1.2;
    private double b = 0.95;

    public TFIDFOptions(TermFrequencies tf, InverseDocumentFrequencies idf){
        this.tfOption = tf;
        this.idfOption = idf;
    }

    public double getK1() {
        return k1;
    }

    public void setK1(double k1) {
        this.k1 = k1;
    }

    public double getB() {
        return b;
    }

    public void setB(double b) {
        this.b = b;
    }

    public TermFrequencies getTfOption() {
        return tfOption;
    }

    public InverseDocumentFrequencies getIdfOption() {
        return idfOption;
    }

    public static TFIDFOptions getDefaultTFIDFOption(){
        return new TFIDFOptions(TermFrequencies.RELATIVE, InverseDocumentFrequencies.IDF);
    }
}
