package mir.formulacloud.beans;

/**
 * @author Andre Greiner-Petter
 */
public enum InverseDocumentFrequencies {
    BINARY(     (r,t) -> r > 0 ? 1 : 0),
    IDF(        (r,t) -> Math.log(t/(double)r)),
    PROP_IDF(   (r,t) -> Math.log((t-r)/(double) r)),
    BM25_IDF(   (r,t) -> Math.log((t-r+0.5)/((double)r + 0.5)));

    private IInverseDocumentFrequencyCalculator calculator;

    InverseDocumentFrequencies(IInverseDocumentFrequencyCalculator calculator){
        this.calculator = calculator;
    }

    public double calculate(long raw, long total){
        return calculator.calculate(raw, total);
    }
}
