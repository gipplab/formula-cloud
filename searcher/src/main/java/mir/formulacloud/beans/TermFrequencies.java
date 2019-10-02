package mir.formulacloud.beans;

/**
 * @author Andre Greiner-Petter
 */
public enum TermFrequencies {
    BINARY(     (r,t) -> r > 0 ? 1 : 0),
    RAW(        (r,t) -> r),
    RELATIVE(   (r,t) -> r/(double)t),
    LOG(        (r,t) -> Math.log(1+r)),
    NORM(       (r,t) -> 0.5+0.5*(r/(double)t)), // note that t isn't total but max
    BM25(       (r,t) ->
            r * (TermFrequencies.k1+1)
                    / (r + TermFrequencies.k1*(
                            1-TermFrequencies.b + TermFrequencies.b*t/MathDocument.AVGDL
                    ))
    );

    private static final double k1 = 1;
    private static final double b = 0.75;

    private ITermFrequencyCalculator calculator;

    TermFrequencies(ITermFrequencyCalculator calculator){
        this.calculator = calculator;
    }

    public double calculate(long raw, long total){
        return this.calculator.calculate(raw, total);
    }
}
