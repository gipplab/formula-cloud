package mir.formulacloud.beans;

/**
 * @author Andre Greiner-Petter
 */
public interface ITermFrequencyCalculator {
    double calculate(long raw, long total);
}
