package mir.formulacloud.beans;

import java.util.Arrays;

/**
 * @author Andre Greiner-Petter
 */
public enum MathMergeFunctions {
    AVG((a) -> Arrays.stream(a).average().getAsDouble()),
    MAX((a) -> Arrays.stream(a).max().getAsDouble()),
    MIN((a) -> Arrays.stream(a).min().getAsDouble());

    private IMathMergeFunction mergeFunction;

    MathMergeFunctions(IMathMergeFunction mergeFunction) {
        this.mergeFunction = mergeFunction;
    }

    public double calculate(double... a) {
        return mergeFunction.calculate(a);
    }
}
