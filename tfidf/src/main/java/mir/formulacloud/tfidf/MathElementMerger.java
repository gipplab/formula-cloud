package mir.formulacloud.tfidf;

import mir.formulacloud.beans.MathElement;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * @author Andre Greiner-Petter
 */
public class MathElementMerger implements ReduceFunction<MathElement> {
    private static final Logger LOG = LogManager.getLogger(MathElementMerger.class.getName());

    @Override
    public MathElement reduce(MathElement mathElement, MathElement t1) throws Exception {
        return mathElement.add(t1);
    }
}
