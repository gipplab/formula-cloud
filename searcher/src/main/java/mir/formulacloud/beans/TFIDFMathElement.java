package mir.formulacloud.beans;

import mir.formulacloud.util.TFIDFLoader;

/**
 * @author Andre Greiner-Petter
 */
public class TFIDFMathElement extends MathElement {

    private double score;

    public TFIDFMathElement(MathElement parent, double score){
        super(
                parent.getExpression(),
                parent.getDepth(),
                parent.getTotalFrequency(),
                parent.getDocFrequency()
        );
        this.score = score;
    }

    public double getScore() {
        return score;
    }

    @Override
    public String toString(){
        MathElement global = TFIDFLoader.getLoaderInstance().getMathElement(getExpression());
        String out = getExpression();
        out += " -> TFIDF Score: " + getScore();
        out += " [depth: " + getDepth();
        out += ", TF: " + getTotalFrequency();
        out += ", HDF: " + getDocFrequency();
        out += ", GTF: " + global.getTotalFrequency();
        out += ", GDF: " + global.getDocFrequency() + "]";
        return out;
    }
}
