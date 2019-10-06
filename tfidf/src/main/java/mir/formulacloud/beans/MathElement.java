package mir.formulacloud.beans;

import mir.formulacloud.tfidf.TFIDFCalculator;

/**
 * @author Andre Greiner-Petter
 */
public class MathElement {
    private String expression;

    private int totalFrequency;
    private int docFrequency;
    private short depth;

    private boolean isStopper = false;

    public MathElement(){
        this.expression = "";
        this.depth = 0;
        this.totalFrequency = 0;
        this.docFrequency = 0;
    }

    public MathElement(String expression, short depth, int totalFrequency, int docFrequency){
        this.expression = expression;
        this.depth = depth;
        this.totalFrequency = totalFrequency;
        this.docFrequency = docFrequency;
    }

    public void markAsStopper(){
        this.isStopper = true;
    }

    public boolean isStopper(){
        return this.isStopper;
    }

    public int getTotalFrequency() {
        return totalFrequency;
    }

    public int getDocFrequency() {
        return docFrequency;
    }

    public short getDepth() {
        return depth;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public void setDepth(short depth) {
        this.depth = depth;
    }

    public void setTotalFrequency(int totalFrequency) {
        this.totalFrequency = totalFrequency;
    }

    public void setDocFrequency(int docFrequency) {
        this.docFrequency = docFrequency;
    }

    public MathElement add(MathElement reference) throws IllegalArgumentException {
        this.totalFrequency += reference.totalFrequency;
        this.docFrequency += reference.docFrequency;
        TFIDFCalculator.updateMerger();
        return this;
    }

    @Override
    public String toString(){
        return String.join(";", '"'+expression+'"', ""+depth, ""+totalFrequency, ""+docFrequency);
    }
}
