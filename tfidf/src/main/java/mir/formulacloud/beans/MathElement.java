package mir.formulacloud.beans;

/**
 * @author Andre Greiner-Petter
 */
public class MathElement {
    public int totalFrequency;
    public int docFrequency;

    public String expression;
    public int depth;

    public MathElement(){
        this.expression = "";
        this.depth = 0;
        this.totalFrequency = 0;
        this.docFrequency = 0;
    }

    public MathElement(String expression, int depth, int totalFrequency, int docFrequency){
        this.expression = expression;
        this.depth = depth;
        this.totalFrequency = totalFrequency;
        this.docFrequency = docFrequency;
    }

    public int getTotalFrequency() {
        return totalFrequency;
    }

    public int getDocFrequency() {
        return docFrequency;
    }

    public int getDepth() {
        return depth;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public void setDepth(int depth) {
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
        return this;
    }

    @Override
    public String toString(){
        return String.join(";", '"'+expression+'"', ""+depth, ""+totalFrequency, ""+docFrequency);
    }
}
