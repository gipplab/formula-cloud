package mir.formulacloud.beans;

import mir.formulacloud.harvest.ArxivSets;

/**
 * @author Andre Greiner-Petter
 */
public class MathCollectionBean {

    private ArxivSets subject;
    private String content;

    public MathCollectionBean(ArxivSets subject, String content){
        this.subject = subject;
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    public ArxivSets getSubject() {
        return subject;
    }
}
