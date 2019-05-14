package mir.formulacloud.beans;

import mir.formulacloud.harvest.ArxivSets;
import mir.formulacloud.harvest.ArxivUtils;

import java.util.LinkedList;

/**
 * @author Andre Greiner-Petter
 */
public class DocBean {

    public static String NL = System.lineSeparator();
    public static String INDENT = "  ";

    private static final String ROOT_START =
//            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + NL +
                    "<mws:mir.formulacloud.harvest " + NL +
                    INDENT + INDENT + "xmlns:mws=\"http://search.mathweb.org/ns\" " + NL +
                    INDENT + INDENT + "data-set=\"arxiv\"" + NL +
                    INDENT + INDENT + "data-doc-id=\"%s\"" + NL +
                    INDENT + INDENT + "data-major-collection=\"%s\"" + NL +
                    INDENT + INDENT + "data-minor-collection=\"%s\"" + NL +
                    INDENT + INDENT + "data-finer-collection=\"%s\">" + NL;

    private static final String ROOT_END = "</mws:mir.formulacloud.harvest>";

    private static final String ARXIV_BASE_URL = "https://arxiv.org/abs/";

    private String id, major, minor, fine;
    private LinkedList<String> mmlList;

    private ArxivSets arxivSet;

    public DocBean(){
        mmlList = new LinkedList<>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setMajor(String major) {
        this.major = major;
    }

    public void setMinor(String minor) {
        this.minor = minor;
    }

    public void setFine(String fine) {
        this.fine = fine;
    }

    public ArxivSets getArxivSet() {
        return arxivSet;
    }

    public void setArxivSet(ArxivSets arxivSet) {
        this.arxivSet = arxivSet;
    }

    public void addMMLExpression(String mml) {
        mmlList.add(mml);
    }

    private String buildSingleExpression(String mml, int num){
        StringBuilder sb = new StringBuilder("<mws:expr url=\"");
        sb.append(ARXIV_BASE_URL)
                .append(ArxivUtils.convertFileNameToURLEntity(id))
                .append("#")
                .append(num)
                .append("\">")
                .append(mml)
                .append("</mws:expr>");
        return sb.toString();
    }

    private String buildExprList() {
        StringBuilder sb = new StringBuilder();
        int counter = 1;
        for (String mml : mmlList){
            sb.append(INDENT);
            sb.append(buildSingleExpression(mml, counter));
            sb.append(NL);
            counter++;
        }
        return sb.toString();
    }

    public String getDocAsString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(ROOT_START, id, major, minor, fine));
        sb.append(buildExprList());
        sb.append(ROOT_END);
        return sb.toString();
    }
}
