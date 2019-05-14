package mir.formulacloud.harvest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * @author Andre Greiner-Petter
 */
public enum ArxivSets {
    cs("cs", "Computer Science", true),
    econ("econ", "Economics", true),
    eess("eess", "Electrical Engineering and Systems Science", true),
    math("math", "Mathematics", true),
    physics("physics", "Physics", true),
    physics_astro("physics:astro-ph", "Astrophysics", false),
    physics_cond("physics:cond-mat", "Condensed Matter", false),
    physics_gr("physics:gr-qc", "General Relativity and Quantum Cosmology", false),
    physics_hep_ex("physics:hep-ex", "High Energy Physics - Experiments", false),
    physics_hep_lat("physics:hep-lat", "High Energy Physics - Lattice", false),
    physics_hep_ph("physics:hep-ph", "High Energy Physics - Phenomenology", false),
    physics_hep_th("physics:hep-th", "High Energy Physics - Theory", false),
    physics_math_ph("physics:math-ph", "Mathematical Physics", false),
    physics_nlin("physics:nlin", "Nonlinear Sciences", false),
    physics_nucl_ex("physics:nucl-ex", "Nuclear Experiment", false),
    physics_nucl_th("physics:nucl-th", "Nuclear Theory", false),
    physics_physics("physics:physics", "Physics (Other)", false),
    physics_quant_ph("physics:quant-ph", "Quantum Physics", false),
    q_bio("q-bio", "Quantitative Biology", true),
    q_fin("q-fin", "Quantitative Finance", true),
    stat("stat", "Statistics", true),
    UNKNOWN("unknown", "Unknown", true);

    private String id, name, shortid;
    private boolean majorCollection;

    private static final HashMap<String, ArxivSets> innerMap = new HashMap<>();

    static {
        for ( ArxivSets set : ArxivSets.values() ){
            innerMap.put(set.shortid, set);
        }
    }

    ArxivSets(String id, String name, boolean majorCollection){
        this.id = id;
        this.name = name;
        this.majorCollection = majorCollection;
        this.shortid = id.split(":").length > 1 ? id.split(":")[1] : id;
    }

    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    public String getShortID(){
        return shortid;
    }

    public static ArxivSets getArxivSet(String shortID){
        ArxivSets as = innerMap.get(shortID);
        return as == null ? UNKNOWN : as;
    }

    public static LinkedList<String> getMajorSubjects(String collections){
        String[] collectionsArr = collections.split(" ");
        LinkedList<String> majors = new LinkedList<>();
        for ( String s : collectionsArr ){
            String major = s.split("\\.")[0];
            if ( !majors.contains(major) ) majors.add(major);
        }
        return majors;
    }

    public static ArrayList<ArxivSets> getMajorCollectionSet(){
        ArrayList<ArxivSets> list = new ArrayList<>();
        for ( ArxivSets as : ArxivSets.values() )
            if (as.majorCollection) list.add(as);
        return list;
    }
}
