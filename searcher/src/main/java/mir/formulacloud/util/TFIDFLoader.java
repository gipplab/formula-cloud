package mir.formulacloud.util;

import com.formulasearchengine.mathosphere.basex.BaseXClient;
import mir.formulacloud.beans.MathElement;
import mir.formulacloud.searcher.FastTest;
import mir.formulacloud.tfidf.BaseXController;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.basex.query.func.math.MathE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * @author Andre Greiner-Petter
 */
public class TFIDFLoader {
    private static final Logger LOG = LogManager.getLogger(TFIDFLoader.class.getName());

    private static final String CMD =
            "XQUERY declare variable $node := /*:harvest/*:expr[@string=\"#SEARCH#\"];\n" +
            "(\n" +
            "  data($node/@complexity),\n" +
            "  data($node/@term-frequency),\n" +
            "  data($node/@document-frequency)\n" +
            ")";

    private static final String CMD_GROUP =
            "XQUERY declare variable $searchElements := (#SEARCH-LIST#);\n" +
                    "for $a in $searchElements\n" +
                    "    let $node := /*:harvest/*:expr[@string=$a]\n" +
                    "    return <m><c>{data($node/@complexity)}</c><tf>{data($node/@term-frequency)}</tf><df>{data($node/@document-frequency)}</df></m>";

    private volatile HashMap<String, MathElement> memory;

    public static final Pattern HIT_PATTERN = Pattern.compile(
            "<c>(\\d+?)</c>.*?<tf>(\\d+?)</tf>.*?<df>(\\d+?)</df>|<c/>.*?<tf/>.*?<df/>",
            Pattern.DOTALL
    );

    private BaseXClient client;

    private TFIDFLoader(){
//        memory = new HashMap<>(350_206_974, 0.95f);
        memory = new HashMap<>(65_000_000, 0.95f);
    }

    private void load(Path path) {
        try{
            LOG.debug("Load TF-IDF file " + path.toString());
            Files.lines(path)
//                    .parallel()
                    .map(l -> l.split(";(?=(\"[^\"]*\")*[^\"]*$)"))
                    .map(TFIDFLoader::stripParentheses)
                    .map(a -> {
                        try {
                            return new MathElement(
                                    a[0],
                                    Short.parseShort(a[1]),
                                    Integer.parseInt(a[2]),
                                    Integer.parseInt(a[3])
                            );
                        } catch (NumberFormatException nfe){
                            System.out.println("Error parsing line: " + Arrays.toString(a));
                            return null;
                        }
                    })
                    .forEach(e -> {
                        memory.put(e.getExpression(), e);
//                        System.out.print("\r" + memory.size());
                    });
            double heapSize = Runtime.getRuntime().totalMemory()/Math.pow(1024,2);
            LOG.info("Loaded TF-IDF math elements from " + path.toString() + " [#" + memory.size() + "; Mem: "+heapSize+" MB]");
        } catch (IOException ioe){
            ioe.printStackTrace();
        }
    }

    private int counter = 0;

    public MathElement getMathElement(String expression){
//        counter++;
//        LOG.info("Request getMathElement: " + counter);
//        String cmd = CMD.replace("#SEARCH#", expression);
//        try {
//            String result = client.execute(cmd);
//            if ( result == null || result.isEmpty() ) return null;
//            String[] data = result.split("\n");
//            if ( data.length != 3 ) return null;
//            return new MathElement(
//                    expression,
//                    Short.parseShort(data[0]),
//                    Integer.parseInt(data[1]),
//                    Integer.parseInt(data[2])
//            );
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null;
//        }

        return memory.get(expression);
    }

//    public LinkedList<MathElement> getMathElementBulk(LinkedList<String> expressions) {
//        String list = "";
//        for ( String s : expressions ) list += ", \""+s+"\"";
//        list = list.substring(2);
//
//        LinkedList<MathElement> results = new LinkedList<>();
//        String cmd = CMD_GROUP.replace("#SEARCH-LIST#", list);
//        try {
//            String result = client.execute(cmd);
//            Matcher m = HIT_PATTERN.matcher(result);
//            int i = 0;
//            while ( m.find() ){
//                if ( m.group(1) == null ) results.addLast(null);
//                else {
//                    MathElement e = new MathElement(
//                            expressions.get(i),
//                            Short.parseShort(m.group(1)),
//                            Integer.parseInt(m.group(2)),
//                            Integer.parseInt(m.group(3))
//                    );
//                    results.addLast(e);
//                }
//                i++;
//            }
//            return results;
//        } catch (IOException e) {
//            e.printStackTrace();
//            return results;
//        }
//    }

    private static TFIDFLoader loader;

    public static void initTFIDFLoader(Path path){
        if (loader != null) return;
        loader = new TFIDFLoader();
//        loader.client = BaseXController.getTFIDFResultsClient();
        try {
            Files.walk(path)
                    .filter(Files::isRegularFile)
                    .forEach(loader::load);
        } catch (IOException e) {
            e.printStackTrace();
        }
//        loader.load(path);
    }

    public static TFIDFLoader getLoaderInstance(){
        return loader;
    }

    public static String[] stripParentheses(String[] a){
        for(int i = 0; i<a.length; i++)
            if(a[i].startsWith("\""))
                a[i] = a[i].substring(1,a[i].length()-1);
        return a;
    }

    public Stream<MathElement> getMathElementStream(){
        return null;
//        return memory.values().stream();
    }

    public static void main(String[] args) throws IOException {
//        long start = System.currentTimeMillis();
//        TFIDFLoader loader = new TFIDFLoader();
//        Files.walk(Paths.get("/opt/zbmath/tfidf"))
//                .parallel()
//                .filter(Files::isRegularFile)
//                .forEach(loader::load);
//
//        System.out.println();
//        System.out.println("Done...");
//        double heapSize = Runtime.getRuntime().totalMemory()/Math.pow(1024,2);
//        System.out.println("Using mem: " + heapSize + "MB");


//        Scanner in = new Scanner(System.in);
//        String input;
//
//        while (!(input = in.next()).matches("quit|exit|\\s*")){
//            System.out.println("You entered: " + input);
//            MathElement me = loader.memory.get(input);
//            System.out.println(me);
//        }

//        long stop = System.currentTimeMillis() - start;
//        LOG.info("Time Elapsed: " + stop + "ms");
//        System.out.println();
//        System.out.println("Done");
//
//        String format = String.format("%02d:%02d",
//                TimeUnit.MILLISECONDS.toMinutes(stop),
//                TimeUnit.MILLISECONDS.toSeconds(stop)%60
//        );
//        System.out.println("Time Elapsed: " + format);
//
//        System.out.println("Bye bye");

//        BaseXController.initBaseXServers(new HashMap<>(), new HashMap<>(), null);
//        initTFIDFLoader(null);
//        TFIDFLoader loader = getLoaderInstance();

//        for (int i = 0; i < 100; i++){
//            MathElement e = loader.getMathElement("mrow(mi:E,mo:=,mrow(mi:m,mo:ivt,msup(mi:c,mn:2)))");
//            System.out.println(e);
//        }


//        LinkedList<String> list = new LinkedList<>();
//        for ( int i = 0; i < 100; i++ ) {
//            list.addLast("mrow(mi:E,mo:=,mrow(mi:m,mo:ivt,msup(mi:c,mn:2)))");
//        }
//
//        LOG.info("Start");
//
//        LinkedList<MathElement> res = loader.getMathElementBulk(list);
//        LOG.info("DONE!");
//
//        for (MathElement e : res) {
//            System.out.println(e);
//        }
//
//        BaseXController.closeAllClients();
    }
}
