package mir.formulacloud.util;

import mir.formulacloud.beans.MathElement;
import mir.formulacloud.searcher.FastTest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.basex.query.func.math.MathE;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Scanner;
import java.util.stream.Stream;

/**
 * @author Andre Greiner-Petter
 */
public class TFIDFLoader {
    private static final Logger LOG = LogManager.getLogger(TFIDFLoader.class.getName());

    private volatile HashMap<String, MathElement> memory;

    private TFIDFLoader(){
        memory = new HashMap<>();
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

    public MathElement getMathElement(String expression){
        return memory.get(expression);
    }

    private static TFIDFLoader loader;

    public static void initTFIDFLoader(Path path){
        if (loader != null) return;
        loader = new TFIDFLoader();
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
        return memory.values().stream();
    }

    public static void main(String[] args) throws IOException {
        TFIDFLoader loader = new TFIDFLoader();
        Files.walk(Paths.get("out"))
                .parallel()
                .filter(Files::isRegularFile)
                .forEach(loader::load);

        System.out.println();
        System.out.println("Done...");
        double heapSize = Runtime.getRuntime().totalMemory()/Math.pow(1024,2);
        System.out.println("Using mem: " + heapSize + "MB");


        Scanner in = new Scanner(System.in);
        String input;

        while (!(input = in.next()).matches("quit|exit|\\s*")){
            System.out.println("You entered: " + input);
            MathElement me = loader.memory.get(input);
            System.out.println(me);
        }

        System.out.println("Bye bye");
    }
}
