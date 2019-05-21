package mir.formulacloud.searcher;

import mir.formulacloud.beans.*;
import mir.formulacloud.util.Helper;
import mir.formulacloud.util.SimpleMMLConverter;
import mir.formulacloud.util.TFIDFLoader;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.IntSummaryStatistics;
import java.util.LinkedList;

import static java.util.stream.Collectors.toList;

/**
 * @author Andre Greiner-Petter
 */
public class FrequencyAnalyzer {

    private TFIDFLoader tfidf;

    public FrequencyAnalyzer(Path frequencyPath){
        TFIDFLoader.initTFIDFLoader(frequencyPath);
        tfidf = TFIDFLoader.getLoaderInstance();
    }

    public void orderedTotalFrequency(){
        tfidf.getMathElementStream()
                .sorted(Comparator.comparingLong(MathElement::getTotalFrequency).reversed())
                .limit(10)
                .forEach(System.out::println);
    }

    public void orderedTotalFrequency(Path outputFile) throws IOException {
        Files.deleteIfExists(outputFile);

        try (BufferedWriter bw = Files.newBufferedWriter(outputFile, StandardOpenOption.CREATE_NEW)){
            tfidf.getMathElementStream()
                    .sorted(Comparator.comparingLong(MathElement::getTotalFrequency).reversed())
                    .forEach( e -> {
                        try {
                            bw.write(""+e.getTotalFrequency()+";"+e.getDepth());
                            bw.newLine();
                        } catch (IOException ioe){
                            ioe.printStackTrace();
                        }
                    });
        } finally {
            System.out.println("Done");
        }
    }

    public void orderedFrequencyPerDepth(Path outputFile, int depth, boolean withMML) throws IOException {
        Files.deleteIfExists(outputFile);

        try (BufferedWriter bw = Files.newBufferedWriter(outputFile, StandardOpenOption.CREATE_NEW)){
            tfidf.getMathElementStream()
                    .filter(e -> e.getDepth() == depth)
                    .sorted(Comparator.comparingLong(MathElement::getTotalFrequency).reversed())
                    .limit(50)
                    .forEach( e -> {
                        try {
                            if (withMML){
                                bw.write(""+e.getTotalFrequency() + "," + SimpleMMLConverter.stringToMML(e.getExpression()));
                            } else {
                                bw.write(""+e.getTotalFrequency());
                            }
                            bw.newLine();
                        } catch (IOException ioe){
                            ioe.printStackTrace();
                        }
                    });
        } finally {
            System.out.println("Done");
        }
    }

    public void generalStats(){
        IntSummaryStatistics stats = tfidf.getMathElementStream()
                .mapToInt(e -> e.getDepth())
                .summaryStatistics();
        System.out.println(stats);
    }

    public static void main(String[] args) throws IOException {
        Path in = Paths.get(args[0]);
        if (args.length < 2){
            FrequencyAnalyzer fa = new FrequencyAnalyzer(in);
            fa.generalStats();
            return;
        }

        Path out = Paths.get(args[1]);
        FrequencyAnalyzer fa = new FrequencyAnalyzer(in);
        fa.orderedTotalFrequency(out.resolve("rawFrequencies.txt"));
//        for ( int i = 1; i <= 10; i++){
//            String name = "allMMLDepth" + i + ".txt";
//            fa.orderedFrequencyPerDepth(out.resolve(name), i, true);
//        }
    }
}
