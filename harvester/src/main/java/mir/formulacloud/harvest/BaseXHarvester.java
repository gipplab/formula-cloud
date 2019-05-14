package mir.formulacloud.harvest;

import mir.formulacloud.beans.DocBean;
import mir.formulacloud.beans.EndElement;
import mir.formulacloud.beans.MathCollectionBean;
import mir.formulacloud.util.CollectionMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Objects;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * @author Andre Greiner-Petter
 */
public class BaseXHarvester {
    public static final String NL = System.lineSeparator();
    public static final Pattern MATH_ID_PATTERN = Pattern.compile("__MATH_(\\d+)__");

    private static final Pattern FILTER_AFFIL = Pattern.compile("alttext=\"\\{}\\^\\{.*?}\"");
    private static final Pattern MML_PATTERN = Pattern.compile(".*(<math.*?</math>).*");

    private Path[] inputP;
    private Path catP;
    private CollectionMapper mapper;

    private HashMap<ArxivSets, BaseXWriter> writerMap;
    private static BaseXWriter[] writerArray;

    private Path outputPath;
    private int parallelism;
    private int chunkSize;

    public BaseXHarvester(int chunkSize, int parallelism, Path categoriesFolder, Path outputPath, Path... inputFolder) {
        catP = categoriesFolder;
        this.inputP = inputFolder;
        this.outputPath = outputPath;
        this.chunkSize = chunkSize;
        this.parallelism = parallelism;
    }

    public void initWriters() {
        this.writerMap = new HashMap<>();
        ArrayList writerArrayList = new ArrayList<BaseXWriter>();
        for ( ArxivSets set : ArxivSets.values() ){
            try {
                BaseXWriter writer = new BaseXWriter(
                        outputPath.resolve(set.getShortID()),
                        new LinkedBlockingQueue<>(),
                        chunkSize
                );
                writerMap.put(set, writer);
                writerArrayList.add(writer);
            } catch ( IOException ioe ){
                ioe.printStackTrace();
                System.exit(1);
            }
        }
        writerArray = (BaseXWriter[])writerArrayList.toArray(new BaseXWriter[writerArrayList.size()]);
    }

    public void initMap() throws IOException {
        System.out.println("Initialize mapper...");
        mapper = new CollectionMapper(catP);
        System.out.println("Done!");
    }

    private void startWriters(){
        for ( BaseXWriter w : writerMap.values() ){
            w.start();
        }
    }

    private void stopWriters() throws InterruptedException {
        for ( BaseXWriter w : writerMap.values() ){
            w.addElement(new EndElement());
        }
    }

    public void startProcess() throws InterruptedException {
        System.out.println("Start process...");
        printHeaderInfo();

        int[] counter = new int[]{0};
        startWriters();
        ForkJoinPool outerPool = new ForkJoinPool(parallelism);
        Path path = inputP[0];
        try (Stream<Path> filesStream = Files.walk(path)) {
            outerPool.submit(
                    () -> filesStream
                            .parallel()
                            .filter(p -> !Files.isDirectory(p))
                            .filter(p -> p.getFileName().toString().endsWith(".ann"))
                            .map(p -> {
                                try {
                                    return analyzeDocument(p);
                                } catch (Exception e) {
                                    System.err.println();
                                    System.err.println("Error in file " + p.toString() + ". Reason: " + e.getMessage() + ": " + e.getLocalizedMessage());
                                    e.printStackTrace();
                                    return null;
                                }
                            })
                            .filter(Objects::nonNull)
                            .forEach(bean -> {
                                counter[0]++;
                                printInfo(counter[0], null, outerPool.getPoolSize());
                                try {
                                    BaseXWriter writer = writerMap.get(bean.getArxivSet());
                                    writer.addElement(bean);
                                } catch (NullPointerException npe) {
                                    npe.printStackTrace();
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            })
            );

            outerPool.shutdown();
            outerPool.awaitTermination(42, TimeUnit.HOURS);
        } catch (Exception e) {
            e.printStackTrace();
            //break;
        } finally {
            System.out.println();
            System.out.println("Finished analyzing... wait until writing process finished...");
            stopWriters();
            mapper.storeRequested();
        }
    }

    private String getDocID(Path doc){
        return doc.getFileName().toString().replace(".ann", "");
    }

    private String getFineCollections(String docID){
        return mapper.getDocumentTopic(docID);
    }

    public DocBean analyzeDocument(Path doc){
        DocBean bean = new DocBean();

        String docID = getDocID(doc);
        bean.setId(docID);

        if ( mapper != null ){
            String fine = getFineCollections(docID);
            bean.setFine(fine);

            LinkedList<String> majors = ArxivSets.getMajorSubjects(fine);

            String majorSub = majors.removeFirst();
            bean.setMajor(majorSub);
            bean.setArxivSet(ArxivSets.getArxivSet(majorSub));

            String minorSub = "";
            while ( !majors.isEmpty() ){
                minorSub += majors.removeFirst() + " ";
            }
            bean.setMinor(minorSub);
        }

        // properly close files.lines stream to avoid "too many files open error"
        try (Stream<String> linesStream = Files.lines(doc)){
            linesStream.forEach( bean::addMMLExpression );
            return bean;
        } catch ( IOException ioe ){
            ioe.printStackTrace();
            return null;
        }
    }

    /**
     * Use analyzeDocument that returns a DocBean instead!
     * @param doc
     * @return
     */
    @Deprecated
    public MathCollectionBean analyzeDocumentOld(Path doc) {
        String docID = doc.getFileName().toString().replace(".ann", "");
        String majorSub = "", minorSub = "", fine = "";
        if ( mapper != null ){
            fine = mapper.getDocumentTopic(docID);
            LinkedList<String> majors = ArxivSets.getMajorSubjects(fine);

            majorSub = majors.removeFirst();
            minorSub = "";
            while ( !majors.isEmpty() ){
                minorSub += majors.removeFirst() + " ";
            }
        }

        String[] docNode = getDocNode(docID, majorSub, "", "");

        String[] mathID = new String[1];
        StringBuilder sb = new StringBuilder();
        sb.append(docNode[0]).append(NL);

        try {
            Files.lines(doc)
                    .forEach( l -> {
                        if ( mathID[0] != null ){
                            String expressionNode = getExpressionNode(mathID[0], l);
                            if ( expressionNode != null ){
                                Matcher m = FILTER_AFFIL.matcher(expressionNode);
                                if ( !m.find() ) sb.append(expressionNode).append(NL);
                            }
                            mathID[0] = null;
                        } else {
                            String id = getMathID(l);
                            if ( id != null ){
                                mathID[0] = id;
                            }
                        }
                    });

            sb.append(docNode[1]);
            String content = sb.toString();
            if ( content.isEmpty() ){
                return null;
            } else {
                return new MathCollectionBean(ArxivSets.getArxivSet(majorSub), content);
            }
        } catch (IOException ioe){
            ioe.printStackTrace();
            return null;
        }
    }

    private String[] getDocNode(String docID, String majorSub, String majorSubSub, String collections){
        return new String[]{
                "  <mws:doc id=\"" + docID + "\" " + NL +
                        "      major-collection=\"" + majorSub + "\" " + NL +
                        "      minor-collection=\"" + majorSubSub + "\" " + NL +
                        "      fine-collection=\"" + collections + "\">",
                "  </mws:doc>"
        };
    }

    private String getMathID(String idLine){
        Matcher m = MATH_ID_PATTERN.matcher(idLine);
        if ( m.find() ){
            return m.group(1);
        } else {
            return null;
        }
    }

    private String getExpressionNode(String id, String xmlLine){
        String math = "";
        Matcher m = MML_PATTERN.matcher(xmlLine);
        if ( m.matches() && !m.group(1).isEmpty() ){
            math = m.group(1);
        } else {
            String[] secondLine = xmlLine.split("\t");
            if ( secondLine.length > 2 ) math = secondLine[2];
        }

        return math.isEmpty() ? null : "    <mws:expr id=\"" + id + "\">" + math + "</mws:expr>";
    }

    private int infoEntryLength = 8;
    private static String updateFormatPattern = "";

    public void printHeaderInfo(){
        StringBuilder h = new StringBuilder();
        StringBuilder c = new StringBuilder();

        h.append(" Total|Pool");
        c.append("%6d|%4d");
        String[] names = new String[writerArray.length];
        for ( int i = 0; i < writerArray.length; i++ ){
            h.append("|%5s"); // name of all writers
            c.append("|%5s");
            names[i] = writerArray[i].publicName;
            if(names[i].length() > 5){
                names[i] = names[i].substring(0,5);
            }
        }
        h.append("|Messages"); // general message
        c.append("|%s");
        String header = String.format(h.toString(), names);
        updateFormatPattern = c.toString();
        System.out.println(header);
    }

    public static void printInfo(Integer processedDocs, String message, int poolSize){
        Object[] info = new Object[writerArray.length+3];
        info[0] = processedDocs;
        info[1] = poolSize;
        int idx = 1;
        for (int i = 0; i < writerArray.length; i++){
            info[2+i] = writerArray[i];
            idx = 2+i;
        }
        info[idx+1] = message;
        String out = String.format(
                updateFormatPattern,
                info
        );
        System.out.print("\r"+out);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Path collections = null, output = null;
        LinkedList<Path> dataList = new LinkedList<>();

        int paral = 4;
        int chunkSize = 10;

        for ( int i = 0; i < args.length; i++ ) {
            switch (args[i]){
                case "-in-collection":
                case "-in-meta":
                    i++;
                    collections = Paths.get(args[i]);
                    break;
                case "-in-data":
                    for ( int j = i+1; j < args.length && !args[j].matches("-.*"); j++, i++ ){
                        dataList.add(Paths.get(args[j]));
                    }
                    break;
                case "-out":
                    i++;
                    output = Paths.get(args[i]);
                    break;
                case "-parallel":
                    i++;
                    paral = Integer.parseInt(args[i]);//Math.min(Integer.parseInt(args[i]),16);
                    break;
                case "-chunk":
                    i++;
                    chunkSize = Integer.parseInt(args[i]);
                    break;
                default:
                    System.err.println("You didn't use the arguments correct!");
            }
        }

        BaseXHarvester harvester = new BaseXHarvester(
                chunkSize,
                paral,
                collections,
                output,
                dataList.toArray(new Path[dataList.size()]));
        harvester.initWriters();
        harvester.initMap();
        harvester.startProcess();
    }
}
