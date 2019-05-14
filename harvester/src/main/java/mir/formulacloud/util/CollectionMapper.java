package mir.formulacloud.util;

import mir.formulacloud.harvest.BaseXHarvester;
import mir.formulacloud.harvest.MetaHarvester;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.TreeMap;

/**
 * @author Andre Greiner-Petter
 */
public class CollectionMapper {

    private TreeMap<String, String> map;
    private TreeMap<String, String> requested;
    private Path folder;

    public CollectionMapper(Path folder) throws IOException {
        this.folder = folder;
        map = new TreeMap<>();
        requested = new TreeMap<>();
        Files.walk(folder, 1)
                .filter( p -> !Files.isDirectory(p) )
                .filter( p -> p.getFileName().toString().endsWith(".csv"))
                .forEach( this::consume );
    }

    private void consume(Path file) {
        try {
            Files.lines(file)
                    .forEach(
                            l -> {
                                String[] args = l.split(", ");
                                map.put(args[0], args[1]);
                            }
                    );
//            System.out.println("Size: " + map.size());
        } catch ( IOException ioe ){
            ioe.printStackTrace();
        }
    }

    public void storeRequested() {
        if ( requested.isEmpty() ) return;
        System.out.println("Write mapping update for meta data...");
        LinkedList<String> lines = new LinkedList<>();
        for ( String id : requested.keySet() ){
            lines.add(id + ", " + requested.get(id));
        }
        Path p = folder.resolve("requested.csv");

        try {
            if (!Files.exists(p)) Files.createFile(p);
            Files.write(p, lines, StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getDocumentTopic(String docID){
        String cat = map.get(docID);

        if ( cat == null ){
            cat = requested.get(docID);
        }

        if ( cat == null ) {
            BaseXHarvester.printInfo(null, "Requesting meta information from arxiv server for DocID: " + docID, 0);
            cat = MetaHarvester.requestSingleDocCategories(docID);
            if ( cat != null ) requested.put(docID, cat);
        }

        return cat;
    }

}
