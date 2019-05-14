package mir.formulacloud.harvest;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Harvesting meta data from arxiv documents by a given ID
 *
 * @author Andre Greiner-Petter
 */
public class MetaHarvester {
    private static final String ID_PATTERN = "$$ID$$";
    private static final String SET_PATTERN = "$$SETID$$";
    private static final String RESUMPTION_PATTERN = "$$KEY$$";

    private static final String ARXIV_BACKEND_URL = "http://export.arxiv.org/oai2";

    private static final String ARXIV_SINGLE_REQ_PARAMS =
            "verb=GetRecord&identifier=oai:arXiv.org:$$ID$$&metadataPrefix=arXiv";

    private static final String ARXIV_SET_REQ_PARAMS =
            "verb=ListRecords&set=$$SETID$$&metadataPrefix=arXiv";

    private static final String ARXIV_SET_REQ_RESUMPTION_PARAMS =
            "verb=ListRecords&resumptionToken=$$KEY$$";

    private static final Pattern RECORD_PATTERN =
            Pattern.compile("<id>(.*?)</id>|<categories>(.*?)</categories>|<resumptionToken cursor=\"(\\d+)\" completeListSize=\"(\\d+)\">(.*?)</resumptionToken>");

    private static long RETRY_SLEEP = 10_000;

    private HashMap<String, String> collection;

    private final Path input, output;
    private static URL backendURL;

    static {
        try {
            backendURL = new URL(ARXIV_BACKEND_URL);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
    }

    private long currentCollectionListSize = 0;
    private long counter = 0;
    private long cursor = -1;

    private boolean error = false;

    public MetaHarvester(Path inputdir, Path outputpath){
        this.input = inputdir;
        this.output = outputpath;
        collection = new HashMap<>();
    }

    public void collectingAllSets(){
        for ( ArxivSets set : ArxivSets.values() ){
            counter = 0;
            cursor = -1;
            currentCollectionListSize = 0;
            String resumptionKey = null;

            System.out.println("Start collecting set " + set.getId() + " (" + set.getName() + ")");

            while ( (resumptionKey = collectingSet(set.getId(), resumptionKey)) != null && !error ){
                System.out.print("\rEntry " + counter + " / " + currentCollectionListSize + " - write results to file...");
                writingCollection(set);
            }

            if (!collection.isEmpty() && !error){
                writingCollection(set);
            }

            if ( error ){
                System.err.println("An error occurred. Stopped the process!");
                return;
            }

            System.out.println();
            System.out.println("Finished Set " + set.getName());
        }
    }

    public String collectingSet(String setID, String resumptionkey) {
        collection.clear();

        String params;

        if ( resumptionkey != null ){
            System.out.print(String.format("\rEntry %8d / %-8d - %-70s", counter, currentCollectionListSize, "continuing next bulk with resumption key " + resumptionkey));
            params = ARXIV_SET_REQ_RESUMPTION_PARAMS.replace(RESUMPTION_PATTERN, resumptionkey);
        } else {
            System.out.print("\rSending request for set " + setID + "...");
            params = ARXIV_SET_REQ_PARAMS.replace(SET_PATTERN, setID);
        }
        HttpURLConnection con = null;
        String innerResumptionKey = null;

        // open connection
        try {
            con = (HttpURLConnection) backendURL.openConnection();

            // setup request
            con.setRequestMethod("GET");
            con.setDoOutput(true);
            DataOutputStream out = new DataOutputStream(con.getOutputStream());
            out.writeBytes(params);
            out.close();

            int resCode = con.getResponseCode();
            if ( resCode == 503 ){ // resumption timeout?
                String delay = con.getHeaderField("Retry-After");
                if ( delay != null && !delay.isEmpty() ){
                    RETRY_SLEEP = Long.parseLong(delay)*1000;
                }
                System.out.print(
                        String.format(
                                "\rEntry %8d / %-8d - 503: %s ==> Waiting %d seconds...%20s",
                                counter,
                                currentCollectionListSize,
                                con.getResponseMessage(),
                                (RETRY_SLEEP/1000), " "
                        )
                );
                try {
                    Thread.sleep(RETRY_SLEEP);
                    return collectingSet(setID, resumptionkey);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    error = true;
                    return null;
                }
            } else if ( resCode != 200 ){ // any error appeared...
                System.err.println();
                System.err.println("Error response code return " + resCode + ": " + con.getResponseMessage());
                error = true;
                return null;
            }

            // reading response
            InputStream is = con.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;
            String id = ""; // id always appears first

            while( (line = br.readLine()) != null ){
                Matcher m = RECORD_PATTERN.matcher(line);
                while ( m.find() ){
                    if ( m.group(1) != null ){ // ID mode
                        String rawid = m.group(1);
                        id = ArxivUtils.convertURLEntityToFileID(rawid);
                    } else if (m.group(2) != null){ // categories mode
                        collection.put(id, m.group(2));
                        counter++;
                        System.out.print(
                                String.format("\rEntry %8d / %-8d %80s",
                                        counter,
                                        currentCollectionListSize,
                                        " ")
                        );
                    } else if (m.group(5) != null){
                        // resumption token... we didn't complete the request yet... we only received a part of it
                        cursor = Long.parseLong(m.group(3));
                        currentCollectionListSize = Long.parseLong(m.group(4));
                        innerResumptionKey = m.group(5);
                    }
                }
            }

            br.close();

            if (innerResumptionKey == null) {
                if ( counter < currentCollectionListSize-100 ){ // -1 just in case we missed something else
                    // something went wrong... didn't received a new resumption key but didn't finished set yet
                    // lets try again!
                    counter = cursor;
                    return collectingSet(setID, resumptionkey);
                } else {
                    //System.out.println("Finished Collecting " + setID);
                    return null;
                }
            } else {
                return innerResumptionKey;
            }
        } catch ( IOException ioe ){
            System.err.println("An error occurred...");
            if ( ioe.getMessage().contains("Premature EOF") ){
                // disconnected before we finished reading? just try to send same request again...
                try { con.disconnect(); } catch (Exception e){}
                counter = cursor;
                return collectingSet(setID, resumptionkey);
            } else {
                ioe.printStackTrace();
                error = true;
                return null;
            }
        } finally {
            if ( con != null ){
                con.disconnect();
            }
        }
    }

    public void writingCollection(ArxivSets set){
        Path outputFile = output.resolve(set.getShortID()+".csv");
        if (!Files.exists(outputFile)){
            try {
                Files.createFile(outputFile);
            } catch (IOException ioe){
                System.err.println("Cannot create output file...");
                ioe.printStackTrace();
                error = true;
            }
        }

        try ( BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile.toFile(), true)) ){
            for ( String key : collection.keySet() ){
                bw.write(key + ", " + collection.get(key));
                bw.newLine();
            }
        } catch (IOException ioe){
            System.err.println("Cannot write to file...");
            ioe.printStackTrace();
            error = true;
        }
    }

    public static String requestSingleDocCategories(String docID){
        docID = ArxivUtils.convertFileNameToURLEntity(docID);
        String params = ARXIV_SINGLE_REQ_PARAMS.replace(ID_PATTERN, docID);
        HttpURLConnection con = null;

        // open connection
        try {
            con = (HttpURLConnection) backendURL.openConnection();

            // setup request
            con.setRequestMethod("GET");
            con.setDoOutput(true);
            DataOutputStream out = new DataOutputStream(con.getOutputStream());
            out.writeBytes(params);
            out.close();

            int resCode = con.getResponseCode();
            if ( resCode == 503 ){
                String delay = con.getHeaderField("Retry-After");
                if ( delay != null && !delay.isEmpty() ){
                    long waitseconds = Long.parseLong(delay)*1000;
                    try {
                        Thread.sleep(waitseconds);
                        // try again
                        return requestSingleDocCategories(docID);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            }

            // reading response
            InputStream is = con.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;

            while( (line = br.readLine()) != null ){
                Matcher m = RECORD_PATTERN.matcher(line);
                while ( m.find() ){
                    if (m.group(2) != null){ // categories mode
                        return m.group(2);
                    }
                }
            }

            br.close();
            return null;
        } catch ( IOException ioe ){
            ioe.printStackTrace();
            return null;
        } finally {
            if ( con != null ){
                con.disconnect();
            }
        }
    }

    public static void main(String[] args){
        MetaHarvester harvester = new MetaHarvester(Paths.get("in"), Paths.get("collections"));
        harvester.collectingAllSets();
    }

}
