package mir.formulacloud.tfidf;

import com.formulasearchengine.mathosphere.basex.BaseXClient;
import mir.formulacloud.beans.BaseXServerInstances;
import mir.formulacloud.util.TFIDFConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Andre Greiner-Petter
 */
public class BaseXController {
    private static final Logger LOG = LogManager.getLogger(BaseXController.class.getName());

    // pool size is controlled by parallelism level
    private volatile static HashMap<String, BaseXServerInstances> basexServers;
    private volatile static HashMap<String, String> docIDatabaseMapper;
    private volatile static HashMap<String, BlockingQueue<BaseXClient>> poolMapper;
    public static int CLIENT_COUNTER = 0;
    public static int SERVER_COUNTER = 0;

    private volatile static BaseXServerInstances basexTFIDFResultsServer;
    private volatile static BaseXClient tfidfBaseXClient;

    private static final int DEFAULT_PORT = 1984;

    public synchronized static void initBaseXServers(
            HashMap<String, BaseXServerInstances> servers,
            HashMap<String, String> docToDatabaseMapper, // can be null
            TFIDFConfig config
    ){
        if ( BaseXController.basexServers != null ){
            LOG.warn("You already setup the basex servers. Your function call will be ignored.");
            return;
        }

        LOG.info("Initialize basex servers.");
        BaseXController.docIDatabaseMapper = docToDatabaseMapper;
        BaseXController.basexServers = servers;
        BaseXController.poolMapper = new HashMap<>();
        int port = DEFAULT_PORT;
        for (String key : basexServers.keySet()){
            LOG.info("Initialize basex server on port " + port + " for database " + key);
            BaseXServerInstances bs = new BaseXServerInstances(key, port);
            bs.start();
            SERVER_COUNTER++;
            basexServers.put(key, bs);

            LOG.info("Initialize first client for basex db " + key);
            BlockingQueue<BaseXClient> syncedClients = new LinkedBlockingQueue<>();

            for ( int i = 0; i < config.getDefaultClients(); i++ ){
                BaseXClient client = establishNewConnection(key, port);
                syncedClients.add(client);
                LOG.info("Setup default client for " + key + ": " + (i+1));
            }

            poolMapper.put(key, syncedClients);
            port++;
        }

        String resultsKey = "tfidf-data";
        basexTFIDFResultsServer = new BaseXServerInstances(resultsKey, port);
        basexTFIDFResultsServer.start();
        tfidfBaseXClient = establishNewConnection(resultsKey, port);
    }

    public static String getDBFromDocID(String docID) {
        return docIDatabaseMapper.get(docID);
    }

    public static BaseXClient getBaseXClient(String docID){
        String db = docIDatabaseMapper.get(docID);
        if ( db == null ){
            LOG.warn("DocID " + docID + " doesn't have a BaseXDatabase (probably it doesn't have math).");
            return null;
        }

        return getBaseXClientByDatabase(db);
    }

    public static BaseXClient getBaseXClientByDatabase(String db){
        BlockingQueue<BaseXClient> dbPool = poolMapper.get(db);
        BaseXServerInstances server = basexServers.get(db);
        if ( dbPool == null || server == null ){
            LOG.error("Unknown database " + db);
            System.exit(1);
        }

        try {
            return dbPool.take();
        } catch (InterruptedException e) {
            LOG.fatal("Why does somebody interrupted this man?");
            return null;
        }
//        return doNotCreateNewClients(dbPool);
    }

    private static BaseXClient doNotCreateNewClients(List<BaseXClient> list){
        while(true){
            synchronized(list){
                if (!list.isEmpty()) break;
            }
        }

        synchronized (list){
            try {
                BaseXClient bc = list.remove(0);
                if (bc != null) return bc;
                else
                    return doNotCreateNewClients(list); // wtf this shouldn't be possible but flink can do everything... O.o
            } catch (IndexOutOfBoundsException iooe){
                // might be a never ending story but fuck it... this shouldn't be possible anyway
                return doNotCreateNewClients(list);
            }
        }
    }

    public static void returnBaseXClient(String docID, BaseXClient client){
        String db = docIDatabaseMapper.get(docID);
        if ( db == null ){
            LOG.error("Returned Unknown docID " + docID);
            System.exit(1);
        }

        returnBaseXClientByDatabase(db, client);
    }

    public static void returnBaseXClientByDatabase(String db, BaseXClient client){
        BlockingQueue<BaseXClient> dbPool = poolMapper.get(db);
        synchronized (dbPool){
            dbPool.add(client);
            LOG.debug("Kindly received a basexclient for "+ db +". Pool size: " + dbPool.size());
        }
    }

    public static BaseXClient getTFIDFResultsClient() {
        return tfidfBaseXClient;
    }

    public synchronized static void closeAllClients(){
        for (String db : basexServers.keySet()){
            BlockingQueue<BaseXClient> pool = poolMapper.get(db);
            // stop all clients first
            while(!pool.isEmpty()){
                try {
                    pool.poll().close();
                } catch (IOException e) {
                    LOG.error("Cannot close basexclients for " + db);
                }
            }
            LOG.info("Stopped all basex clients for database " + db);
            try {
                BaseXServerInstances bs = basexServers.get(db);
                LOG.info("Stop basex server " + db);
                bs.stop();
                LOG.info("Stopped basex server for database " + db);
            } catch (Exception e){
                LOG.error("Strange, cannot stop basexserver " + db, e);
            }
        }

        try {
            tfidfBaseXClient.close();
            basexTFIDFResultsServer.stop();
        } catch (IOException e) {
            LOG.error("Cannot stop result database");
        }


        LOG.info("Stopped all BaseX clients and servers!");
    }

    private synchronized static BaseXClient establishNewConnection(String db, int port){
        try {
            BaseXClient client = new BaseXClient("localhost", port, "admin", "admin");
            client.execute("OPEN " + db);
            CLIENT_COUNTER++;
            return client;
        } catch (IOException e) {
            LOG.error("Cannot create new BaseXClient.", e);
            System.exit(1);
            return null;
        }
    }

}
