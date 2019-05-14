package mir.formulacloud.harvest;

import mir.formulacloud.beans.DocBean;
import mir.formulacloud.beans.EndElement;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;

/**
 * @author Andre Greiner-Petter
 */
public class BaseXWriter extends Thread {
    private static String NL = System.lineSeparator();
    private static String ROOT_START =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + NL +
            "<generic-root>" + NL;
    private static String ROOT_END = "</generic-root>";

    private Path outputPath;
    private BlockingQueue<DocBean> queue;

    private int chunkSize;

    public int printedDocs = 0;
    public String publicName;

    public BaseXWriter(Path outputPath, BlockingQueue<DocBean> queue, int chunkSize) throws IOException {
        this.outputPath = outputPath;
        this.queue = queue;
        this.chunkSize = chunkSize;
        this.publicName = outputPath.getFileName().toString();

        if ( !Files.isDirectory(outputPath) ){
            Files.createDirectory(outputPath);
        }
    }

    public void addElement(DocBean element) throws InterruptedException {
        queue.put(element);
    }

    @Override
    public void run(){
        this.printedDocs = 0;
        try {
            DocBean doc;
            while((doc = queue.take()) != null && !(doc instanceof EndElement)){
                Path filePath = outputPath.resolve(doc.getId()+".xml");
                try( OutputStreamWriter writer =
                             new OutputStreamWriter(
                                     new FileOutputStream(filePath.toFile()),
                                     StandardCharsets.UTF_8
                             )) {
                    writer.write(doc.getDocAsString());
                    printedDocs++;
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                    System.exit(1);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        } catch ( InterruptedException ie ){
            ie.printStackTrace();
        } finally {
            String info = String.format(
                    "Done writing to %s: %d files",
                    outputPath.getFileName(),
                    printedDocs
            );
            System.out.println(info);
        }
    }

    @Override
    public String toString(){
        return ""+printedDocs;
    }

//    public void run() {
//        int printedDocs = 1;
//        LinkedList<DocBean> buffer = new LinkedList<>();
//        try {
//            DocBean doc;
//            while((doc = queue.take()) != null && !(doc instanceof EndElement)){
//
//
//                if ( buffer.size() < chunkSize ){
//                    buffer.add(doc);
//                    continue;
//                } else {
//                    writeBuffer(buffer, printedChunks);
//                    printedChunks++;
//                }
//            }
//        } catch ( InterruptedException ie ){
//            ie.printStackTrace();
//        } finally {
//            if ( !buffer.isEmpty() ){
//                writeBuffer(buffer, printedChunks);
//            }
//            String info = String.format(
//                    "Done writing to %s: %d files",
//                    outputPath.getFileName(),
//                    printedChunks
//            );
//            System.out.println(info);
//        }
//    }
//
//    private void writeBuffer(LinkedList<DocBean> buffer, int printedChunks){
//        Path filePath = outputPath.resolve(printedChunks+".xml");
//        try( OutputStreamWriter writer =
//                     new OutputStreamWriter(
//                             new FileOutputStream(filePath.toFile()),
//                             StandardCharsets.UTF_8
//                     )) {
//            writer.write(ROOT_START);
//            while(!buffer.isEmpty()){
//                DocBean tmpdoc = buffer.removeFirst();
//                writer.write(tmpdoc.getDocAsString()+NL);
//            }
//            writer.write(ROOT_END);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

}
