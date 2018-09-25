package edu.uchicago.cs.ucare.dmck;

import java.io.File;
import java.io.FileWriter;
import org.apache.log4j.Logger;

public class InterceptionLayer {

  private static final Logger LOG = Logger.getLogger(InterceptionLayer.class);

  // Hard-coded IPC Directory.
  private final static String IPC_DIR = "/tmp/ipc";

  public static void interceptMessageEvent(long sendID, long recvID, String verb, String payload) {
    long hashID = getHashId(sendID, recvID, verb, payload);

    String filename = "cassPaxos-" + hashID + "-" + getTimestamp();

    String content = "";
    content += "sender=" + sendID + "\n";
    content += "recv=" + recvID + "\n";
    content += "verb=" + verb + "\n";
    content += "payload=" + payload + "\n";
    content += "eventId=" + hashID + "\n";

    LOG.debug("DMCK: Intercept a message:\n" + content);

    writePacketToFile(filename, content);
    commitFile(filename);
    waitForAck(filename);
  }

  public static void updateNodeState(long nodeId, String type, String ballot) {
    String filename = "cassUpdate-" + nodeId + "-" + getTimestamp();

    String content = "";
    content += "sender=" + nodeId + "\n";
    content += "type=" + type + "\n";
    content += "ballot=" + ballot + "\n";

    writePacketToFile(filename, content);
    commitFile(filename);
  }

  private static void commitFile(String filename) {
    try {
      Runtime.getRuntime()
          .exec("mv " + IPC_DIR + "/new/" + filename + " " + IPC_DIR + "/send/" + filename);
    } catch (Exception e) {
      LOG.error("DMCK: Error in committing the file=" + filename);
    }
  }

  private static void waitForAck(String filename) {
    File f = new File(IPC_DIR + "/ack/" + filename);
    while (!f.exists()) {
      try {
        Thread.sleep(0, 100);
      } catch (InterruptedException ie) {
        ie.printStackTrace();
      }
    }

    // Remove DMCK enabling message
    try {
      Runtime.getRuntime().exec("rm " + IPC_DIR + "/ack/" + filename);
    } catch (Exception e) {
      LOG.error("DMCK: Error in deleting ack file=" + filename);
    }
  }

  private static void writePacketToFile(String filename, String content) {
    File file = new File(IPC_DIR + "/new/" + filename);
    try {
      file.createNewFile();
      FileWriter writer = new FileWriter(file);
      writer.write(content);
      writer.flush();
      writer.close();
    } catch (Exception e) {
      LOG.error("DMCK: Error in writing state content to file=" + filename);
    }
  }

  private static long getTimestamp() {
    return System.currentTimeMillis() % 100000;
  }

  private static long getHashId(long sendID, long recvID, String verb, String payload) {
    long prime = 31;
    long hash = 1;
    hash = prime * hash + recvID;
    hash = prime * hash + sendID;
    hash = prime * hash + verb.hashCode();
    if (!verb.equals("PAXOS_PREPARE_RESPONSE") && !verb.equals("PAXOS_PROPOSE_RESPONSE")) {
      hash = prime * hash + payload.hashCode();
    }
    return hash;
  }

}
