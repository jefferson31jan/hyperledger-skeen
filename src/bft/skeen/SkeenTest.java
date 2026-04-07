package bft.skeen;

import java.io.*;
import java.net.*;

public class SkeenTest {

    public static void main(String[] args) throws Exception {

        // Conectar aos dois shards
        Socket sockA = new Socket("localhost", 12000);
        Socket sockB = new Socket("localhost", 12001);
        PrintWriter outA = new PrintWriter(sockA.getOutputStream(), true);
        PrintWriter outB = new PrintWriter(sockB.getOutputStream(), true);

        String[] msgs = { "msg-001", "msg-002", "msg-003" };

        for (String msgId : msgs) {
            String line = "START|" + msgId + "|TestClient|0|ShardA,ShardB|bftchannel";
            System.out.println("Enviando para ShardA: " + line);
            outA.println(line);
            System.out.println("Enviando para ShardB: " + line);
            outB.println(line);
            Thread.sleep(200);
        }

        sockA.close();
        sockB.close();
        System.out.println("Mensagens enviadas. Observe os logs dos shards.");
    }
}