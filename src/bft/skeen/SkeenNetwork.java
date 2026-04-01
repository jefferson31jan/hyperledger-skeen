package bft.skeen;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SkeenNetwork implements SkeenNode.NetworkLayer {

    private final Logger logger = LoggerFactory.getLogger(SkeenNetwork.class);

    private final String localShardId;
    private final int    listenPort;
    private SkeenNode node;

    private final Map<String, InetSocketAddress>       peers    = new ConcurrentHashMap<>();
    private final Map<String, PrintWriter>             outConns = new ConcurrentHashMap<>();
    private final Map<String, Queue<SkeenNode.SkeenMsg>> pending = new ConcurrentHashMap<>();

    public SkeenNetwork(String localShardId, int listenPort, SkeenNode node) {
        this.localShardId = localShardId;
        this.listenPort   = listenPort;
        this.node         = node;
    }

    public void addPeer(String shardId, String host, int port) {
        peers.put(shardId, new InetSocketAddress(host, port));
        pending.put(shardId, new ConcurrentLinkedQueue<>());
    }

    public void start() {
        new Thread(this::acceptLoop, "skeen-accept").start();
        new Thread(this::connectToPeers, "skeen-connect").start();
    }

    @Override
    public void send(SkeenNode.SkeenMsg msg, String toShardId) {
        if (toShardId.equals(localShardId)) {
            node.receive(msg);
            return;
        }
        PrintWriter out = outConns.get(toShardId);
        if (out == null) {
            // Enfileirar para envio posterior
            Queue<SkeenNode.SkeenMsg> q = pending.computeIfAbsent(
                toShardId, k -> new ConcurrentLinkedQueue<>());
            q.add(msg);
            logger.warn("Sem conexão com shard {} — mensagem enfileirada (total={})",
                toShardId, q.size());
            return;
        }
        doSend(out, msg, toShardId);
    }

    private void doSend(PrintWriter out, SkeenNode.SkeenMsg msg, String toShardId) {
        String destStr = msg.dest != null ? String.join(",", msg.dest) : "";
        String line = msg.type.name() + "|" + msg.msgId + "|"
                    + msg.fromShard + "|" + msg.ts + "|" + destStr;
        synchronized (out) {
            out.println(line);
            out.flush();
            if (out.checkError()) {
                logger.warn("Erro ao enviar para shard {}", toShardId);
                outConns.remove(toShardId);
            }
        }
    }

    private void flushPending(String shardId) {
        Queue<SkeenNode.SkeenMsg> q = pending.get(shardId);
        if (q == null || q.isEmpty()) return;
        PrintWriter out = outConns.get(shardId);
        if (out == null) return;
        int count = 0;
        SkeenNode.SkeenMsg msg;
        while ((msg = q.poll()) != null) {
            doSend(out, msg, shardId);
            count++;
        }
        if (count > 0) logger.info("Enviadas {} mensagens pendentes para shard {}", count, shardId);
    }

    private void acceptLoop() {
        try (ServerSocket server = new ServerSocket(listenPort)) {
            logger.info("Skeen ouvindo na porta {}", listenPort);
            while (true) {
                Socket sock = server.accept();
                new Thread(() -> readLoop(sock), "skeen-read").start();
            }
        } catch (IOException e) {
            logger.error("Erro no servidor Skeen", e);
        }
    }

    private void readLoop(Socket sock) {
        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(sock.getInputStream()))) {
            String line;
            while ((line = in.readLine()) != null) {
                SkeenNode.SkeenMsg msg = deserialize(line);
                if (msg != null) node.receive(msg);
            }
        } catch (IOException e) {
            logger.warn("Conexão encerrada: {}", e.getMessage());
        }
    }

    private void connectToPeers() {
        for (Map.Entry<String, InetSocketAddress> entry : peers.entrySet()) {
            String shardId = entry.getKey();
            InetSocketAddress addr = entry.getValue();
            new Thread(() -> {
                while (true) {
                    try {
                        Socket sock = new Socket(addr.getHostName(), addr.getPort());
                        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
                        outConns.put(shardId, out);
                        logger.info("Conectado ao shard {}", shardId);
                        flushPending(shardId);
                        // monitorar desconexão
                        try { sock.getInputStream().read(); } catch (IOException e) {}
                        outConns.remove(shardId);
                        logger.warn("Desconectado do shard {} — tentando reconectar", shardId);
                    } catch (IOException e) {
                        logger.warn("Aguardando shard {}...", shardId);
                        try { Thread.sleep(1000); } catch (InterruptedException ie) { break; }
                    }
                }
            }, "skeen-conn-" + shardId).start();
        }
    }

    private SkeenNode.SkeenMsg deserialize(String line) {
        try {
            String[] parts = line.split("\\|", -1);
            SkeenNode.Type type      = SkeenNode.Type.valueOf(parts[0]);
            String         msgId     = parts[1];
            String         fromShard = parts[2];
            long           ts        = Long.parseLong(parts[3]);
            SkeenNode.SkeenMsg msg = new SkeenNode.SkeenMsg(type, msgId, fromShard, ts);
            if (!parts[4].isEmpty())
                msg.dest = new HashSet<>(Arrays.asList(parts[4].split(",")));
            return msg;
        } catch (Exception e) {
            logger.error("Erro ao deserializar mensagem Skeen: {}", line, e);
            return null;
        }
    }
}
