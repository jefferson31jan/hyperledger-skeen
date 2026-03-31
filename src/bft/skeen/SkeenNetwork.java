package bft.skeen;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Camada de rede do Skeen — TCP simples entre shards.
 * Cada shard abre um ServerSocket para receber mensagens
 * e cria conexões de saída para os outros shards.
 */
public class SkeenNetwork implements SkeenNode.NetworkLayer {

    private final Logger logger = LoggerFactory.getLogger(SkeenNetwork.class);

    private final String localShardId;
    private final int    listenPort;
    private final SkeenNode node;

    // shardId -> host:porta dos outros shards
    private final Map<String, InetSocketAddress> peers = new ConcurrentHashMap<>();

    // shardId -> conexão de saída
    private final Map<String, PrintWriter> outConns = new ConcurrentHashMap<>();

    public SkeenNetwork(String localShardId, int listenPort, SkeenNode node) {
        this.localShardId = localShardId;
        this.listenPort   = listenPort;
        this.node         = node;
    }

    // Registrar endereço de um shard remoto
    public void addPeer(String shardId, String host, int port) {
        peers.put(shardId, new InetSocketAddress(host, port));
    }

    // Iniciar servidor de escuta e threads de conexão
    public void start() {
        // Thread que aceita conexões de entrada
        new Thread(this::acceptLoop, "skeen-accept").start();
        // Conectar aos peers em background
        new Thread(this::connectToPeers, "skeen-connect").start();
    }

    // ── envio (NetworkLayer) ─────────────────────────────────────────────────
    @Override
    public void send(SkeenNode.SkeenMsg msg, String toShardId) {
        // Se destino é o próprio shard, entrega direto (loopback)
        if (toShardId.equals(localShardId)) {
            node.receive(msg);
            return;
        }

        PrintWriter out = outConns.get(toShardId);
        if (out == null) {
            logger.warn("Sem conexão com shard {}, descartando mensagem", toShardId);
            return;
        }

        // Formato: TYPE|msgId|fromShard|ts|dest1,dest2,...
        String destStr = msg.dest != null ? String.join(",", msg.dest) : "";
        String line = msg.type.name() + "|" + msg.msgId + "|"
                    + msg.fromShard + "|" + msg.ts + "|" + destStr;
        synchronized (out) {
            out.println(line);
            out.flush();
        }
    }

    // ── servidor TCP ─────────────────────────────────────────────────────────
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

    // ── cliente TCP ──────────────────────────────────────────────────────────
    private void connectToPeers() {
        for (Map.Entry<String, InetSocketAddress> entry : peers.entrySet()) {
            String shardId = entry.getKey();
            InetSocketAddress addr = entry.getValue();

            // tentar conectar com retry
            new Thread(() -> {
                while (true) {
                    try {
                        Socket sock = new Socket(addr.getHostName(), addr.getPort());
                        PrintWriter out = new PrintWriter(sock.getOutputStream(), true);
                        outConns.put(shardId, out);
                        logger.info("Conectado ao shard {}", shardId);
                        break;
                    } catch (IOException e) {
                        logger.warn("Aguardando shard {}...", shardId);
                        try { Thread.sleep(2000); } catch (InterruptedException ie) { break; }
                    }
                }
            }, "skeen-conn-" + shardId).start();
        }
    }

    // ── serialização ─────────────────────────────────────────────────────────
    private SkeenNode.SkeenMsg deserialize(String line) {
        try {
            String[] parts = line.split("\\|", -1);
            SkeenNode.Type type      = SkeenNode.Type.valueOf(parts[0]);
            String         msgId     = parts[1];
            String         fromShard = parts[2];
            long           ts        = Long.parseLong(parts[3]);

            SkeenNode.SkeenMsg msg = new SkeenNode.SkeenMsg(type, msgId, fromShard, ts);

            if (!parts[4].isEmpty()) {
                msg.dest = new HashSet<>(Arrays.asList(parts[4].split(",")));
            }
            return msg;
        } catch (Exception e) {
            logger.error("Erro ao deserializar mensagem Skeen: {}", line, e);
            return null;
        }
    }
}