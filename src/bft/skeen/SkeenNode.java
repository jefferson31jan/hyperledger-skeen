package bft.skeen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.*;

public class SkeenNode {

    private static final Logger logger = LoggerFactory.getLogger(SkeenNode.class);

    private final String shardId;
    private NetworkLayer net;
    private final DeliveryHandler onDeliver;

    private long clock = 0;
    private final Map<String, Long>              local         = new ConcurrentHashMap<>();
    private final Map<String, Long>              finalTs       = new ConcurrentHashMap<>();
    private final Set<String>                    acked         = ConcurrentHashMap.newKeySet();
    private final Set<String>                    delivered     = ConcurrentHashMap.newKeySet();
    private final Map<String, byte[]>            payloads      = new ConcurrentHashMap<>();
    private final Map<String, String>            channels      = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Long>> localTsBuffer = new ConcurrentHashMap<>();
    private final Map<String, Set<String>>       ackBuffer     = new ConcurrentHashMap<>();
    private final Map<String, Set<String>>       destMap       = new ConcurrentHashMap<>();

    public SkeenNode(String shardId, NetworkLayer net, DeliveryHandler onDeliver) {
        this.shardId   = shardId;
        this.net       = net;
        this.onDeliver = onDeliver;
    }

    public synchronized void multicast(String msgId, byte[] payload,
                                       String channelId, Set<String> dest) {
        logger.info("[{}] multicast msgId={} dest={}", shardId, msgId, dest);
        payloads.put(msgId, payload);
        channels.put(msgId, channelId);
        destMap.put(msgId, new HashSet<>(dest));
        SkeenMsg msg = new SkeenMsg(Type.START, msgId, shardId, 0);
        msg.dest = new HashSet<>(dest);
        for (String d : dest) net.send(msg, d);
    }

    public synchronized void onReceiveStart(SkeenMsg msg) {
        logger.info("[{}] recebeu START msgId={} de={}", shardId, msg.msgId, msg.fromShard);
        if (msg.dest != null && !destMap.containsKey(msg.msgId)) {
            destMap.put(msg.msgId, new HashSet<>(msg.dest));
        }
        // Se não conhecemos o destino, não registrar em local[] para não bloquear tryDeliver
        if (destMap.get(msg.msgId) == null) {
            logger.warn("[{}] START sem destMap para msgId={} — ignorando", shardId, msg.msgId);
            return;
        }
        clock++;
        long ts = clock;
        // Só registrar em local[] se este shard tem o payload (iniciou ou recebeu via BFT)
        // Mensagens de outros shards sem payload local não bloqueiam o tryDeliver
        if (payloads.containsKey(msg.msgId)) {
            local.put(msg.msgId, ts);
            logger.info("[{}] timestamp local={} para msgId={}", shardId, ts, msg.msgId);
        } else {
            logger.info("[{}] timestamp local={} para msgId={} (sem payload local — não registra em local[])",
                shardId, ts, msg.msgId);
        }
        Set<String> dest = destMap.get(msg.msgId);
        SkeenMsg reply = new SkeenMsg(Type.LOCAL_TS, msg.msgId, shardId, ts);
        for (String d : dest) net.send(reply, d);
    }

    public synchronized void onReceiveLocalTs(SkeenMsg msg) {
        String mid = msg.msgId;
        logger.info("[{}] recebeu LOCAL_TS msgId={} de={} ts={}", shardId, mid, msg.fromShard, msg.ts);
        localTsBuffer.computeIfAbsent(mid, k -> new ConcurrentHashMap<>())
                     .put(msg.fromShard, msg.ts);
        Set<String> dest = destMap.get(mid);
        if (dest == null) { logger.warn("[{}] destMap nulo para msgId={}", shardId, mid); return; }
        logger.info("[{}] LOCAL_TS recebidos={} esperados={} para msgId={}",
            shardId, localTsBuffer.get(mid).size(), dest.size(), mid);
        if (localTsBuffer.get(mid).size() < dest.size()) return;
        long maxTs = localTsBuffer.get(mid).values().stream().mapToLong(Long::longValue).max().orElse(0);
        finalTs.put(mid, maxTs);
        clock = Math.max(clock, maxTs);
        logger.info("[{}] timestamp final={} para msgId={}", shardId, maxTs, mid);
        tryDeliver();
        SkeenMsg ack = new SkeenMsg(Type.ACK, mid, shardId, maxTs);
        for (String d : dest) net.send(ack, d);
    }

    public synchronized void onReceiveAck(SkeenMsg msg) {
        String mid = msg.msgId;
        logger.info("[{}] recebeu ACK msgId={} de={}", shardId, mid, msg.fromShard);
        ackBuffer.computeIfAbsent(mid, k -> ConcurrentHashMap.newKeySet()).add(msg.fromShard);
        Set<String> dest = destMap.get(mid);
        if (dest == null) return;
        logger.info("[{}] ACKs recebidos={} esperados={} para msgId={}",
            shardId, ackBuffer.get(mid).size(), dest.size(), mid);
        if (ackBuffer.get(mid).size() < dest.size()) return;
        acked.add(mid);
        tryDeliver();
    }

    private int compare(long ts1, String id1, long ts2, String id2) {
        if (ts1 != ts2) return Long.compare(ts1, ts2);
        return id1.compareTo(id2);
    }

    private void tryDeliver() {
        List<Map.Entry<String, Long>> candidates = new ArrayList<>(finalTs.entrySet());
        candidates.removeIf(e -> delivered.contains(e.getKey()));
        candidates.removeIf(e -> !acked.contains(e.getKey()));
        candidates.sort((a, b) -> compare(a.getValue(), a.getKey(), b.getValue(), b.getKey()));

        for (Map.Entry<String, Long> entry : candidates) {
            String mid = entry.getKey();
            long   fm  = entry.getValue();

            final long fm_final = fm;
            final String mid_final = mid;
            boolean safe = local.entrySet().stream()
                .filter(e -> !delivered.contains(e.getKey()))
                .filter(e -> !e.getKey().equals(mid_final))
                .allMatch(e -> {
                    String mid2 = e.getKey();
                    long   lm2  = e.getValue();
                    boolean result;
                    if (finalTs.containsKey(mid2)) {
                        result = compare(fm_final, mid_final, finalTs.get(mid2), mid2) < 0;
                    } else {
                        result = compare(fm_final, mid_final, lm2, mid2) < 0;
                    }
                    if (!result) logger.warn("[{}] tryDeliver bloqueado: mid={} fm={} mid2={} lm2={} final2={}",
                        shardId, mid_final, fm_final, mid2, lm2, finalTs.get(mid2));
                    return result;
                });

            if (safe) {
                delivered.add(mid);
                String channelId = channels.getOrDefault(mid, "unknown");
                byte[] payload   = payloads.getOrDefault(mid, new byte[0]);
                logger.info("[{}] *** ENTREGOU msgId={} finalTs={} canal={} ***",
                    shardId, mid, fm, channelId);
                onDeliver.deliver(mid, payload, channelId);
            }
        }
    }

    public void receive(SkeenMsg msg) {
        switch (msg.type) {
            case START:    onReceiveStart(msg);   break;
            case LOCAL_TS: onReceiveLocalTs(msg); break;
            case ACK:      onReceiveAck(msg);     break;
        }
    }

    public enum Type { START, LOCAL_TS, ACK }

    public static class SkeenMsg {
        public Type        type;
        public String      msgId;
        public String      fromShard;
        public long        ts;
        public Set<String> dest;
        public SkeenMsg(Type type, String msgId, String fromShard, long ts) {
            this.type = type; this.msgId = msgId;
            this.fromShard = fromShard; this.ts = ts;
        }
    }

    public interface NetworkLayer {
        void send(SkeenMsg msg, String toShardId);
    }

    public interface DeliveryHandler {
        void deliver(String msgId, byte[] payload, String channelId);
    }
}
