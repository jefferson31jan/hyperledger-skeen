package bft.skeen;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Fila de envelopes ordenados pelo Skeen aguardando envio ao BFTNode.
 * O SkeenOrderingNode produz, o ReceiverThread do BFTProxy consome.
 */
public class SkeenDeliveryQueue {

    private static final SkeenDeliveryQueue INSTANCE = new SkeenDeliveryQueue();
    private final BlockingQueue<Map.Entry<String, byte[]>> queue =
        new LinkedBlockingQueue<>();

    private SkeenDeliveryQueue() {}

    public static SkeenDeliveryQueue getInstance() { return INSTANCE; }

    public void put(String channelId, byte[] payload) throws InterruptedException {
        queue.put(new SimpleEntry<>(channelId, payload));
    }

    public Map.Entry<String, byte[]> take() throws InterruptedException {
        return queue.take();
    }

    public boolean isEmpty() { return queue.isEmpty(); }
}
