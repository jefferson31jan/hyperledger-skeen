package bft.skeen;

import org.hyperledger.fabric.protos.common.Common;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Fila compartilhada entre SkeenOrderingNode (produtor)
 * e BFTProxy.SenderThread (consumidor).
 * Substitui o sysProxy.getNext() do ProxyReplyListener.
 */
public class SkeenBlockQueue {

    private static final SkeenBlockQueue INSTANCE = new SkeenBlockQueue();
    private final BlockingQueue<Map.Entry<String, Common.Block>> queue =
        new LinkedBlockingQueue<>();

    private SkeenBlockQueue() {}

    public static SkeenBlockQueue getInstance() {
        return INSTANCE;
    }

    /** Chamado pelo BFTNode/SkeenOrderingNode ao gerar um bloco */
    public void put(String channelId, boolean isConfig, Common.Block block)
            throws InterruptedException {
        queue.put(new SimpleEntry<>(channelId + ":" + isConfig, block));
    }

    /** Chamado pelo BFTProxy.SenderThread — bloqueia até ter um bloco */
    public Map.Entry<String, Common.Block> getNext() throws InterruptedException {
        return queue.take();
    }
}