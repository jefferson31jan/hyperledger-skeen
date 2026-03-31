package bft.skeen;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

/**
 * Singleton que recebe envelopes do BFTProxy
 * e os repassa ao SkeenOrderingNode.
 */
public class SkeenProxy {

    private static final Logger logger = LoggerFactory.getLogger(SkeenProxy.class);
    private static final SkeenProxy INSTANCE = new SkeenProxy();

    private SkeenOrderingNode skeenNode = null;

    // shards destino padrão — lidos do skeen.config
    private Set<String> defaultDest = new HashSet<>();

    private SkeenProxy() {}

    public static SkeenProxy getInstance() {
        return INSTANCE;
    }

    /** Chamado na inicialização do BFTProxy para registrar o nó Skeen */
    public void init(SkeenOrderingNode node, Set<String> dest) {
        this.skeenNode  = node;
        this.defaultDest = dest;
        logger.info("SkeenProxy inicializado — destinos={}", dest);
    }

    /** Chamado pelo BFTProxy.ReceiverThread para cada envelope recebido do Go */
    public void submitEnvelope(byte[] env, String channelId, boolean isConfig) {
        if (skeenNode == null) {
            logger.error("SkeenProxy não inicializado — envelope descartado");
            return;
        }
        logger.debug("Submetendo envelope canal={} isConfig={}", channelId, isConfig);
        skeenNode.submitEnvelope(env, channelId, defaultDest);
    }
}
