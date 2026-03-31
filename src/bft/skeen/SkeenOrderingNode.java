package bft.skeen;

import bft.util.BFTCommon;
import bft.util.BlockCutter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.security.Security;
import java.util.*;

public class SkeenOrderingNode {

    private static final Logger logger =
        LoggerFactory.getLogger(SkeenOrderingNode.class);

    private final SkeenNode    skeenNode;
    private final SkeenNetwork network;
    private final BlockCutter  blockCutter;

    public SkeenOrderingNode(String shardId, String configDir) throws Exception {

        Properties props = new Properties();
        props.load(new FileInputStream(configDir + "skeen.config"));

        int localPort = Integer.parseInt(
            props.getProperty("shard." + shardId + ".port", "12000"));

        this.blockCutter = BlockCutter.getInstance();

        SkeenNode.DeliveryHandler onDeliver = (msgId, payload, channelId) -> {
            logger.debug("Skeen entregou msgId={} canal={}", msgId, channelId);
            try {
                List<byte[][]> batches = blockCutter.ordered(channelId, payload, false);
                if (!batches.isEmpty()) {
                    logger.info("Bloco gerado com {} batches para canal {}",
                        batches.size(), channelId);
                }
            } catch (Exception e) {
                logger.error("Erro ao processar envelope", e);
            }
        };

        this.skeenNode = new SkeenNode(shardId, null, onDeliver);
        this.network   = new SkeenNetwork(shardId, localPort, skeenNode);

        for (String key : props.stringPropertyNames()) {
            if (key.endsWith(".host")) {
                String sid  = key.replace("shard.", "").replace(".host", "");
                if (sid.equals(shardId)) continue;
                String host = props.getProperty("shard." + sid + ".host");
                int    port = Integer.parseInt(
                    props.getProperty("shard." + sid + ".port", "12000"));
                network.addPeer(sid, host, port);
                logger.info("Peer: {} -> {}:{}", sid, host, port);
            }
        }

        Field field = SkeenNode.class.getDeclaredField("net");
        field.setAccessible(true);
        field.set(skeenNode, network);

        network.start();
        logger.info("SkeenOrderingNode pronto — shard={} porta={}", shardId, localPort);
    }

    public void submitEnvelope(byte[] payload, String channelId,
                               Set<String> destShards) {
        String msgId = UUID.randomUUID().toString();
        skeenNode.multicast(msgId, payload, channelId, destShards);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Uso: java bft.skeen.SkeenOrderingNode <shardId>");
            System.exit(1);
        }
        Security.addProvider(new BouncyCastleProvider());
        String shardId   = args[0];
        String configDir = BFTCommon.getBFTSMaRtConfigDir("NODE_CONFIG_DIR");
        if (System.getProperty("logback.configurationFile") == null)
            System.setProperty("logback.configurationFile",
                configDir + "logback.xml");
        new SkeenOrderingNode(shardId, configDir);
        Thread.currentThread().join();
    }
}