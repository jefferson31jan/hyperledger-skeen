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

        // Inicializar BlockCutter lendo o genesis block do canal do sistema
        try {
            java.io.File genesisFile = new java.io.File(configDir + "genesisblock");
            if (genesisFile.exists()) {
                org.hyperledger.fabric.protos.common.Common.Block genesis =
                    org.hyperledger.fabric.protos.common.Common.Block.parseFrom(
                        java.nio.file.Files.readAllBytes(genesisFile.toPath()));
                org.hyperledger.fabric.protos.common.Configtx.ConfigEnvelope conf =
                    bft.util.BFTCommon.extractConfigEnvelope(genesis);
                org.hyperledger.fabric.protos.orderer.Configuration.BatchSize batchSize =
                    bft.util.BFTCommon.extractBachSize(conf.getConfig());
                org.hyperledger.fabric.protos.common.Common.ChannelHeader chanHeader =
                    org.hyperledger.fabric.protos.common.Common.ChannelHeader.parseFrom(
                        org.hyperledger.fabric.protos.common.Common.Payload.parseFrom(
                            org.hyperledger.fabric.protos.common.Common.Envelope.parseFrom(
                                genesis.getData().getData(0)).getPayload())
                        .getHeader().getChannelHeader());
                String sysChannel = chanHeader.getChannelId();
                this.blockCutter.setBatchParms(sysChannel,
                    batchSize.getPreferredMaxBytes(), batchSize.getMaxMessageCount());
                logger.info("BlockCutter inicializado — canal={} preferredMaxBytes={} maxMsgCount={}",
                    sysChannel, batchSize.getPreferredMaxBytes(), batchSize.getMaxMessageCount());
            } else {
                logger.warn("Genesis block não encontrado em {}", configDir + "genesisblock");
                // fallback: usar valores padrão do Fabric
                this.blockCutter.setBatchParms("bftchannel", 512 * 1024L, 10L);
                this.blockCutter.setBatchParms("channel47",  512 * 1024L, 10L);
            }
        } catch (Exception e) {
            logger.error("Falha ao inicializar BlockCutter com genesis", e);
            this.blockCutter.setBatchParms("bftchannel", 512 * 1024L, 10L);
            this.blockCutter.setBatchParms("channel47",  512 * 1024L, 10L);
        }

        SkeenNode.DeliveryHandler onDeliver = (msgId, payload, channelId) -> {
            logger.info("Skeen entregou msgId={} canal={} — repassando ao BFTNode via AsynchServiceProxy", msgId, channelId);
            try {
                // Após ordenação pelo Skeen, enviar ao BFTNode via AsynchServiceProxy
                // O BFTNode gera o bloco e coloca na SkeenBlockQueue
                bft.skeen.SkeenDeliveryQueue.getInstance().put(channelId, payload);
            } catch (Exception e) {
                logger.error("Erro ao enfileirar envelope entregue pelo Skeen", e);
            }
        };

        this.network   = new SkeenNetwork(shardId, localPort, null);
        this.skeenNode = new SkeenNode(shardId, this.network, onDeliver);

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

        network.setNode(skeenNode);

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