package net.floodlightcontroller.mqtt;

import de.ostfalia.remotecar.Constants;
import io.moquette.BrokerConstants;
import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.HazelcastInterceptHandler;
import io.moquette.interception.messages.InterceptConnectMessage;
import io.moquette.interception.messages.InterceptConnectionLostMessage;
import io.moquette.interception.messages.InterceptDisconnectMessage;
import io.moquette.parser.proto.messages.ConnectIpPortMessage;
import io.moquette.server.Server;
import io.moquette.server.config.ClasspathResourceLoader;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.IResourceLoader;
import io.moquette.server.config.ResourceLoaderConfig;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.PortChangeType;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.types.NodePortTuple;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.TCP;
import net.floodlightcontroller.packet.UDP;
import net.floodlightcontroller.routing.Path;
import org.projectfloodlight.openflow.protocol.OFMeterFlags;
import org.projectfloodlight.openflow.protocol.OFMeterModCommand;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static net.floodlightcontroller.mqtt.MqttModule.*;

/**
 * Created by Marius Reimer on 25/03/17.
 */
public class MqttSwitchService extends AbstractInterceptHandler implements IOFSwitchListener {

    private Set<DatapathId> switchSet; // collecting all activated switches
    private ClientSwitchMatches clientIdSwitchMatchMap;
    private IMqttFlowPushService pushService;
    private MqttRoutingService mqttRoutingService;
    private Map<IPv4Address, MacAddress> macAddressIPv4AddressMap; //todo tmp hack because devicemanager could not detect ipv4 address

    protected static Logger logger;
    protected IOFSwitchService switchService;

    public MqttSwitchService(IMqttFlowPushService pushService, IOFSwitchService switchService, MqttRoutingService mqttRoutingService) {
        logger = LoggerFactory.getLogger(getClass());
        this.pushService = pushService;
        this.switchService = switchService;
        this.mqttRoutingService = mqttRoutingService;
        switchSet = new HashSet<>();
        clientIdSwitchMatchMap = new ClientSwitchMatches();
        macAddressIPv4AddressMap = new HashMap<IPv4Address, MacAddress>();
        initBroker();
    }

    public IOFSwitchService getSwitchService() {
        return switchService;
    }

    /**
     * Starting the embedded MQTT broker with the hazelcast extension to receive IP and port for the clientId
     * Also attaches a shutdownHook to the runtime
     */
    private void initBroker() {
        IResourceLoader classpathLoader = new ClasspathResourceLoader();
        final IConfig classPathConfig = new ResourceLoaderConfig(classpathLoader);
        classPathConfig.setProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME, HazelcastInterceptHandler.class.getCanonicalName());

        Server mqttBrokerServer = new Server();

        new Thread(() -> {
            try {
                logger.info("Starting MQTT Broker Server...");
                mqttBrokerServer.startServer(classPathConfig, Collections.singletonList(this));
                logger.info("MQTT Broker Server Started.");
                mqttBrokerServer.setInterceptHandler(this);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping MQTT Broker Server...");
            if (mqttBrokerServer != null) {
                mqttBrokerServer.stopServer();
            }
            logger.info("MQTT Broker Server Stopped.");
        }));
    }

    @Override
    public void switchActivated(DatapathId switchId) {
        logger.info("switchActivated: {}", switchId);

        switchSet.add(switchId);
        // you might wanna install proactive flows here
//        testAddFlow(switchId);

        IOFSwitch sw = switchService.getActiveSwitch(switchId);
        // TODO: 18/03/17 temporary
        MeterUtils meterMqtt = new MeterUtils(sw, METER_ID_MQTT, new HashSet<>(Collections.singletonList(OFMeterFlags.KBPS)), 10000, 0);
        MeterUtils meterHazelcast = new MeterUtils(sw, METER_ID_HAZELCAST, new HashSet<>(Collections.singletonList(OFMeterFlags.KBPS)), 10000, 0);
        MeterUtils meterStream = new MeterUtils(sw, METER_ID_STREAM, new HashSet<>(Collections.singletonList(OFMeterFlags.KBPS)), 150000, 0);
        MeterUtils meterDefault = new MeterUtils(sw, METER_ID_DEFAULT, new HashSet<>(Collections.singletonList(OFMeterFlags.KBPS)), 10000, 0);

        meterMqtt.writeMeter(OFMeterModCommand.DELETE);
        meterHazelcast.writeMeter(OFMeterModCommand.DELETE);
        meterStream.writeMeter(OFMeterModCommand.DELETE);
        meterDefault.writeMeter(OFMeterModCommand.DELETE);

//        meterMqtt.writeMeter(OFMeterModCommand.ADD);
//        meterHazelcast.writeMeter(OFMeterModCommand.ADD);
//        meterStream.writeMeter(OFMeterModCommand.ADD);
//        meterDefault.writeMeter(OFMeterModCommand.ADD);

        logger.info("meters are installed");
    }

    @Override
    public void switchAdded(DatapathId switchId) {
        logger.info("switchAdded: {}", switchId);
    }

    @Override
    public void switchRemoved(DatapathId switchId) {
        logger.info("switchRemoved: {}", switchId);
    }

    @Override
    public void switchPortChanged(DatapathId switchId, OFPortDesc port, PortChangeType type) {
        logger.info("switchPortChanged: {}", switchId);
    }

    @Override
    public void switchChanged(DatapathId switchId) {
        logger.info("switchChanged: {}", switchId);
    }

    @Override
    public void switchDeactivated(DatapathId switchId) {
        logger.info("switchDeactivated: {}", switchId);
    }

    @Override
    public void onDisconnect(InterceptDisconnectMessage msg) {
        logger.info("MQTT BROKER onDisconnect: {}", msg.getUsername());
        deleteFlowsForCookie(msg.getClientID());
        super.onDisconnect(msg);
    }

    @Override
    public void onConnectionLost(InterceptConnectionLostMessage msg) {
        logger.info("MQTT BROKER onConnectionLost: {}", msg.getUsername());
        deleteFlowsForCookie(msg.getClientID());
        super.onConnectionLost(msg);
    }

    @Override
    public void onConnect(InterceptConnectMessage msg) {
        logger.info("MQTT BROKER onConnect: {}", msg.getUsername());

        processAddFlowsForIpPortMessage(msg.getIpPortMessage(), msg.getClientID());
//
        processRTPFlowPathCalculation();

        super.onConnect(msg);
    }

    /**
     * if sender and receiver are connected, proactively calculate a RTP/UDP flow
     */
    private void processRTPFlowPathCalculation() {
//        if (clientIdSwitchMatchMap.contains(Constants.DEFAULT_PUBLISH_CLIENT_ID) && clientIdSwitchMatchMap.contains(Constants.DEFAULT_SUBSCRIBE_CLIENT_ID)) {

            // stream goes from subscriber (cps) to publisher!
            final Match matchPublisher = clientIdSwitchMatchMap.getFirstMatch(Constants.DEFAULT_PUBLISH_CLIENT_ID);
            final Match matchSubscriber = clientIdSwitchMatchMap.getFirstMatch(Constants.DEFAULT_SUBSCRIBE_CLIENT_ID);

            if (matchPublisher != null && matchSubscriber != null) {
                logger.info("RTP/UDP flows are calculated and will be pushed proactively...");
                final UDP udp = new UDP()
                        .setDestinationPort(MqttModule.UDP_PORT_VR_STREAM)
                        .setSourcePort(TransportPort.FULL_MASK);

                final IPv4 iPv4 = (IPv4) new IPv4()
                        .setDestinationAddress(matchPublisher.get(MatchField.IPV4_SRC))
                        .setSourceAddress(matchSubscriber.get(MatchField.IPV4_SRC))
                        .setProtocol(IpProtocol.UDP)
                        .setPayload(udp);

                final Ethernet eth = (Ethernet) new Ethernet()
                        .setSourceMACAddress(matchSubscriber.get(MatchField.ETH_SRC))
                        .setDestinationMACAddress(matchPublisher.get(MatchField.ETH_SRC))
                        .setEtherType(EthType.IPv4)
                        .setPayload(iPv4);

                videoPktIn(eth);
//            }
        }
    }

    private void testAddFlow(DatapathId switchId) {
        final TCP tcp = new TCP()
                .setDestinationPort(TransportPort.FULL_MASK)
                .setSourcePort(1883);

        final IPv4 iPv4Udp = new IPv4()
                .setDestinationAddress("10.0.0.3")
                .setProtocol(IpProtocol.TCP);
        iPv4Udp.setPayload(tcp);

        //pushService.pushAndReturnFlows(switchId, iPv4Udp, OFPort.ALL, QUEUE_ID_MQTT, METER_ID_MQTT, COOKIE_NAME_MQTT);
    }

    /**
     * Removes the default 'low priority queued' mqtt flows and pushes 'higher prioritized flows'
     * @param receivedIpPortMessage the message that is received by hazelcast
     * @param clientId the MQTT client that has recently connected to the broker
     */
    private void processAddFlowsForIpPortMessage(ConnectIpPortMessage receivedIpPortMessage, String clientId) {
        if (receivedIpPortMessage == null) {
            return;
        }

        MacAddress srcMac = macAddressIPv4AddressMap.get(IPv4Address.of(receivedIpPortMessage.getRemoteAddress()));
        MacAddress dstMac = macAddressIPv4AddressMap.get(IPv4Address.of(receivedIpPortMessage.getLocalAddress()));

        // calculate the path between the client and broker
        Path forwardPath;
        Path backwardPath;

        if (srcMac == null || dstMac == null) {
            logger.warn("Map hack did not work");
            forwardPath = mqttRoutingService.getPath(
                    mqttRoutingService.getAttachmentPoints(IPv4Address.of(receivedIpPortMessage.getRemoteAddress())),
                    mqttRoutingService.getAttachmentPoints(IPv4Address.of(receivedIpPortMessage.getLocalAddress()))
            );
            backwardPath = mqttRoutingService.getPath(
                    mqttRoutingService.getAttachmentPoints(IPv4Address.of(receivedIpPortMessage.getLocalAddress())),
                    mqttRoutingService.getAttachmentPoints(IPv4Address.of(receivedIpPortMessage.getRemoteAddress()))
            );
        } else {
            logger.warn("Map hack did work");
            final NodePortTuple srcNode = mqttRoutingService.getAttachmentPoints(srcMac);
            final NodePortTuple dstNode = mqttRoutingService.getAttachmentPoints(dstMac);

            forwardPath = mqttRoutingService.getPath(
                    srcNode,
                    dstNode
            );
            backwardPath = mqttRoutingService.getPath(
                    dstNode,
                    srcNode
            );
        }

        if (forwardPath == null || backwardPath == null) {
            logger.error("Error in calculating the path!. Flooding!");
            // push flows to each connected switch, flood
            switchSet.forEach(dataPathId -> {
                clientIdSwitchMatchMap.putMatch(dataPathId, clientId, pushService.pushMqttEstFlows(receivedIpPortMessage, true, clientId, dataPathId, OFPort.ALL));
                clientIdSwitchMatchMap.putMatch(dataPathId, clientId, pushService.pushMqttEstFlows(receivedIpPortMessage, false, clientId, dataPathId, OFPort.ALL));
            });
        } else {
            System.out.println("push flows along the calculated path");
            // push flows along the calculated path
            forwardPath.getPath().forEach(nodePortTuple -> {
                clientIdSwitchMatchMap.putMatch(nodePortTuple.getNodeId(), clientId, pushService.pushMqttEstFlows(receivedIpPortMessage, false, clientId,
                    nodePortTuple.getNodeId(), nodePortTuple.getPortId()));
            });

            // push flows along the calculated path
            backwardPath.getPath().forEach(nodePortTuple -> {
                clientIdSwitchMatchMap.putMatch(nodePortTuple.getNodeId(), clientId, pushService.pushMqttEstFlows(receivedIpPortMessage, true, clientId,
                        nodePortTuple.getNodeId(), nodePortTuple.getPortId()));
            });
        }
    }

    /**
     * Deleting the flow(s) for the given cookie in all switches
     * @param cookieName cookie name of the flow(s) to be removed
     */
    private void deleteFlowsForCookie(String cookieName) {
        switchSet.forEach(dataPathId -> {
            // get the flows from the mqtt connect step. cookieName may be clientId
            final List<Match> clientIdMatches = clientIdSwitchMatchMap.getFor(cookieName, dataPathId);

            if (clientIdMatches != null) {
                // send remove message to the switch
                pushService.removeFlowsForCookie(dataPathId, clientIdMatches, cookieName);

                // remove the matches from the list
                clientIdSwitchMatchMap.remove(cookieName, dataPathId);
            }
        });
    }

    /**
     * Process an incoming ethernet packet for the video stream flow
     * @param eth incoming ethernet packet
     */
    public void videoPktIn(final Ethernet eth) {
        final IPv4 iPv4 = (IPv4) eth.getPayload();

        // source ip is always the sender (cps), thus we must look for the client id to match
        // goal: group the rtp flow to the cps client id
        String clientId = clientIdSwitchMatchMap.getClientIdForIpV4Src(iPv4.getSourceAddress());

        if (clientId != null) {
            Path path = mqttRoutingService.getPath(
                    mqttRoutingService.getAttachmentPoints(iPv4.getSourceAddress()),
                    mqttRoutingService.getAttachmentPoints(iPv4.getDestinationAddress())
            );

            if (path == null) {
                logger.info("RTP map hack try...");
                path = mqttRoutingService.getPath(
                        mqttRoutingService.getAttachmentPoints(macAddressIPv4AddressMap.get(iPv4.getSourceAddress())),
                        mqttRoutingService.getAttachmentPoints(macAddressIPv4AddressMap.get(iPv4.getDestinationAddress()))
                );
            }

            if (path == null) {
                logger.error("Error in calculating the stream path! Flooding.");
                // push flows to each connected switch
                switchSet.forEach(dataPathId -> clientIdSwitchMatchMap.putMatch(dataPathId, clientId, pushService.pushStreamFlows(iPv4, false, clientId, dataPathId, OFPort.ALL)));
            } else {
                // push flows along the calculated path
                path.getPath().forEach(nodePortTuple -> {
                    clientIdSwitchMatchMap.putMatch(nodePortTuple.getNodeId(), clientId, pushService.pushStreamFlows(iPv4, false, clientId,
                            nodePortTuple.getNodeId(), nodePortTuple.getPortId()));
                });
            }
        }
    }

    public void addMacAddress(Ethernet eth) {
        IPv4 iPv4 = (IPv4) eth.getPayload();
        macAddressIPv4AddressMap.put(iPv4.getSourceAddress(), eth.getSourceMACAddress());
        macAddressIPv4AddressMap.put(iPv4.getDestinationAddress(), eth.getDestinationMACAddress());
    }
}
