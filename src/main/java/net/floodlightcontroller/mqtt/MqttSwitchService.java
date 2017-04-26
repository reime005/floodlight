package net.floodlightcontroller.mqtt;

import io.moquette.BrokerConstants;
import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.HazelcastInterceptHandler;
import io.moquette.interception.messages.InterceptConnectionLostMessage;
import io.moquette.interception.messages.InterceptDisconnectMessage;
import io.moquette.interception.messages.InterceptPublishMessage;
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
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.TCP;
import net.floodlightcontroller.packet.UDP;
import net.floodlightcontroller.routing.Path;
import org.projectfloodlight.openflow.protocol.OFMeterFlags;
import org.projectfloodlight.openflow.protocol.OFMeterModCommand;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import static net.floodlightcontroller.mqtt.MqttModule.*;

/**
 * Created by Marius Reimer on 25/03/17.
 */
public class MqttSwitchService extends AbstractInterceptHandler implements IOFSwitchListener {

    private Set<DatapathId> switchSet; // collecting all activated switches
    private Map<String, List<Match>> clientIdMatchMap;
    private IMqttFlowPushService pushService;
    private MqttRoutingService mqttRoutingService;

    protected static Logger logger;
    protected IOFSwitchService switchService;

    public MqttSwitchService(IMqttFlowPushService pushService, IOFSwitchService switchService, MqttRoutingService mqttRoutingService) {
        logger = LoggerFactory.getLogger(getClass());
        this.pushService = pushService;
        this.switchService = switchService;
        this.mqttRoutingService = mqttRoutingService;
        switchSet = new HashSet<>();
        clientIdMatchMap = new HashMap<>();
        initBroker();
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

        final IOFSwitch sw = switchService.getActiveSwitch(switchId);

        /* Metering if OF version at least 1.3 */
        if (sw.getOFFactory().getVersion().compareTo(OFVersion.OF_13) >= 0) {
            MeterUtils meterMqtt = new MeterUtils(sw, MqttModule.METER_ID_MQTT, new HashSet<>(Collections.singletonList(OFMeterFlags.KBPS)), 10000, 0);
            MeterUtils meterHazelcast = new MeterUtils(sw, MqttModule.METER_ID_HAZELCAST, new HashSet<>(Collections.singletonList(OFMeterFlags.KBPS)), 10000, 0);
            MeterUtils meterStream = new MeterUtils(sw, MqttModule.METER_ID_STREAM, new HashSet<>(Collections.singletonList(OFMeterFlags.KBPS)), 150000, 0);
            MeterUtils meterDefault = new MeterUtils(sw, MqttModule.METER_ID_DEFAULT, new HashSet<>(Collections.singletonList(OFMeterFlags.KBPS)), 10000, 0);

            meterMqtt.writeMeter(OFMeterModCommand.DELETE);
            meterHazelcast.writeMeter(OFMeterModCommand.DELETE);
            meterStream.writeMeter(OFMeterModCommand.DELETE);
            meterDefault.writeMeter(OFMeterModCommand.DELETE);

            meterMqtt.writeMeter(OFMeterModCommand.ADD);
            meterHazelcast.writeMeter(OFMeterModCommand.ADD);
            meterStream.writeMeter(OFMeterModCommand.ADD);
            meterDefault.writeMeter(OFMeterModCommand.ADD);

            logger.info("meters are installed");
        }
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
    public void onPublish(InterceptPublishMessage msg) {
        super.onPublish(msg);
        logger.info("MQTT BROKER onPublish: {}", msg.getUsername());

        if (!msg.getTopicName().equals(BrokerConstants.HAZELCAST_TOPIC_IP_PORT)) {
            return;
        }

        try {
            ConnectIpPortMessage receivedIpPortMessage = ConnectIpPortMessage.asString(new String(msg.getPayload().array(), Charset.forName("UTF-8")));
            logger.info("processAddFlowsForIpPortMessage");
            processAddFlowsForIpPortMessage(receivedIpPortMessage, msg.getClientID());
        } catch (Exception e) {
            e.printStackTrace();
        }
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

        final Path path = mqttRoutingService.getPath(
                mqttRoutingService.getAttachmentPoints(IPv4Address.of(receivedIpPortMessage.getLocalAddress())),
                mqttRoutingService.getAttachmentPoints(IPv4Address.of(receivedIpPortMessage.getRemoteAddress()))
        );

        if (path == null) {
            logger.error("Error in calculating the path!");
            return;
        }

        path.getPath().forEach(nodePortTuple -> {
            final IOFSwitch sw = switchService.getSwitch(nodePortTuple.getNodeId());

            if (sw == null) return;

            final UDP udp = new UDP()
                    .setDestinationPort(MqttModule.UDP_PORT_VR_STREAM)
                    .setSourcePort(TransportPort.FULL_MASK);

            final UDP udp2 = new UDP()
                    .setDestinationPort(TransportPort.FULL_MASK)
                    .setSourcePort(MqttModule.UDP_PORT_VR_STREAM);

            final TCP tcp = new TCP()
                    .setSourcePort(receivedIpPortMessage.getLocalPort())
                    .setDestinationPort(receivedIpPortMessage.getRemotePort());

            final TCP tcpReverse = new TCP()
                    .setSourcePort(receivedIpPortMessage.getRemotePort())
                    .setDestinationPort(receivedIpPortMessage.getLocalPort());

            // stream
            final IPv4 iPv4Udp = new IPv4()
                    .setProtocol(IpProtocol.UDP)
                    .setSourceAddress(receivedIpPortMessage.getRemoteAddress())
                    .setDestinationAddress(receivedIpPortMessage.getLocalAddress());
            iPv4Udp.setPayload(udp);

            // stream
            final IPv4 iPv4Udp2 = new IPv4()
                    .setProtocol(IpProtocol.UDP)
                    .setSourceAddress(receivedIpPortMessage.getRemoteAddress())
                    .setDestinationAddress(receivedIpPortMessage.getLocalAddress());
            iPv4Udp2.setPayload(udp2);

            // mqtt
            final IPv4 iPv4Tcp = new IPv4()
                    .setProtocol(IpProtocol.TCP)
                    .setSourceAddress(receivedIpPortMessage.getRemoteAddress())
                    .setDestinationAddress(receivedIpPortMessage.getLocalAddress());
            iPv4Tcp.setPayload(tcp);

            // mqtt
            final IPv4 ipv4TcpReverse = new IPv4();
            ipv4TcpReverse.setProtocol(IpProtocol.TCP);
            ipv4TcpReverse.setSourceAddress(receivedIpPortMessage.getRemoteAddress());
            ipv4TcpReverse.setDestinationAddress(receivedIpPortMessage.getLocalAddress());
            ipv4TcpReverse.setPayload(tcpReverse);

            pushService.removeDefaultFlows(sw, iPv4Tcp);

            // push the new flows
            final List<Match> mqttMatches = pushService.pushAndReturnFlows(sw.getId(), iPv4Tcp,
                    nodePortTuple.getPortId(), MqttModule.QUEUE_ID_MQTT, MqttModule.METER_ID_MQTT, clientId);
            final List<Match> mqttMatchesReverse = pushService.pushAndReturnFlows(sw.getId(), ipv4TcpReverse,
                    nodePortTuple.getPortId(), QUEUE_ID_MQTT, MqttModule.METER_ID_MQTT, clientId);
            final List<Match> streamMatches = pushService.pushAndReturnFlows(sw.getId(), iPv4Udp,
                    nodePortTuple.getPortId(), MqttModule.QUEUE_ID_STREAM, MqttModule.METER_ID_STREAM, clientId);
            final List<Match> streamMatches2 = pushService.pushAndReturnFlows(sw.getId(), iPv4Udp2,
                    nodePortTuple.getPortId(), MqttModule.QUEUE_ID_STREAM, MqttModule.METER_ID_STREAM, clientId);
            logger.info("New Flows are pushed");

            final List<Match> mergedList = new ArrayList<>(mqttMatches);
            mergedList.addAll(mqttMatchesReverse);
            mergedList.addAll(streamMatches);
            mergedList.addAll(streamMatches2);

            clientIdMatchMap.put(clientId, mergedList);
        });
    }

    /**
     * Deleting the flow(s) for the given cookie in all switches
     * @param cookieName cookie name of the flow(s) to be removed
     */
    private void deleteFlowsForCookie(String cookieName) {
        switchSet.forEach(dataPathId -> {
            final IOFSwitch sw = switchService.getSwitch(dataPathId);
            if (sw == null) return;

            // get the flows from the mqtt connect step
            final List<Match> clientIdMatches = clientIdMatchMap.get(cookieName);

            if (clientIdMatches == null) {
                return;
            }

            clientIdMatches.remove(cookieName);

            clientIdMatches.forEach(match -> sw.write(sw.getOFFactory().buildFlowDelete()
                    .setMatch(match)
                    .setCookie(MqttFlowBuildService.buildCookie(cookieName))
                    .build()));

            logger.info("cleared flows for cookieName {} in switch {}", cookieName, dataPathId);
        });
    }

    @Override
    public void onConnectionLost(InterceptConnectionLostMessage msg) {
        logger.info("MQTT BROKER onConnectionLost: {}", msg.getUsername());
        deleteFlowsForCookie(msg.getClientID());
        super.onConnectionLost(msg);
    }
}
