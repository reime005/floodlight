package net.floodlightcontroller.mqtt;

import io.moquette.parser.proto.messages.ConnectIpPortMessage;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.TCP;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowAdd;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.OFPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static net.floodlightcontroller.mqtt.MqttModule.METER_ID_MQTT;
import static net.floodlightcontroller.mqtt.MqttModule.QUEUE_ID_MQTT;
import static net.floodlightcontroller.mqtt.MqttModule.QUEUE_ID_STREAM;

/**
 * Created by Marius Reimer on 25/03/17.
 */
public class MqttFlowPushService implements IMqttFlowPushService {

    protected IOFSwitchService switchService;
    private IMqttFlowBuildService flowBuildService;
    protected static Logger logger;

    public MqttFlowPushService(IOFSwitchService switchService, IMqttFlowBuildService flowBuildService) {
        this.switchService = switchService;
        this.flowBuildService = flowBuildService;
        logger = LoggerFactory.getLogger(getClass());
    }

    @Override
    public Match pushAndReturnFlows(DatapathId switchId, boolean flipIPv4, IPv4 iPv4, OFPort ofPort, Integer queueId, Integer meterId, String cookieName) {
        final IOFSwitch sw = switchService.getSwitch(switchId);
        final OFFactory myFactory = sw.getOFFactory();

        logger.info("Switch {} Port {}", switchId.toString(),
                ofPort.toString());

        final Match match = flowBuildService.buildMatch(myFactory, flipIPv4, iPv4);
        final OFFlowAdd flow = flowBuildService.buildFlowAdd(queueId, meterId, ofPort, myFactory, match, cookieName);

        sw.write(flow);
        logger.info("flowPusher: Flows were pushed");

        return match;
    }

    private void removeDefaultFlows(IOFSwitch sw, IPv4 iPv4) {
        final OFFactory factory = sw.getOFFactory();

        final Match forwardMatch = flowBuildService.buildMatch(factory, false, iPv4);
        final Match backwardMatch = flowBuildService.buildMatch(factory, true, iPv4);

        sw.write(factory.buildFlowDelete()
                .setMatch(forwardMatch)
                .setCookie(MqttFlowBuildService.buildCookie(MqttModule.COOKIE_NAME_DEFAULT))
                .build());

        sw.write(factory.buildFlowDelete()
                .setMatch(backwardMatch)
                .setCookie(MqttFlowBuildService.buildCookie(MqttModule.COOKIE_NAME_DEFAULT))
                .build());
    }

    @Override
    public Match pushStreamFlows(IPv4 iPv4, boolean flipIPv4, String clientId, DatapathId dataPathId, OFPort ofPort) {
        final IOFSwitch sw = switchService.getSwitch(dataPathId);

        if (sw == null) return null;

        // push the new flows
        final Match streamMatch = pushAndReturnFlows(sw.getId(), flipIPv4, iPv4,
                ofPort, QUEUE_ID_STREAM, MqttModule.METER_ID_STREAM, clientId);

        logger.info("Stream flows are pushed");

        return streamMatch;
    }

    @Override
    public Match pushMqttEstFlows(ConnectIpPortMessage msg, boolean flipIPv4, String clientId, DatapathId dataPathId, OFPort ofPort) {
        final IOFSwitch sw = switchService.getSwitch(dataPathId);

        if (sw == null) {
            return null;
        }

        final TCP tcp = new TCP()
                .setSourcePort(msg.getRemotePort())
                .setDestinationPort(msg.getLocalPort());

        // information part to the broker
        final IPv4 iPv4Tcp = new IPv4()
                .setProtocol(IpProtocol.TCP)
                .setSourceAddress(msg.getRemoteAddress())
                .setDestinationAddress(msg.getLocalAddress());
        iPv4Tcp.setPayload(tcp);

        // todo test if it causes socket closed exception
//        removeDefaultFlows(sw, iPv4Tcp);

        // push the new flows
        final Match mqttMatch = pushAndReturnFlows(sw.getId(), flipIPv4, iPv4Tcp,
                ofPort, QUEUE_ID_MQTT, METER_ID_MQTT, clientId);

        logger.info("New Flows are pushed");

        return mqttMatch;
    }

    @Override
    public void removeFlowsForCookie(DatapathId dataPathId, List<Match> clientIdMatches, String cookieName) {
        final IOFSwitch sw = switchService.getSwitch(dataPathId);
        if (sw == null) return;

        clientIdMatches.forEach(match -> sw.write(sw.getOFFactory().buildFlowDelete()
                .setMatch(match)
                .setCookie(MqttFlowBuildService.buildCookie(cookieName))
                .build()));

        logger.info("cleared flows for cookieName {} in switch {}", MqttFlowBuildService.buildCookie(cookieName), dataPathId);
    }
}
