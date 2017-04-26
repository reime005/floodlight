package net.floodlightcontroller.mqtt;

import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.packet.IPv4;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowAdd;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.OFPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

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
    public List<Match> pushAndReturnFlows(DatapathId switchId, IPv4 iPv4, OFPort ofPort, Integer queueId, Integer meterId, String cookieName) {
        final IOFSwitch sw = switchService.getSwitch(switchId);
        final OFFactory myFactory = sw.getOFFactory();

        logger.info("Switch {} Port {}", switchId.toString(),
                OFPort.ALL.toString());

        final Match forwardMatch = flowBuildService.buildMatch(myFactory, false, iPv4);
        final Match backwardMatch = flowBuildService.buildMatch(myFactory, true, iPv4);

        final OFFlowAdd forwardFlow = flowBuildService.buildFlowAdd(queueId, meterId, ofPort, myFactory, forwardMatch, cookieName);
        final OFFlowAdd backwardFlow = flowBuildService.buildFlowAdd(queueId, meterId, ofPort, myFactory, backwardMatch, cookieName);

        sw.write(forwardFlow);
        logger.info("flowPusher: Flows were pushed: {}", forwardFlow);

        // stream is a one direction udp flow (source -> sink)
        if (iPv4.getProtocol().equals(IpProtocol.UDP)) {
            return Arrays.asList(forwardMatch);
        } else {
            sw.write(backwardFlow);
            logger.info("flowPusher: Flows were pushed: {}", backwardFlow);
        }

        return Arrays.asList(forwardMatch, backwardMatch);
    }

    @Override
    public void removeDefaultFlows(IOFSwitch sw, IPv4 iPv4Tcp) {
        final OFFactory factory = sw.getOFFactory();

        final Match forwardMatch = flowBuildService.buildMatch(factory, true, iPv4Tcp);
        final Match backwardMatch = flowBuildService.buildMatch(factory, false, iPv4Tcp);

        sw.write(factory.buildFlowDelete()
                .setMatch(forwardMatch)
                .setCookie(MqttFlowBuildService.buildCookie(MqttModule.COOKIE_NAME_DEFAULT))
                .build());

        sw.write(factory.buildFlowDelete()
                .setMatch(backwardMatch)
                .setCookie(MqttFlowBuildService.buildCookie(MqttModule.COOKIE_NAME_DEFAULT))
                .build());
    }
}
