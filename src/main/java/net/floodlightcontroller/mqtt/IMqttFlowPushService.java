package net.floodlightcontroller.mqtt;

import io.moquette.parser.proto.messages.ConnectIpPortMessage;
import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.packet.IPv4;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.OFPort;

import java.util.List;

/**
 * Created by Marius Reimer on 25/03/17.
 */
public interface IMqttFlowPushService {
    /**
     * Pushes a flow to a switch
     * @param switchId related switch ID
     * @param iPv4 iPv4 packet for the message
     * @param ofPort Output port of the switch
     * @param queueId OF queue id
     * @param meterId OF metering id
     * @param cookieName cookie name to be added    @return list of matches that were created
     */
    Match pushAndReturnFlows(DatapathId switchId, boolean flipIPv4, IPv4 iPv4, OFPort ofPort, Integer queueId, Integer meterId, String cookieName);

    Match pushStreamFlows(IPv4 iPv4, boolean flipIPv4, String clientId, DatapathId dataPathId, OFPort ofPort);

    Match pushMqttEstFlows(ConnectIpPortMessage msg, boolean flipIPv4, String clientId, DatapathId dataPathId, OFPort ofPort);

    void removeFlowsForCookie(DatapathId dataPathId, List<Match> clientIdMatches, String cookieName);
}
