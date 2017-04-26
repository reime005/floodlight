package net.floodlightcontroller.mqtt;

import net.floodlightcontroller.packet.IPv4;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowAdd;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.types.OFPort;

/**
 * Created by Marius Reimer on 25/03/17.
 */
public interface IMqttFlowBuildService {

    /**
     * Builds a match for a flow entry
     * @param factory for the correct OF version related to the switch's preference
     * @param flipIPv4 TRUE keeps the iPv4 direction, FALSE flips dst and src
     * @param iPv4 iPv4 packet
     * @return the match
     */
    Match buildMatch(OFFactory factory, boolean flipIPv4, IPv4 iPv4);

    /**
     * Creates a flow to be added to a switch
     * @param queueId queue to be put in
     * @param meterId meter to be added
     * @param port port to be forwarded to
     * @param factory for the correct OF version related to the switch's preference
     * @param match match for the flow
     * @param cookieName to group and identify a flow entry
     * @return add flow
     */
    OFFlowAdd buildFlowAdd(Integer queueId, Integer meterId, OFPort port, OFFactory factory, Match match, String cookieName);
}
