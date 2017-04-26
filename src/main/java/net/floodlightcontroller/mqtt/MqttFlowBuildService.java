package net.floodlightcontroller.mqtt;

import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.TCP;
import net.floodlightcontroller.packet.UDP;
import net.floodlightcontroller.staticentry.StaticEntries;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.OFFlowAdd;
import org.projectfloodlight.openflow.protocol.OFVersion;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.action.OFActionSetQueue;
import org.projectfloodlight.openflow.protocol.instruction.OFInstruction;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructionApplyActions;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructionMeter;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.*;

import java.util.ArrayList;

/**
 * Created by Marius Reimer on 25/03/17.
 */
public class MqttFlowBuildService implements IMqttFlowBuildService {

    @Override
    public Match buildMatch(OFFactory factory, boolean flipIPv4, IPv4 iPv4) {
        final Match.Builder matchBuilder = factory.buildMatch()
                .setExact(MatchField.ETH_TYPE, EthType.IPv4);

        // layer 4 part
        if (iPv4.getProtocol() == IpProtocol.TCP) {
            matchBuilder.setExact(MatchField.IP_PROTO, IpProtocol.TCP)
                    .setExact(MatchField.TCP_DST, ((TCP) iPv4.getPayload()).getDestinationPort())
                    .setExact(MatchField.TCP_SRC, ((TCP) iPv4.getPayload()).getSourcePort());

        } else if (iPv4.getProtocol() == IpProtocol.UDP) {
            matchBuilder.setExact(MatchField.IP_PROTO, IpProtocol.UDP)
                    .setExact(MatchField.UDP_DST, ((UDP) iPv4.getPayload()).getDestinationPort())
                    .setExact(MatchField.UDP_SRC, ((UDP) iPv4.getPayload()).getSourcePort());
        }

        // layer 3 part
        if (flipIPv4) {
            matchBuilder.setExact(MatchField.IPV4_SRC, iPv4.getDestinationAddress())
                    .setExact(MatchField.IPV4_DST, iPv4.getSourceAddress());
        } else {
            matchBuilder.setExact(MatchField.IPV4_DST, iPv4.getDestinationAddress())
                    .setExact(MatchField.IPV4_SRC, iPv4.getSourceAddress());
        }

        return matchBuilder.build();
    }

    @Override
    public OFFlowAdd buildFlowAdd(Integer queueId, Integer meterId, OFPort port, OFFactory factory, Match match, String cookieName) {
        final ArrayList<OFAction> actionsList = new ArrayList<>();
        final ArrayList<OFInstruction> instructionsList = new ArrayList<>();

        /* No Actions means drop */

        // queue to be put in
        final OFActionSetQueue setQueue = factory.actions().buildSetQueue()
                .setQueueId(queueId).build();

        // port to be forwarded to
        final OFActionOutput setPort = factory.actions().buildOutput()
                .setPort(port)
                .setMaxLen(Integer.MAX_VALUE)
                .build();

        // meter to be added
        final OFInstructionMeter meter = factory.instructions().buildMeter()
                .setMeterId(meterId).build();

        if (queueId != -1) {
            actionsList.add(setQueue);
        }
        actionsList.add(setPort);

        // build apply actions appropriate to the OF version
        final OFInstructionApplyActions applyActions = factory.instructions()
                .buildApplyActions()
                .setActions(actionsList)
                .build();
        if (meterId != -1 && factory.getVersion().compareTo(OFVersion.OF_13) >= 0) {
            instructionsList.add(meter);
        }
        instructionsList.add(applyActions);

        /*
         * The cookie is a opaque data value chosen by the controller. It may be
         * used by the controller to filter flow statistics, flow modification
         * and flow deletion.
         */

        /*
         * OpenFlow packets are received on an ingress port and processed by the
         * OpenFlow pipeline which may forward them to an output port.
         */

        int idleTimeout;
        int hardTimeout;

        switch (cookieName) {
            case MqttModule.COOKIE_NAME_MQTT:
                idleTimeout = MqttModule.MQTT_FLOW_IDLE_TIMEOUT_SECONDS;
                hardTimeout = MqttModule.MQTT_FLOW_HARD_TIMEOUT_SECONDS;
                break;
            case MqttModule.COOKIE_NAME_HAZELCAST:
                idleTimeout = MqttModule.HAZELCAST_FLOW_IDLE_TIMEOUT_SECONDS;
                hardTimeout = MqttModule.HAZELCAST_FLOW_HARD_TIMEOUT_SECONDS;
                break;
            case MqttModule.COOKIE_NAME_STREAM:
                idleTimeout = MqttModule.STREAM_FLOW_IDLE_TIMEOUT_SECONDS;
                hardTimeout = MqttModule.STREAM_FLOW_HARD_TIMEOUT_SECONDS;
                break;
            case MqttModule.COOKIE_NAME_DEFAULT:
                idleTimeout = MqttModule.DEFAULT_FLOW_IDLE_TIMEOUT_SECONDS;
                hardTimeout = MqttModule.DEFAULT_FLOW_HARD_TIMEOUT_SECONDS;
                break;
            default:
                idleTimeout = MqttModule.CLIENT_ID_FLOW_IDLE_TIMEOUT_SECONDS;
                hardTimeout = MqttModule.CLIENT_ID_FLOW_HARD_TIMEOUT_SECONDS;
        }

        final OFFlowAdd.Builder fmb = factory.buildFlowAdd()
                .setMatch(match)
                .setCookie(buildCookie(cookieName))
                .setIdleTimeout(idleTimeout)
                .setHardTimeout(hardTimeout)
                .setOutPort(OFPort.ANY)
                .setPriority(Integer.MAX_VALUE)
                .setBufferId(OFBufferId.NO_BUFFER);

        if (factory.getVersion().compareTo(OFVersion.OF_11) >= 0) {
            // Instructions are used starting in OF 1.1
            fmb.setInstructions(instructionsList);
        } else {
            // OF 1.0 only supports actions
            fmb.setActions(actionsList);
        }

        return fmb.build();
    }



    /**
     * Generic cookie generator
     * @param cookieName name of the cookie to be generated
     * @return U64 cookie for a flow entry
     */
    static U64 buildCookie(String cookieName) {
        return StaticEntries.computeEntryCookie(0, cookieName);
    }
}
