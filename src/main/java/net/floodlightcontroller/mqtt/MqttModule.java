package net.floodlightcontroller.mqtt;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.TCP;
import net.floodlightcontroller.packet.UDP;
import net.floodlightcontroller.restserver.IRestApiService;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Path;
import net.floodlightcontroller.topology.ITopologyService;
import net.floodlightcontroller.util.OFMessageUtils;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPacketIn;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.OFPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Created by mariu on 04.01.2017.
 */
public class MqttModule implements IFloodlightModule, IOFMessageListener {

    // ports
    static final short UDP_PORT_VR_STREAM = 1337;
    static final short TCP_PORT_MQTT = 1883;
    static final short TCP_PORT_HAZELCAST = 5701;
    static final short UDP_PORT_BEST_EFFORT = 12345;
    static final short TCP_PORT_BEST_EFFORT = 12355;

    // openflow queue ids
    static final int QUEUE_ID_MQTT = 7;
    static final int QUEUE_ID_HAZELCAST = 5;
    static final int QUEUE_ID_STREAM = 6;
    static final int QUEUE_ID_DEFAULT = 4;
    static final int QUEUE_ID_NONE = -1;

    // openflow metering ids
    static final int METER_ID_MQTT = 0;
    static final int METER_ID_STREAM = 2;
    static final int METER_ID_HAZELCAST = 1;
    static final int METER_ID_DEFAULT = 3;
    static final int METER_ID_NONE = -1;

    // flow cookie names for grouping and identifying flows
    static final String COOKIE_NAME_MQTT = "mqtt";
    static final String COOKIE_NAME_HAZELCAST = "hazelcast";
    static final String COOKIE_NAME_STREAM = "stream";
    static final String COOKIE_NAME_DEFAULT = "default";
    static final String COOKIE_NAME_BEST_EFFORT = "best_effort";

    /*
      * A non-zero idle timeout causes the flow entry to be removed when it
      * has matched no packets in the given number of seconds and the switch
      * must note the arrival time of the last packet associated with the
      * flow, as it may not evict the entry later.
      */
    static final int DEFAULT_FLOW_IDLE_TIMEOUT_SECONDS = 25;

    /*
     * A non-zero hard timeout causes the flow entry to be removed after the
     * given number of seconds, regardless of how many packets it has
     * matched.
     */
    static final int DEFAULT_FLOW_HARD_TIMEOUT_SECONDS = 0;

    static final int MQTT_FLOW_IDLE_TIMEOUT_SECONDS = 25;
    static final int MQTT_FLOW_HARD_TIMEOUT_SECONDS = 0;

    static final int STREAM_FLOW_IDLE_TIMEOUT_SECONDS = 600;
    static final int STREAM_FLOW_HARD_TIMEOUT_SECONDS = 0;

    static final int HAZELCAST_FLOW_IDLE_TIMEOUT_SECONDS = 25;
    static final int HAZELCAST_FLOW_HARD_TIMEOUT_SECONDS = 0;

    static final int CLIENT_ID_FLOW_IDLE_TIMEOUT_SECONDS = 25;
    static final int CLIENT_ID_FLOW_HARD_TIMEOUT_SECONDS = 0;

    private MqttSwitchService mqttSwitchService;
    private MqttRoutingService mqttRoutingService;
    private IMqttFlowPushService pushService;

    protected static Logger logger;
    protected IFloodlightProviderService floodlightProvider;
    protected IOFSwitchService switchService;
    protected IRestApiService restApiService;

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public boolean isCallbackOrderingPrereq(OFType type, String name) {
        return false;
    }

    @Override
    public boolean isCallbackOrderingPostreq(OFType type, String name) {
        return false;
    }

    private void pushPath(IOFSwitch sw, IDevice srcDevice, IDevice dstDevice, IPv4 iPv4, int queueId, int meterId, String cookieName) {
        Path pathForward = mqttRoutingService.getPath(srcDevice, dstDevice);
        Path pathBackward = mqttRoutingService.getPath(dstDevice, srcDevice);

        if (pathForward == null || pathBackward == null) {
            logger.warn("Could not calculate the flow path. Flooding.");
            pushService.pushAndReturnFlows(sw.getId(), false, iPv4, OFPort.ALL, queueId, meterId, cookieName);
            pushService.pushAndReturnFlows(sw.getId(), true, iPv4, OFPort.ALL, queueId, meterId, cookieName);
        } else {
            pathForward.getPath().forEach(nodePortTuple -> {
                pushService.pushAndReturnFlows(nodePortTuple.getNodeId(), false, iPv4, nodePortTuple.getPortId(), queueId, meterId, cookieName);
            });
//
            pathBackward.getPath().forEach(nodePortTuple -> {
                pushService.pushAndReturnFlows(nodePortTuple.getNodeId(), true, iPv4, nodePortTuple.getPortId(), queueId, meterId, cookieName);
            });
        }
    }

    /*
     * A PacketIn message is the OpenFlow message that is sent from the switch
     * to the controller if the switch does not have a flow table rule that
     * matches the packet The controller is expected to handle the packet and to
     * install any necessary flow table entries
     */
    @Override
    public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext context) {
        switch (msg.getType()) {
            case PACKET_IN:

                /* Retrieve the deserialized packet in message */
                final Ethernet eth = IFloodlightProviderService.bcStore.get(context,  IFloodlightProviderService.CONTEXT_PI_PAYLOAD);

                final IDevice dstDevice = IDeviceService.fcStore.get(context, IDeviceService.CONTEXT_DST_DEVICE);
                final IDevice srcDevice = IDeviceService.fcStore.get(context, IDeviceService.CONTEXT_SRC_DEVICE);
                final OFPort srcOFPort = OFMessageUtils.getInPort((OFPacketIn)msg);

                /*
                 * Check the ethertype of the Ethernet frame and retrieve the
                 * appropriate payload. Note the shallow equality check. EthType
                 * caches and reuses instances for valid types.
                 */
                if (eth.getEtherType() == EthType.IPv4) {
                    /* We got an IPv4 packet; get the payload from Ethernet */
                    final IPv4 ipv4 = (IPv4) eth.getPayload();

//                    logger.info("received IPv4 msg from ip {} to {} ", ipv4.getSourceAddress(), ipv4.getDestinationAddress());
                    /*
                     * Check the IP protocol version of the IPv4 packet's
                     * payload.
                     */
                    if (ipv4.getProtocol() == IpProtocol.TCP) {
                        /* We got a TCP packet; get the payload from IPv4 */
                        final TCP tcp = (TCP) ipv4.getPayload();

                        /* Various getters and setters that are exposed in TCP */
                        final int srcPort = tcp.getSourcePort().getPort();
                        final int dstPort = tcp.getDestinationPort().getPort();
//                        logger.info("tcp from port {} to {} ", srcPort, dstPort);

                        if (srcPort == TCP_PORT_MQTT || dstPort == TCP_PORT_MQTT) {
                            logger.info("message is of type mqtt");
                            mqttSwitchService.addMacAddress(eth);
                            pushPath(sw, srcDevice, dstDevice, ipv4, QUEUE_ID_DEFAULT, METER_ID_DEFAULT, COOKIE_NAME_DEFAULT);
                            return Command.STOP;
                        } else if (srcPort == TCP_PORT_HAZELCAST || dstPort == TCP_PORT_HAZELCAST) {
                            logger.info("message is of type hazelcast tcp socket srcPort {} dstPort {}", srcPort, dstPort);
                            pushPath(sw, srcDevice, dstDevice, ipv4, QUEUE_ID_HAZELCAST, METER_ID_HAZELCAST, COOKIE_NAME_HAZELCAST);
                            return Command.CONTINUE;
                        } else if (srcPort == TCP_PORT_BEST_EFFORT || dstPort == TCP_PORT_BEST_EFFORT) {
                            pushPath(sw, srcDevice, dstDevice, ipv4, QUEUE_ID_NONE, METER_ID_NONE, COOKIE_NAME_BEST_EFFORT);
                            return Command.CONTINUE;
                        }
                    } else if (ipv4.getProtocol() == IpProtocol.UDP) {
                        /* We got a UDP packet; get the payload from IPv4 */
                        final UDP udp = (UDP) ipv4.getPayload();

                        /* Various getters and setters are exposed in UDP */
                        final int srcPort = udp.getSourcePort().getPort();
                        final int dstPort = udp.getDestinationPort().getPort();
//                        logger.info("udp from port {} to {} ", srcPort, dstPort);

                        if (srcPort == UDP_PORT_VR_STREAM || dstPort == UDP_PORT_VR_STREAM) {
//                            pushService.pushAndReturnFlows(sw.getId(), ipv4, OFPort.ALL, QUEUE_ID_STREAM, METER_ID_STREAM, COOKIE_NAME_STREAM);
                            // look if source ip is in client id match map, so already connected via high priority mqtt
                            mqttSwitchService.videoPktIn(eth);
                            return Command.CONTINUE;
                        } else if (srcPort == UDP_PORT_BEST_EFFORT || dstPort == UDP_PORT_BEST_EFFORT) {
                            pushPath(sw, srcDevice, dstDevice, ipv4, QUEUE_ID_NONE, METER_ID_NONE, COOKIE_NAME_BEST_EFFORT);
                            return Command.CONTINUE;
                        }
                    }
                }

                break;
        }
        return Command.CONTINUE; // allow this message to continue to be handled by other PACKET_IN handlers as well
    }

    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleServices() {
        return null;
    }

    @Override
    public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
        return null;
    }

    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
        Collection<Class<? extends IFloodlightService>> l = new ArrayList<>();
        l.add(IFloodlightProviderService.class);
        l.add(IOFSwitchService.class);
        l.add(IRestApiService.class);
        return l;
    }

    @Override
    public void init(FloodlightModuleContext context) throws FloodlightModuleException {
        floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
        switchService = context.getServiceImpl(IOFSwitchService.class);
        logger = LoggerFactory.getLogger(getClass());
        logger.info("init module: " + this.getClass().getSimpleName());

        mqttRoutingService = new MqttRoutingService(context.getServiceImpl(IDeviceService.class), context.getServiceImpl(IRoutingService.class),
                context.getServiceImpl(ITopologyService.class));
        pushService = new MqttFlowPushService(switchService, new MqttFlowBuildService());
        mqttSwitchService = new MqttSwitchService(pushService, switchService, mqttRoutingService);
        restApiService = context.getServiceImpl(IRestApiService.class);
    }

    @Override
    public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
        floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
        switchService.addOFSwitchListener(mqttSwitchService);
        restApiService.addRestletRoutable(new MqttWebRoutable());
    }
}
