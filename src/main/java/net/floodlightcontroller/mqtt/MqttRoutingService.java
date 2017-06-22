package net.floodlightcontroller.mqtt;

import net.floodlightcontroller.core.types.NodePortTuple;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.SwitchPort;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Path;
import net.floodlightcontroller.topology.ITopologyService;
import org.projectfloodlight.openflow.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

/**
 * Created by Marius Reimer on 19/04/17.
 */
public class MqttRoutingService implements IMqttRoutingService {

    private IDeviceService deviceService;
    private IRoutingService routingService;
    protected static Logger logger;
    private ITopologyService topologyService;

    public MqttRoutingService(IDeviceService deviceService,
                              IRoutingService routingService,
                              ITopologyService topologyService) {
        logger = LoggerFactory.getLogger(getClass());
        this.deviceService = deviceService;
        this.routingService = routingService;
        this.topologyService = topologyService;
    }

    @Override
    public NodePortTuple getAttachmentPoints(IPv4Address iPv4Address) {
        // retrieve all known devices
        Collection<? extends IDevice> allDevices = deviceService.getAllDevices();
        Optional<? extends IDevice> optDevice = allDevices.stream()
                                            .filter(o -> Arrays.asList(o.getIPv4Addresses()).contains(iPv4Address))
                                            .findFirst();

        if (optDevice.isPresent()) {
            IDevice device = optDevice.get();
            SwitchPort[] attachmentPoints = device.getAttachmentPoints();
            if (attachmentPoints.length > 0) {
                return new NodePortTuple(attachmentPoints[0].getNodeId(), attachmentPoints[0].getPortId());
            }
        } else {
            logger.error("Cannot discover devices. IP: {}", iPv4Address);
        }
        return null;
    }

    @Override
    public NodePortTuple getAttachmentPoints(MacAddress macAddress) {
        final IDevice device = deviceService.findDevice(macAddress,
                VlanVid.ZERO,
                IPv4Address.NONE,
                IPv6Address.NONE,
                DatapathId.NONE,
                OFPort.ZERO);

        if (device != null){
            SwitchPort[] attachmentPoints = device.getAttachmentPoints();
            if (attachmentPoints.length > 0) {
                return new NodePortTuple(attachmentPoints[0].getNodeId(), attachmentPoints[0].getPortId());
            }
        }

        return null;
    }

    @Override
    public Path getPath(NodePortTuple src, NodePortTuple dst) {
        if (src == null || dst == null) {
            return null;
        }

        return routingService.getPath(src.getNodeId(), src.getPortId(), dst.getNodeId(), dst.getPortId());
    }

    public Path getPath(IDevice srcDevice, IDevice dstDevice) {
        if (dstDevice == null || srcDevice == null) {
            logger.info("device unknown. Flooding packet");
            return null;
        }

        SwitchPort dstAp = null;
        for (SwitchPort ap : dstDevice.getAttachmentPoints()) {
            if (topologyService.isEdge(ap.getNodeId(), ap.getPortId())) {
                dstAp = ap;
                break;
            }
        }

        SwitchPort srcAp = null;
        for (SwitchPort ap : srcDevice.getAttachmentPoints()) {
            if (topologyService.isEdge(ap.getNodeId(), ap.getPortId())) {
                srcAp = ap;
                break;
            }
        }

        /*
         * This should only happen (perhaps) when the controller is
         * actively learning a new topology and hasn't discovered
         * all links yet, or a switch was in standalone mode and the
         * packet in question was captured in flight on the dst point
         * of a link.
         */
        if (dstAp == null) {
            logger.warn("Could not locate edge attachment point for destination device {}. Flooding packet");
            return null;
        }

        if (srcAp == null) {
            logger.warn("Could not locate edge attachment point for source device {}. Flooding packet");
            return null;
        }

        /* Validate that the source and destination are not on the same switch port */
//        if (srcAp.getNodeId().equals(dstAp.getNodeId()) && srcAp.getPortId().equals(dstAp.getPortId())) {
//            logger.info("Both source and destination are on the same switch/port {}/{}. Dropping packet", srcAp.getNodeId().toString(), srcAp.getPortId());
//            return null;
//        }

        return routingService.getPath(srcAp.getNodeId(), srcAp.getPortId(), dstAp.getNodeId(), dstAp.getPortId());
    }
}
