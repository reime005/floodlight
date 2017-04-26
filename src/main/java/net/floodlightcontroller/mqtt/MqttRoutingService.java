package net.floodlightcontroller.mqtt;

import net.floodlightcontroller.core.types.NodePortTuple;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.SwitchPort;
import net.floodlightcontroller.routing.IRoutingService;
import net.floodlightcontroller.routing.Path;
import org.projectfloodlight.openflow.types.IPv4Address;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

/**
 * Created by Marius Reimer on 19/04/17.
 */
public class MqttRoutingService implements IMqttRoutingService {

    private IDeviceService deviceService;
    private IRoutingService routingService;

    public MqttRoutingService(IDeviceService deviceService,
                              IRoutingService routingService) {
        this.deviceService = deviceService;
        this.routingService = routingService;
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
}
