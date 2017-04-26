package net.floodlightcontroller.mqtt;

import net.floodlightcontroller.core.types.NodePortTuple;
import net.floodlightcontroller.routing.Path;
import org.projectfloodlight.openflow.types.IPv4Address;

/**
 * Created by Marius Reimer on 20/04/17.
 */
public interface IMqttRoutingService {

    /**
     * Get the device for the iPv4 address
     * @param iPv4Address iPv4 address
     * @return found device information
     */
    NodePortTuple getAttachmentPoints(IPv4Address iPv4Address);

    /**
     * Calculates the path between src and dst
     *
     * @param src Source
     * @param dst Destination
     * @return null, if no path possible
     */
    Path getPath(NodePortTuple src, NodePortTuple dst);
}
