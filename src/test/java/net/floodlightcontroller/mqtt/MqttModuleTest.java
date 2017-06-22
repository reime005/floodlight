package net.floodlightcontroller.mqtt;

import net.floodlightcontroller.packet.IPv4;
import net.floodlightcontroller.packet.TCP;
import org.junit.BeforeClass;
import org.junit.Test;
import org.projectfloodlight.openflow.protocol.OFFactory;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.protocol.ver14.OFFactoryVer14;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.IpProtocol;
import org.projectfloodlight.openflow.types.TransportPort;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by marius on 11.06.17.
 */
public class MqttModuleTest {

    private final static String CLIENT_ID_CPS = "CPS";
    private final static String CLIENT_ID_HTC = "HTC-VIVE";
    private static IMqttFlowBuildService flowBuildService;
    private static OFFactory ofFactory;

    @BeforeClass
    public static void before() {
        flowBuildService = new MqttFlowBuildService();
        ofFactory = new OFFactoryVer14();
    }

    @Test
    public void testContains() {
        ClientSwitchMatches clientSwitchMatches = new ClientSwitchMatches();
        List<Match> listMatches1 = new ArrayList<>();
        listMatches1.add(flowBuildService.buildMatch(ofFactory, true, getIpV4("10.0.0.1", "10.0.0.2", 1337)));
        listMatches1.add(flowBuildService.buildMatch(ofFactory, true, getIpV4("10.0.0.5", "10.0.0.2", 1234)));
        clientSwitchMatches.putMatches(DatapathId.of(1), CLIENT_ID_CPS, listMatches1);

        assertFalse(clientSwitchMatches.contains(CLIENT_ID_HTC)); // should not be in
    }

    @Test
    public void testRemove() {
        ClientSwitchMatches clientSwitchMatches = new ClientSwitchMatches();
        List<Match> listMatches1 = new ArrayList<>();
        listMatches1.add(flowBuildService.buildMatch(ofFactory, true, getIpV4("10.0.0.1", "10.0.0.2", 1337)));
        listMatches1.add(flowBuildService.buildMatch(ofFactory, true, getIpV4("10.0.0.5", "10.0.0.2", 1234)));
        clientSwitchMatches.putMatches(DatapathId.of(1), CLIENT_ID_CPS, listMatches1);
        clientSwitchMatches.putMatches(DatapathId.of(2), CLIENT_ID_CPS, listMatches1);
        clientSwitchMatches.putMatches(DatapathId.of(2), CLIENT_ID_HTC, listMatches1);

        clientSwitchMatches.remove(CLIENT_ID_CPS, DatapathId.of(2));
        clientSwitchMatches.remove(CLIENT_ID_CPS, DatapathId.of(1));

        assertFalse(clientSwitchMatches.contains(CLIENT_ID_CPS)); // should be removed
        assertTrue(clientSwitchMatches.contains(CLIENT_ID_HTC)); // should still be in
    }

    @Test
    public void testGetClientIdForIpV4Src() {
        final IPv4Address searchedAddress = IPv4Address.of("10.0.0.5");
        ClientSwitchMatches clientSwitchMatches = new ClientSwitchMatches();
        List<Match> listMatches1 = new ArrayList<>();
        listMatches1.add(flowBuildService.buildMatch(ofFactory, true, getIpV4("10.0.0.1", "10.0.0.2", 1337)));
        listMatches1.add(flowBuildService.buildMatch(ofFactory, true, getIpV4("10.0.0.5", "10.0.0.2", 1234)));
        listMatches1.add(flowBuildService.buildMatch(ofFactory, false, getIpV4("10.0.0.1", "10.0.0.2", 1337)));
        listMatches1.add(flowBuildService.buildMatch(ofFactory, false, getIpV4("10.0.0.5", "10.0.0.2", 1234)));
        clientSwitchMatches.putMatches(DatapathId.of(1), CLIENT_ID_CPS, listMatches1);

        assertEquals(CLIENT_ID_CPS, clientSwitchMatches.getClientIdForIpV4Src(searchedAddress)); // should be in
        assertNotEquals(CLIENT_ID_HTC, clientSwitchMatches.getClientIdForIpV4Src(searchedAddress)); // should be in
    }

    @Test
    public void testGetFirst() {
        final IPv4Address searchedAddress = IPv4Address.of("10.0.0.1");
        ClientSwitchMatches clientSwitchMatches = new ClientSwitchMatches();

        List<Match> listMatches1 = new ArrayList<>();
        listMatches1.add(flowBuildService.buildMatch(ofFactory, false, getIpV4("10.0.0.1", "10.0.0.2", 1234)));

        List<Match> listMatches2 = new ArrayList<>();
        listMatches1.add(flowBuildService.buildMatch(ofFactory, false, getIpV4("10.0.0.5", "10.0.0.2", 1337)));

        clientSwitchMatches.putMatches(DatapathId.of(1), CLIENT_ID_CPS, listMatches1);
        clientSwitchMatches.putMatches(DatapathId.of(1), CLIENT_ID_HTC, listMatches2);
        clientSwitchMatches.putMatches(DatapathId.of(2), CLIENT_ID_CPS, listMatches1);

        Match firstMatch = clientSwitchMatches.getFirstMatch(CLIENT_ID_CPS);
        assertNotEquals(null, firstMatch);
        assertEquals(searchedAddress, firstMatch.get(MatchField.IPV4_SRC));

        Match secondMatch = clientSwitchMatches.getFirstMatch(CLIENT_ID_HTC);
        assertEquals(null, secondMatch);
    }

    private IPv4 getIpV4(String sourceIp, String dstIp, int port) {
        final TCP tcp = new TCP()
                .setDestinationPort(port)
                .setSourcePort(TransportPort.FULL_MASK);
        return (IPv4) new IPv4()
                .setProtocol(IpProtocol.IPv4)
                .setDestinationAddress(dstIp)
                .setSourceAddress(sourceIp)
                .setPayload(tcp);
    }
}
