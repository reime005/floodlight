package net.floodlightcontroller.mqtt;

import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.IPAddress;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.MacAddress;

import java.util.*;

/**
 * Purpose: Managing all connected MQTT clients.
 * The flows are grouped by the MQTT clientId, using the cookie flow entry.
 * If a flow is pushed to a switch, the used matches are saved in the {@link DataPathMap}.
 * If an UDP/RTP stream is initiated (PKT_IN), the clientId is searched via its ipv4 source address (because it must be the same host).
 *
 * Created by Marius Reimer on 30/04/17.
 */
public class ClientSwitchMatches {

    private final Map<String, DataPathMap> content;

    public ClientSwitchMatches() {
        content = new HashMap<>();
    }

    /**
     * Searches for the ipv4 source address in all matches and returns first related clientId
     * @param iPv4Address searched
     * @return clientId
     */
    public String getClientIdForIpV4Src(IPv4Address iPv4Address) {
        for (Map.Entry<String, DataPathMap> entry : content.entrySet()) {
            if (entry.getValue() != null && entry.getValue().hasIpV4Src(iPv4Address)) {
                return entry.getKey();
            }
        }
        return null;
    }

//    public MacAddress getMacAddressForIpSrc(IPv4Address iPv4Address) {
//        for (Map.Entry<String, DataPathMap> entry : content.entrySet()) {
//            if (entry.getValue() != null && entry.getValue().hasIpV4Src(iPv4Address)) {
//                return entry.getValue().getFirstMatch().get(MatchField.ETH_SRC);
//            }
//        }
//        return MacAddress.FULL_MASK;
//    }

    /**
     * Checks, whether a clientId is in the map
     * @param clientId searched client id
     * @return TRUE if found, else FALSE
     */
    public boolean contains(String clientId) {
        return content.containsKey(clientId);
    }

    /**
     * Adds new matches
     * @param dataPathId switch id
     * @param clientId mqtt client id
     * @param matchList related match list to be added
     */
    public void putMatches(DatapathId dataPathId, String clientId, List<Match> matchList) {
        if (matchList == null || matchList.isEmpty()) {
            return;
        }

        if (!content.containsKey(clientId)) {
            content.put(clientId, new DataPathMap());
        }
        content.get(clientId).add(dataPathId, matchList);
    }

    /**
     * Adds new match
     * @param dataPathId switch id
     * @param clientId mqtt client id
     * @param match related match to be added
     */
    public void putMatch(DatapathId dataPathId, String clientId, Match match) {
        if (match == null) {
            return;
        }

        final List<Match> matchList = new ArrayList<>();
        matchList.add(match);
        this.putMatches(dataPathId, clientId, matchList);
    }

    /**
     * Searches all matches for the client id and switch id
     * @param clientId mqtt client id
     * @param dataPathId switch id
     * @return list of the found matches
     */
    public List<Match> getFor(String clientId, DatapathId dataPathId) {
        if (content.containsKey(clientId)) {
            return content.get(clientId).getFor(dataPathId);
        }
        return null;
    }

    /**
     * Searches all matches for the client id source IP
     * @param clientId mqtt client id
     * @return list of the found matches
     */
    public Match getFirstMatch(String clientId) {
        if (content.containsKey(clientId)) {
            return content.get(clientId).getFirstMatch();
        }
        return null;
    }

    /**
     * Removes the datapath id for the client id
     * @param clientId mqtt client d
     * @param datapathId switch id
     */
    public void remove(String clientId, DatapathId datapathId) {
        if (content.containsKey(clientId)) {
            content.get(clientId).remove(datapathId);

            if (content.get(clientId).isEmpty()) {
                content.remove(clientId);
            }
        }
    }

    private class DataPathMap {

        private final Map<DatapathId, List<Match>> innerContent;

        DataPathMap() {
            innerContent = new HashMap<>();
        }

        /**
         * Adds new matches to the datapath id
         * @param datapathId switch id
         * @param matchList match list
         */
        private void add(DatapathId datapathId, List<Match> matchList) {
            if (innerContent.containsKey(datapathId)) {
                final List<Match> existingMatches = innerContent.get(datapathId);
                if (existingMatches != null) {
                    List<Match> merged = new ArrayList<>(existingMatches);
                    merged.addAll(matchList);
                    innerContent.replace(datapathId, merged);
                }
            } else {
                innerContent.put(datapathId, matchList);
            }
        }

        /**
         * Removes the datapath id key
         * @param datapathId switch id
         */
        private void remove(DatapathId datapathId) {
            if (innerContent.containsKey(datapathId)) {
                innerContent.remove(datapathId);
            }
        }

        /**
         * Checks, whether the ipv4 source address equals to any every used for a match. Purpose: UDP/RTP stream
         * @param iPv4Address searched ipv4 source address
         * @return TRUE if found, FALSE otherwise
         */
        private boolean hasIpV4Src(IPv4Address iPv4Address) {
            for (List<Match> matches : innerContent.values()) {
                final Optional<Match> matchList = matches.stream()
                        .filter(Objects::nonNull)
                        .filter(match -> match.get(MatchField.IPV4_SRC).equals(iPv4Address))
                        .findFirst();

                if (matchList.isPresent()) {
                    return true;
                }
            }
            return false;
        }

        public boolean isEmpty() {
            return innerContent.isEmpty();
        }

        /**
         * Matches for the datapath id
         * @param dataPathId switch id
         * @return list of matches for the given datapath id
         */
        private List<Match> getFor(DatapathId dataPathId) {
            if (dataPathId == null) {
                return null;
            }

            if (innerContent.containsKey(dataPathId)) {
                return innerContent.get(dataPathId);
            }
            return null;
        }

        private Match getFirstMatch() {
            final Optional<List<Match>> firstMatches = innerContent.values().stream().findFirst();

            if (firstMatches.isPresent()) {
                List<Match> matches = firstMatches.get();

                if (matches.isEmpty()) {
                    return null;
                }
                return matches.get(0);
            }

            return null;
        }
    }
}
