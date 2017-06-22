package net.floodlightcontroller.mqtt.resources;

import com.google.common.util.concurrent.ListenableFuture;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import org.projectfloodlight.openflow.protocol.OFQueueStatsEntry;
import org.projectfloodlight.openflow.protocol.OFQueueStatsReply;
import org.projectfloodlight.openflow.protocol.OFQueueStatsRequest;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.U64;
import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by Marius Reimer on 05/05/17.
 */
public class QueueResource extends ServerResource {
    protected static Logger log = LoggerFactory.getLogger(QueueResource.class);

    @Get("json")
    public Map getQueueStats() {

        final IOFSwitchService switchService = (IOFSwitchService)getContext().getAttributes().
                get(IOFSwitchService.class.getCanonicalName());

        final Map<String, Map<String, Map<String, Object>>> retMap = new HashMap<String, Map<String, Map<String, Object>>>();

        for (DatapathId datapathId : switchService.getAllSwitchDpids()) {
            final IOFSwitch sw = switchService.getActiveSwitch(datapathId);

            if (sw == null) {
                continue;
            }

            final OFQueueStatsRequest sr = sw.getOFFactory().buildQueueStatsRequest().build();

            /* Note use of writeStatsRequest (not writeRequest) */
            final ListenableFuture<List<OFQueueStatsReply>> future = sw.writeStatsRequest(sr);
            try {
                final List<OFQueueStatsReply> replies = future.get(10, TimeUnit.SECONDS);
                for (OFQueueStatsReply reply : replies) {
                    for (OFQueueStatsEntry e : reply.getEntries()) {
                        long id = e.getQueueId();
                        U64 txb = e.getTxBytes();

                        if (txb.getValue() <= 0) {
                            continue;
                        }

                        Map<String, Map<String, Object>> innerMap = retMap.get(datapathId);

                        if (innerMap == null) {
                            innerMap = new HashMap<>();
                        }

                        Map<String, Object> statsMap = new HashMap<>();
                        statsMap.put("id", id);
                        statsMap.put("txBytes", txb);

                        innerMap.put(String.valueOf(e.getQueueId()), statsMap);

                        retMap.put(datapathId.toString(), innerMap);

                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return retMap;
    }
}
