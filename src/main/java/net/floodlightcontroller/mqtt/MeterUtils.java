package net.floodlightcontroller.mqtt;

import net.floodlightcontroller.core.IOFSwitch;
import org.projectfloodlight.openflow.protocol.*;
import org.projectfloodlight.openflow.protocol.meterband.OFMeterBand;
import org.projectfloodlight.openflow.protocol.meterband.OFMeterBandDrop;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by Marius Reimer on 18/03/17.
 */
public class MeterUtils {

    private IOFSwitch sw;
    private Set<OFMeterFlags> flags; /* Meter flags */
    private long rate; /* Meter band drop rate */
    private long id; /* Meter ID */
    private long burstSize; /* Burst control rate */

    public MeterUtils(IOFSwitch sw, long id, Set<OFMeterFlags> flags, long rate,
                      long burst) {
        this.sw = sw;
        this.flags = flags;
        this.rate = rate;
        this.id = id;
        this.burstSize = burst;
    }

    public void writeMeter(OFMeterModCommand cmd) {
        OFFactory meterFactory = OFFactories.getFactory(OFVersion.OF_13);
        OFMeterMod.Builder meterModBuilder = meterFactory.buildMeterMod()
                                                         .setMeterId(id)
                                                         .setCommand(cmd);
        switch (cmd) {
            case ADD:
            case MODIFY:
                /* Create and set meter band */
                OFMeterBandDrop.Builder bandBuilder = meterFactory.meterBands()
                                                                  .buildDrop()
                                                                  .setRate(rate);
                if (this.burstSize != 0) {
                    bandBuilder = bandBuilder.setBurstSize(this.burstSize);
                }

                OFMeterBand band = bandBuilder.build();
                List<OFMeterBand> bands = new ArrayList<OFMeterBand>();
                bands.add(band);

                /* Create meter modification message */
                meterModBuilder.setMeters(bands).setFlags(flags).build();

                break;
            case DELETE:
        }

        /* Send meter modification message to switch */
        sw.write(meterModBuilder.build());
    }
}
