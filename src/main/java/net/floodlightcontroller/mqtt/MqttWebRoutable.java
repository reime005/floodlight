package net.floodlightcontroller.mqtt;

import net.floodlightcontroller.mqtt.resources.QueueResource;
import net.floodlightcontroller.restserver.RestletRoutable;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.routing.Router;

/**
 * Created by Marius Reimer on 05/05/17.
 */
public class MqttWebRoutable implements RestletRoutable {

    public MqttWebRoutable() {

    }

    @Override
    public Restlet getRestlet(Context context) {
        Router router = new Router(context);
        router.attach("/queues", QueueResource.class);
        return router;
    }

    @Override
    public String basePath() {
        return "/wm/mqtt";
    }
}
