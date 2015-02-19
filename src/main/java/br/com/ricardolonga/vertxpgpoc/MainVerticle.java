package br.com.ricardolonga.vertxpgpoc;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

public class MainVerticle extends Verticle {

    @Override
    public void start() {
        container.deployModule("io.vertx~mod-mongo-persistor~2.1.1-SNAPSHOT", container.config().getObject("mongo-persistor"));

        container.deployWorkerVerticle("br.com.ricardolonga.vertxpgpoc.workers.ZipsWorker", 20);

        RouteMatcher routes = new RouteMatcher().get("/zips", (request) -> {
            System.out.println(Thread.currentThread().getName() + " - [GET] /zips");

            JsonObject mongoAction = new JsonObject();

            MultiMap params = request.params();

            if (params.contains("fields")) {
                JsonObject fields = new JsonObject();

                String fields__ = params.get("fields");
                String[] fields_ = fields__.split(",");

                for (String field : fields_) {
                    fields.putString(field, "1");
                }

                mongoAction.putObject("keys", fields);
            }

            mongoAction.putString("collection", "zips");
            mongoAction.putString("action", "find");
            mongoAction.putObject("matcher", new JsonObject());

            vertx.eventBus().send("zips.get", mongoAction, new Handler<Message<JsonObject>>() {
                @Override
                public void handle(Message<JsonObject> result) {
                    System.out.println(Thread.currentThread().getName() + " - " + result.body());

                    request.response().putHeader("Content-Type", "application/json");
                    request.response().end(result.body().encodePrettily());
                }
            });
        });

        vertx.createHttpServer().requestHandler(routes).listen(8888);
    }

}
