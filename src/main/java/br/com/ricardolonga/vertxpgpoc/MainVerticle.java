package br.com.ricardolonga.vertxpgpoc;

import org.vertx.java.core.MultiMap;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.platform.Verticle;

public class MainVerticle extends Verticle {

    @Override
    public void start() {
        JsonObject defMongoConnection = new JsonObject().putString("host", "core1").putString("db_name", "vertx").putNumber("port", 27017);

        container.deployModule("io.vertx~mod-mongo-persistor~2.1.1-SNAPSHOT", container.config().getObject("mongo-persistor", defMongoConnection));
        container.deployWorkerVerticle("br.com.ricardolonga.vertxpgpoc.workers.ZipsWorker", 20);

        RouteMatcher routes = new RouteMatcher().get("/zips", (request) -> {
            System.out.println(Thread.currentThread().getName() + " - [GET] /zips");

            JsonObject mongoAction = buildJsonObject(request.params());

            vertx.eventBus().send("zips.get", mongoAction,  (Message<JsonObject> result) -> {
                System.out.println(Thread.currentThread().getName() + " - " + result.body());
                request.response().putHeader("Content-Type", "application/json");
                request.response().end(result.body().encodePrettily());
            });

        });

        vertx.createHttpServer().requestHandler(routes).listen(8888);
    }

    private JsonObject buildJsonObject(MultiMap params) {
        JsonObject mongoAction = new JsonObject();

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
        return mongoAction;
    }

}
