package io.rcw.heartbeat;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.push.PushListener;
import io.lettuce.core.api.push.PushMessage;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class Main {


    public static final String getEnv(String key, String def) {
        final String val = System.getenv(key);

        if (val == null) {
            return def;
        }

        return val;
    }





    record Info(UUID id, int playersOnline) {
        @Override
        public String toString() {
            return String.format("server_id:%s;players_online:%d;", id.toString(), playersOnline);
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Connect to Redis
        RedisClient redisClient = RedisClient.create(getEnv("REDIS_HOST", "redis://localhost:6379"));
        final StatefulRedisPubSubConnection<String, String> connection = redisClient.connectPubSub();

        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        // generate a v4 UUID
        final UUID serverId = UUID.randomUUID();

        executor.scheduleAtFixedRate(() -> {
            try {
                connection.async()
                        .publish("heartbeat", new Info(serverId,Math.max((int)(Math.random() * 1000.0), 1)).toString()); // wait for it to complete
            } catch (Exception ignored) {}
        }, 0, 2, TimeUnit.SECONDS);


        new Thread(() -> {
            final StatefulRedisPubSubConnection<String, String> pubSubConnection = redisClient.connectPubSub();
            pubSubConnection.addListener(new RedisPubSubListener<String, String>() {
                @Override
                public void message(String channel, String data) {
                    Logger.getAnonymousLogger().info(() -> String.format("[channel-%s] %s\n", channel, data));
                }

                @Override
                public void message(String s, String channel, String data) {
                    Logger.getAnonymousLogger().info(() -> String.format("[channel-%s] %s\n", channel, data));

                }

                @Override
                public void subscribed(String s, long l) {

                }

                @Override
                public void psubscribed(String s, long l) {

                }

                @Override
                public void unsubscribed(String s, long l) {

                }

                @Override
                public void punsubscribed(String s, long l) {

                }
            });

           pubSubConnection.async().subscribe("heartbeat");
        }).start();
    }
}
