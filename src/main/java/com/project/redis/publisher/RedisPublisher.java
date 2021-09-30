package com.project.redis.publisher;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.scheduling.annotation.Scheduled;


public class RedisPublisher {

    private final RedisTemplate< String, Object > template;
    private final ChannelTopic topic;
    private final AtomicLong counter = new AtomicLong( 0 );

    public RedisPublisher(final RedisTemplate< String, Object > template,
                          final ChannelTopic topic ) {
        this.template = template;
        this.topic = topic;
    }

    @Scheduled( fixedDelay = 200 )
    public void publish() {
        ObjectRecord<String, String> record = StreamRecords.newRecord()
                .ofObject("value")
                .withStreamKey("key");

        template.opsForStream().add(record);

        template.convertAndSend( topic.getTopic(), record +
                ", " + Thread.currentThread().getName() );
    }
}