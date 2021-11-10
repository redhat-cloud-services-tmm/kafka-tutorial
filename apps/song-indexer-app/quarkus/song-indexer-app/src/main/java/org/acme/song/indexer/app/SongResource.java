package org.acme.song.indexer.app;

import java.time.Duration;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.eclipse.microprofile.reactive.messaging.Channel;

import io.smallrye.mutiny.Multi;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;

@Path("/events")
public class SongResource {

    @Channel("songs")
    Multi<KafkaRecord<Integer, String>> songs;

    @GET
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public Multi<String> getEvents() {

        return Multi.createBy().merging().streams(songs.invoke(t -> System.out.println("Song " + t.getPayload() + " indexed")).map(k -> k.getPayload()),
                Multi.createFrom().ticks().every(Duration.ofSeconds(10)).map(x -> "{}"));

    }

}