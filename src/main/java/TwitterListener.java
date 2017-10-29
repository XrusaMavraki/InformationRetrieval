import twitter4j.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by xrusa on 11/6/2016.
 */
public class TwitterListener {

    private TwitterListener() {}

    public static void startListener() {
        startListener("tweets.bin");
    }

    public static void startListener(String filePath) {
        ObjectOutputStream oos;
        try {
            oos = getOutputStream(filePath);
        } catch (IOException ioe) {
            System.err.println("Cannot open save file " + ioe.getMessage());
            throw new RuntimeException(ioe);
        }
        StatusListener listener = createListener(oos);
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        FilterQuery filterQuery = new FilterQuery();
        Set<String> allTrack = new HashSet<>(Util.CAT_1.getHashtags());
        allTrack.addAll(Util.CAT_2.getHashtags());
        allTrack.addAll(Util.CAT_1.getMentions());
        allTrack.addAll(Util.CAT_2.getMentions());
        String[] arr = new String[allTrack.size()];
        arr = allTrack.toArray(arr);
        filterQuery.track(arr);
        twitterStream.filter(filterQuery);
    }

    public static ObjectOutputStream getOutputStream(String filePath) throws IOException {
        if (Files.exists(Paths.get(filePath))) {
            return new ObjectOutputStream(new FileOutputStream(filePath, true)) {
                @Override
                protected void writeStreamHeader() throws IOException {
                    reset();
                }
            };
        } else {
            return new ObjectOutputStream(new FileOutputStream(filePath, true));
        }
    }

    private static StatusListener createListener(ObjectOutputStream oos) {
        return new StatusListener() {

            @Override
            public void onStatus(Status status) {
                String tweet;
                if (status.isRetweet() || status.isRetweeted()) {
                    return;
                }
                int hashTagId = Util.getHashtagsCategory(status);
                int mentionId = Util.getMentionsCategory(status);
                int sum = hashTagId + mentionId;
                if (sum == 0 || sum == 3)
                    return;

                tweet = status.getText();
                try {
                    oos.writeObject(status);
                    oos.flush();
                    System.out.println("!!!Received new tweet!!!: " + tweet);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
            }

            public void onScrubGeo(long l, long l1) {
            }

            public void onStallWarning(StallWarning stallWarning) {
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
    }





}
