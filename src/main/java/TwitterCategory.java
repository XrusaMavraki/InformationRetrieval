import java.util.Collections;
import java.util.Set;

/**
 * Created by xrusa on 11/6/2016.
 */
public class TwitterCategory {

    private final String name;
    private final Set<String> hashtags;
    private final Set<String> mentions;

    public TwitterCategory(String name, Set<String> hashtags, Set<String> mentions) {
        this.name = name;
        this.hashtags = Collections.unmodifiableSet(hashtags);
        this.mentions = Collections.unmodifiableSet(mentions);
    }

    public String getName() {
        return name;
    }

    public Set<String> getHashtags() {
        return hashtags;
    }

    public Set<String> getMentions() {
        return mentions;
    }
}
