import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.UserMentionEntity;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.Normalizer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by xrusa on 11/6/2016.
 */
public class Util {

    private static final Set<String> HASHTAG_1 = new HashSet<>();
    private static final Set<String> HASHTAG_2 = new HashSet<>();
    private static final Set<String> MENTIONS_1 = new HashSet<>();
    private static final Set<String> MENTIONS_2 = new HashSet<>();
    private static final Set<String> POSITIVE_EMOJIS = new HashSet<>();
    private static final Set<String> NEGATIVE_EMOJIS = new HashSet<>();
    public static final Set<String> POSITIVE_WORDS = new HashSet<>();
    public static final Set<String> NEGATIVE_WORDS = new HashSet<>();
    static {
        HASHTAG_1.add("syriza");
        HASHTAG_1.add("syrizanel");
        HASHTAG_1.add("ΣΥΡΙΖΑ");
        HASHTAG_2.add("ΝΔ");
        HASHTAG_2.add("ΝΕΑΔΗΜΟΚΡΑΤΙΑ");
        HASHTAG_2.add("newdemocracy");
        MENTIONS_1.add("tsipras_eu");
        MENTIONS_2.add("kmitsotakis");
        Collections.addAll(POSITIVE_EMOJIS, ":smile:", ":simple_smile:", ":laughing:", ":blush:", ":smiley:", ":relaxed:", ":smirk:",
                ":heart_eyes:", ":kissing_heart:",":kissing_closed_eyes:",":satisfied:",":satisfied:",":grin:",":wink:",
                ":stuck_out_tongue_winking_eye:",":stuck_out_tongue_closed_eyes:",":grinning:",":kissing:",":stuck_out_tongue:"
                ,":joy:",":yum:",":heart:",":+1:",":thumbsup:",":ok_hand:");
        Collections.addAll(NEGATIVE_EMOJIS,":worried:",":frowning:",":anguished:",":open_mouth:",":grimacing:",":confused:" ,":hushed:",
                ":expressionless:",":unamused:",":weary:",":pensive:",":disappointed:",":confounded:",":fearful:"
                ,":cold_sweat:",":cry:",":sob:",":scream:",":tired_face:",":angry:",":rage:",
                ":dizzy_face:",":neutral_face:",":broken_heart:",":poop:",":shit:",":thumbsdown:",":-1:");
    }
    public static final TwitterCategory CAT_1 = new TwitterCategory("Syriza", HASHTAG_1, MENTIONS_1);

    public static final TwitterCategory CAT_2 = new TwitterCategory("ND", HASHTAG_2, MENTIONS_2);

    private static final String GREEK_REGEX = "\\p{InGreek}";

    private static final Pattern GREEK_PATTERN = Pattern.compile(GREEK_REGEX, Pattern.CASE_INSENSITIVE);

    public Util() {
    }

    public static int getMentionsCategory(Status status) {
        //returns 1 for Syriza 2 for Nd 0 for none
        boolean hasMentions1 = false;
        boolean hasMentions2 = false;
        for (UserMentionEntity userMentionEntity : status.getUserMentionEntities()) {
            hasMentions1 = CAT_1.getMentions().contains(userMentionEntity.getScreenName()) || hasMentions1;
            hasMentions2 = CAT_2.getMentions().contains(userMentionEntity.getScreenName()) || hasMentions2;
        }
        if(hasMentions1^hasMentions2){
            if(hasMentions1) {return 1;}
            else return 2;
        }
        return 0;
    }

    /**
     * Returns 1 if the status text contains one or more positive emojis, 2 if it contains 1 or more negative emojis.
     * If it contains both positive or negative or none of them, it returns 0.
     * @param statusText The status text to test.
     * @return 0, 1 or 2.
     */

    public static int getEmojiCategory(String statusText) {
        int countPositive=0;
        for (String positiveEmoji : POSITIVE_EMOJIS) {
            if (statusText.contains(positiveEmoji)) {
                countPositive++;
            }
        }
        int countNegative=0;
        for (String negativeEmoji : NEGATIVE_EMOJIS) {
            if (statusText.contains(negativeEmoji)) {
                countNegative++;
            }
        }
        if(countNegative<countPositive){
            return 1;
        }else if(countNegative>countPositive){
            return 2;
        }
        return 0;
    }

    public static int getHashtagsCategory(Status status) {
        //returns 1 for Syriza 2 for Nd 0 for none
        boolean hasHashtag1 = false;
        boolean hasHashtag2 = false;
        for (HashtagEntity hashtagEntity : status.getHashtagEntities()) {
            hasHashtag1 = CAT_1.getHashtags().contains(hashtagEntity.getText()) || hasHashtag1;
            hasHashtag2 = CAT_2.getHashtags().contains(hashtagEntity.getText()) || hasHashtag2;
        }
        if (hasHashtag1^hasHashtag2 ){
            if(hasHashtag1){return 1;}
            else {return 2;}
        }
        return 0;
    }
    public static String removeUrl(String commentstr)
    {
        String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$~_?\\+-=\\\\\\.&]*)";
        Pattern p = Pattern.compile(urlPattern,Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(commentstr);
        int i = 0;
        while (m.find()) {
            commentstr = commentstr.replaceAll(m.group(i),"").trim();
            i++;
        }
        return commentstr;
    }

    public static String stem(String term) {



        //Check if term is numeric
        //DIKH MOY ALLAGH GIATI XTYPAGE TO INDEXfILES
        //  if (term.matches("^[+-]?(\\d+(\\.\\d*)?|\\.\\d+)([eE][+-]?\\d+)?$"))
        //      return "";

        // Remove first level suffixes only if the term is 4 letters or more
        if (term.length() >= 4) {

            // Remove the 3 letter suffixes
            if (term.endsWith("ΟΥΣ") ||
                    term.endsWith("ΕΙΣ") ||
                    term.endsWith("ΕΩΝ") ||
                    term.endsWith("ΟΥΝ")) {
                term = term.substring(0, term.length() - 3);
            }

            // Remove the 2 letter suffixes
            else if (term.endsWith("ΟΣ") ||
                    term.endsWith("ΗΣ") ||
                    term.endsWith("ΕΣ") ||
                    term.endsWith("ΩΝ") ||
                    term.endsWith("ΟΥ") ||
                    term.endsWith("ΟΙ") ||
                    term.endsWith("ΑΣ") ||
                    term.endsWith("ΩΣ") ||
                    term.endsWith("ΑΙ") ||
                    term.endsWith("ΥΣ") ||
                    term.endsWith("ΟΝ") ||
                    term.endsWith("ΑΝ") ||
                    term.endsWith("ΕΙ")) {

                term = term.substring(0, term.length() - 2);
            }
            // Remove the 1 letter suffixes
            else if (term.endsWith("Α") ||
                    term.endsWith("Η") ||
                    term.endsWith("Ο") ||
                    term.endsWith("Ε") ||
                    term.endsWith("Ω") ||
                    term.endsWith("Υ") ||
                    term.endsWith("Ι")) {

                term = term.substring(0, term.length() - 1);
            }

        }
        return term;
    }
    public static  String  getCleanString(String tweetOLD){
       String tweet= Util.removeUrl(tweetOLD); //removes urls
        tweet = tweet.replaceAll("\\s+", " ");
        tweet= tweet.toUpperCase();
        tweet= Normalizer.normalize(tweet, Normalizer.Form.NFD);
        tweet = tweet.replaceAll("[^\\p{L}\\p{Nd} ]", ""); //removes everything that isnt text
        StringBuilder sbTweet = new StringBuilder();
        for (String term : tweet.split(" ")) {
            String stemmed = Util.stem(term);
            if (!term.trim().isEmpty()) {
                sbTweet.append(" ").append(stemmed);
            }
        }
        tweet = sbTweet.toString().trim();
        return tweet;
    }
    public static String getPositiveWords(String tweetText){
       return getPosNegWords(tweetText,1);
    }
    public static String getNegativeWords(String tweetText){
       return getPosNegWords(tweetText,2);
    }
    public static String getPosNegWords(String tweetText,int posNeg) {
        String[] tokens = tweetText.split("\\s+");
        String st = "";
        if (posNeg == 1) {
            for (String t : tokens) {
                if (POSITIVE_WORDS.contains(t)) {
                    st += t + " ";
                }
            }
        } else if (posNeg == 2) {
            for (String t : tokens) {
                if (NEGATIVE_WORDS.contains(t)) {
                    st += t + " ";
                }
            }
        }
        return st;
    }



    @SuppressWarnings({ "unchecked", "unchecked" }) public static void setPosNegWordDataFromXLS(String filename,int posNeg) throws Exception{
            FileInputStream fis = null;
            try {
                //
                // Create a FileInputStream that will be use to read the
                // excel file.
                //
                fis = new FileInputStream(filename);
                HSSFWorkbook workbook = new HSSFWorkbook(fis);
                HSSFSheet sheet = workbook.getSheetAt(0);
                Iterator rows = sheet.rowIterator();
                while (rows.hasNext()) {
                    HSSFRow row = (HSSFRow) rows.next();
                    HSSFCell cell = row.getCell(2);
                    if (posNeg == 1) {
                        POSITIVE_WORDS.add(cell.getStringCellValue());
                    }
                    else if(posNeg == 2){
                        NEGATIVE_WORDS.add(cell.getStringCellValue());
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (fis != null) {
                    fis.close();
                }
            }


        }

    public static int getWordSentiment(String positive,String negative){
        String[] pos= positive.split("\\s+");
        int posLength = positive.isEmpty() ? 0 : pos.length;
        String[] neg = negative.split("\\s+");
        int negLength = negative.isEmpty() ? 0 : neg.length;
        if(posLength>negLength){
            return 1;
        }else if(posLength<negLength){
            return 2;
        }else return 0;
    }

    public static List<Status> readFromFile(String filePath){
        List<Status> list = new LinkedList<>();
        try (ObjectInputStream ios = new ObjectInputStream(new FileInputStream(filePath))) {
            Status stat=null;
            while((stat= (Status) ios.readObject())!=null) {
                list.add(stat);
            }
        } catch (IOException e) {
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return list;
    }

    public static boolean isGreekTweet(String tweet) {
        boolean isGreekTweet = false;
        for (String c : tweet.trim().split("")) {
            Matcher matcher = GREEK_PATTERN.matcher(c);
            if (matcher.matches()) {
                isGreekTweet = true;
                break;
            }
        }
        return  isGreekTweet;
    }
}
