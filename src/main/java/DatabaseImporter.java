import com.vdurmont.emoji.EmojiParser;
import twitter4j.Status;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by xrusa on 26-Nov-16.
 */
public class DatabaseImporter extends DbOperation {

    private static final String SQL_INSERT_STATEMENT = "INSERT INTO tweets"
            + "(id,text, category_hashtag, category_mention, category_sentiment,tweet_date,word_sentiment,positive_words,negative_words) VALUES"
            + "(?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE category_sentiment =VALUES(category_sentiment),word_sentiment=VALUES(word_sentiment),positive_words=VALUES(positive_words),negative_words=VALUES(negative_words)";
    public static List<ListStructure> extractToList(String filePath){
        List<ListStructure> tweetStuff=  new LinkedList<>();
        List<Status> list=Util.readFromFile(filePath);
        for(Status st : list){
            long tweetId= st.getId();
            String tweetText= st.getText();
            tweetText = EmojiParser.parseToAliases(tweetText);
            String tweetClean= Util.getCleanString(tweetText);
            if (!Util.isGreekTweet(tweetClean)) {
                continue;
            }
            int hashtag= Util.getHashtagsCategory(st);
            int mention= Util.getMentionsCategory(st);
            int sentiment= Util.getEmojiCategory(tweetText);
            String positiveWords= Util.getPositiveWords(tweetClean);
            String negativeWords= Util.getNegativeWords(tweetClean);
            int wordSentiment= Util.getWordSentiment( positiveWords,negativeWords);
            Date date= new Date(st.getCreatedAt().getTime());
            tweetStuff.add(new ListStructure(tweetId,tweetText,hashtag,mention,sentiment,date,positiveWords,negativeWords,wordSentiment));
        }
        return tweetStuff;
    }


    public static void insertRecordIntoTable(String filepath) throws SQLException {

        Connection dbConnection = null;
        PreparedStatement preparedStatement = null;



        try {
            dbConnection = getDBConnection();
            preparedStatement = dbConnection.prepareStatement(SQL_INSERT_STATEMENT);

            for( ListStructure l: extractToList(filepath)){
                preparedStatement.clearParameters();
                preparedStatement.setString(1,Long.toString(l.id));
                preparedStatement.setString(2,l.text);
                preparedStatement.setInt(3,l.category_hashtag);
                preparedStatement.setInt(4,l.category_mention);
                preparedStatement.setInt(5,l.category_sentiment);
                preparedStatement.setDate(6,l.tweet_date);
                preparedStatement.setInt(7,l.word_sentiment);
                preparedStatement.setString(8,l.positive_words);
                preparedStatement.setString(9,l.negative_words);
                // execute insert SQL stetement
                try {
                    preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    System.out.println(e.getMessage());
                }
            }
        } catch (SQLException e) {

            System.out.println(e.getMessage());

        } finally {

            if (preparedStatement != null) {
                preparedStatement.close();
            }

            if (dbConnection != null) {
                dbConnection.close();
            }

        }

    }

    private static class ListStructure{
        long id;
        String text;
        int category_hashtag;
        int category_mention;
        int category_sentiment;
        int word_sentiment;
        String positive_words;
        String negative_words;
        Date tweet_date;

        public ListStructure(long id,String text, int category_hashtag, int category_mention,int category_sentiment, Date tweet_date,String positive_words,String negative_words,int word_sentiment) {
            this.id=id;
            this.text = text;
            this.category_hashtag = category_hashtag;
            this.category_mention = category_mention;
            this.category_sentiment= category_sentiment;
            this.tweet_date = tweet_date;
            this.positive_words= positive_words;
            this.negative_words= negative_words;
            this.word_sentiment= word_sentiment;
        }
    }
}
