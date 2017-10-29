
import org.apache.commons.math3.stat.StatUtils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by xrusa on 11/6/2016.
 */
public class StatisticsExtractor extends DbOperation {
    private Stats syrizaStats= new Stats();
    private Stats ndStats= new Stats();
    public StatisticsExtractor(){
        extractStats();
    }
    private static final String SQL_SELECT_STATEMENT = "SELECT * FROM tweets";
    private List<String> ndtweetsPositive = new ArrayList<>();
    private List<String> ndtweetsNegative = new ArrayList<>();
    private List<String> ndtweetsNeutral = new ArrayList<>();
    private List<String> syrizatweetsPositive = new ArrayList<>();
    private List<String> syrizatweetsNegative = new ArrayList<>();
    private List<String> syrizatweetsNeutral = new ArrayList<>();
    private Map<Date,Integer[]> dailyStats = new TreeMap<>();
    private Map<Integer,List<Integer[]>> weeklyStats = new TreeMap<>();
    public  void extractStats() {
        try (Connection dbConnection = getDBConnection();
             PreparedStatement preparedStatement = dbConnection.prepareStatement(SQL_SELECT_STATEMENT);
             ResultSet rs = preparedStatement.executeQuery()) {
            int categoryHashtag;
            int categoryMention;
            int categorySentiment;
            Date tweetDate;
            String positiveWords;
            String negativeWords;
            int wordSentiment;
            //String tweetOLD;
            String tweet;
            while (rs.next()) {
                tweet=rs.getString(2);
                categoryHashtag=rs.getInt(3);
                categoryMention=rs.getInt(4);
                categorySentiment=rs.getInt(5);
                tweetDate=rs.getDate(6);
                wordSentiment=rs.getInt(7);
                extractDailyStats(categoryHashtag, categoryMention, tweetDate, wordSentiment);
                if (categoryHashtag==1 || categoryMention == 1){
                    syrizaStats.addStats(categoryMention, categoryHashtag, categorySentiment, tweet);
                   addToList(tweet,categorySentiment,1);
                }

                else if (categoryHashtag==2 || categoryMention==2){
                    ndStats.addStats(categoryMention, categoryHashtag, categorySentiment, tweet);
                   addToList(tweet,categorySentiment,2);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        extractWeeklyStats();
    }
    public void addToList(String tweet,int categorySentiment,int party){
        switch(categorySentiment){
            case 0: {
                if(party==1) syrizatweetsNeutral.add(tweet);
                if(party==2) ndtweetsNeutral.add(tweet);
                break;
            }case 1:{
                if(party==1) syrizatweetsPositive.add(tweet);
                if(party==2) ndtweetsPositive.add(tweet);
                break;
            }
            case 2:{
                if(party==1) syrizatweetsNegative.add(tweet);
                if(party==2) ndtweetsNegative.add(tweet);
                break;
            }
        }
    }
    public void extractDailyStats(int categoryHashtag,  int categoryMention, Date tweetDate, int wordSentiment) {
        // puts into maps Date, Int[] where
        // 0: @syriza_positive 1: @syriza_negative 2: #syriza_positive 3: #syriza_negative
        // 4: @nd_positive 5: @nd_negative 6: #nd_positive 7: #nd_negative

        Integer[] daily = dailyStats.computeIfAbsent(tweetDate, date -> {
           Integer[] newArr = new Integer[8];
            Arrays.fill(newArr, 0);
            dailyStats.put(date, newArr);
            return newArr;
        });
        if(categoryMention==1){
            if(wordSentiment==1){
                daily[0]+=1;
            }
            else if(wordSentiment==2){
                daily[1]+=1;
            }
        }
        else if(categoryMention==2){
            if(wordSentiment==1){
                daily[4]+=1;
            }
            else if(wordSentiment==2){
                daily[5]+=1;
            }
        }
        if(categoryHashtag==1){
            if(wordSentiment==1){
                daily[2]+=1;
            }
            else if(wordSentiment==2){
                daily[3]+=1;
            }
        }
        else if(categoryHashtag==2){
            if(wordSentiment==1){
                daily[6]+=1;
            }
            else if(wordSentiment==2){
                daily[7]+=1;
            }
        }
    }
    public void extractWeeklyStats(){
        for(Map.Entry<Date, Integer[]> entry: dailyStats.entrySet() ) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(entry.getKey());
            int week = calendar.get(Calendar.WEEK_OF_YEAR);
            List<Integer[]> weekArr = weeklyStats.computeIfAbsent(week, weekNum -> {
                List<Integer[]> newArr = new ArrayList<Integer[]>();
                weeklyStats.put(weekNum, newArr);
                return newArr;
            });
            Integer []arr=new Integer [8];
            System.arraycopy(entry.getValue(),0,arr,0,8);
            weekArr.add(arr);
        }
    }
    public void printDailyStats(){
        for(Map.Entry<Date, Integer[]> entry: dailyStats.entrySet() ) {
            System.out.println("The date is: "+ entry.getKey());
            Integer[] arr= entry.getValue();
            printStuff(arr);

        }
    }
    public void printWeeklyStats(){
        for(Map.Entry<Integer,List<Integer[]>> entry: weeklyStats.entrySet()){
            System.out.println("For week "+entry.getKey());
            List<Integer[]> arr=entry.getValue();
            int totalWeekly = arr.stream().flatMapToInt(intArr -> Arrays.stream(intArr).mapToInt(i -> i)).sum();
            double[] syrizaPositive = arr.stream().mapToDouble(intArr -> intArr[0] + intArr[2]).toArray();
            double[] syrizaNegative = arr.stream().mapToDouble(intArr -> intArr[1] + intArr[3]).toArray();
            double[] ndPositive = arr.stream().mapToDouble(intArr -> intArr[4] + intArr[6]).toArray();
            double[] ndNegative = arr.stream().mapToDouble(intArr -> intArr[5] + intArr[7]).toArray();
            double syrizaMeanPos = StatUtils.mean(syrizaPositive);
            double syrizaMeanNeg = StatUtils.mean(syrizaNegative);
            double syrizaVariancePos = StatUtils.variance(syrizaPositive);
            double syrizaVarianceNeg = StatUtils.variance(syrizaNegative);
            double ndMeanPos = StatUtils.mean(ndPositive);
            double ndMeanNeg = StatUtils.mean(ndNegative);
            double ndVariancePos = StatUtils.variance(ndPositive);
            double ndVarianceNeg = StatUtils.variance(ndNegative);
            double[] positive = new double[syrizaPositive.length + ndPositive.length];
            System.arraycopy(syrizaPositive, 0, positive, 0, syrizaPositive.length);
            System.arraycopy(ndPositive, 0, positive, syrizaPositive.length, ndPositive.length);
            double[] negative = new double[syrizaNegative.length + ndNegative.length];
            System.arraycopy(syrizaNegative, 0, negative, 0, syrizaNegative.length);
            System.arraycopy(ndNegative, 0, negative, syrizaNegative.length, ndNegative.length);
            double meanPos= StatUtils.mean(positive);
            double meanNeg= StatUtils.mean(negative);
            double variancePos = StatUtils.variance(positive);
            double varianceNeg = StatUtils.variance(negative);
            System.out.println("The total tweets were: "+totalWeekly );
            System.out.println("The total positive tweets were: "+ sumArray(positive));
            System.out.println("The total negative tweets were: "+ sumArray(negative));
            System.out.println("The total mean positive was: "+meanPos);
            System.out.println("the total mean negative was: "+ meanNeg);
            System.out.println("The total variance positive was " + variancePos);
            System.out.println("The total variance negative was " + varianceNeg);
            System.out.println("By party");
            System.out.println("Syriza had : " + sumArray(syrizaPositive) +" positive tweets and "+ sumArray(syrizaNegative)+ " negative tweets" );
            System.out.println("New Democracy had: "+ sumArray(ndPositive)+" positive tweets and "+ sumArray(ndNegative)+" negative tweets");
            System.out.println("By party the mean positive was: ");
            System.out.println("For Syriza : "+ syrizaMeanPos);
            System.out.println("For New Democracy: "+ ndMeanPos);
            System.out.println("By party the variance for positive was: ");
            System.out.println("For Syriza : "+ syrizaVariancePos);
            System.out.println("For New Democracy: "+ ndVariancePos);
            System.out.println("By party the mean negative was: ");
            System.out.println("For Syriza: "+ syrizaMeanNeg);
            System.out.println("For New Democracy: "+ndMeanNeg);
            System.out.println("By party the variance for negative was: ");
            System.out.println("For Syriza : "+ syrizaVarianceNeg);
            System.out.println("For New Democracy: "+ ndVarianceNeg);

        }
    }

    private static double sumArray(double[] arr) {
        return Arrays.stream(arr).sum();
    }

    public void printStuff(Integer[]arr){
        System.out.println("@syriza_positive "+arr[0]+ " @syriza_negative "+arr[1] +" #syriza_positive "+ arr[2]+" #syriza_negative "+arr[3] );
        System.out.println("@nd_positive "+arr[4]+ " @nd_negative "+arr[5]+ " #nd_positive "+arr[6]+ " #nd_negative "+arr[7]);
    }

    public void printStats(boolean syriza){

        Stats st;
        if(syriza){
            st=syrizaStats;
        }
        else{
            st=ndStats;
        }
        int positive=st.getPositiveTweetCount();
        int negative=st.getNegativeTweetCount();
        int neutral=st.getNeutralTweetCount();
        int total= positive+negative+neutral;
        System.out.println("The total tweets were "+total+"\n The positive tweets are :"+positive + "\nThe negative tweets are: "+ negative+
                "\nthe neutral or not specified tweets are: "+ neutral+"\nThe total mentions were: "+st.getMapCount(1) +"\nThe positive mentions were: "+ st.getMapCount(1,1)+ "\nThe negative mentions were: "
                + st.getMapCount(1,2)+ "\nThe neutral mentions were: "+ st.getMapCount(1,0) +"\nThe total hashtags were: "+st.getMapCount(2) +"\nThe positive hashtags were: "+ st.getMapCount(2,1)+ "\nThe negative hashtags were: "
                + st.getMapCount(2,2)+ "\nThe neutral hashtags were: "+ st.getMapCount(2,0) );
        writeToFile("ndNeutral.txt",ndtweetsNeutral);
        writeToFile("ndPositive.txt",ndtweetsPositive);
        writeToFile("ndNegative.txt",ndtweetsNegative);
        writeToFile("syrizaNeutral.txt",syrizatweetsNeutral);
        writeToFile("syrizaPositive.txt",syrizatweetsPositive);
        writeToFile("syrizaNegative.txt",syrizatweetsNegative);

    }
    public void writeToFile(String filename,List<String> tweets){
        try {
            PrintWriter out = new PrintWriter(new FileOutputStream(filename, false));

            for(String tweet : tweets){
                out.println("tweet recieved: ");
                out.println(tweet);
            }
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static class Stats{
        int positiveTweetCount;
        int negativeTweetCount;
        int neutralTweetCount;
        Map<Integer, Set<String>> mentionMap= new HashMap<>();// key sentiment,value tweetText
        Map<Integer, Set<String>> hashtagMap= new HashMap<>(); // key sentiment,value tweetText
        public void  addStats(int mention,int hashtag,int sentiment, String tweet){
            addToMap(mentionMap, mention, sentiment, tweet);
            addToMap(hashtagMap, hashtag, sentiment, tweet);
            if(sentiment==1){
                positiveTweetCount++;
            }
            else if(sentiment==2){
                negativeTweetCount++;
            }
            else{
                neutralTweetCount++;
            }

        }

        private void addToMap(Map<Integer, Set<String>> map, int hashtagOrMention, int sentiment, String tweet) {
            if (hashtagOrMention != 0){
                Set<String> hashtags = map.get(sentiment);
                if (hashtags == null) {
                    hashtags = new HashSet<>();
                    map.put(sentiment, hashtags);
                }
                hashtags.add(tweet);
            }
        }

        public int getMapCount(int mapId) {
            Map<Integer, Set<String>> map = mentionMap;
            if (mapId == 2) {
                map = hashtagMap;
            }
            int count = 0;
            for (Set<String> list : map.values()) {
                count += list.size();
            }
            return count;
        }

        public int getMapCount(int mapId, int sentiment) {
            Map<Integer, Set<String>> map = mentionMap;
            if (mapId == 2) {
                map = hashtagMap;
            }
            return map.getOrDefault(sentiment, Collections.EMPTY_SET).size();
        }

        public int getPositiveTweetCount() {
            return positiveTweetCount;
        }

        public int getNegativeTweetCount() {
            return negativeTweetCount;
        }

        public int getNeutralTweetCount() {
            return neutralTweetCount;
        }

        public Map<Integer, Set<String>> getMentionMap() {
            return mentionMap;
        }

        public Map<Integer, Set<String>> getHashtagMap() {
            return hashtagMap;
        }
    }
}
