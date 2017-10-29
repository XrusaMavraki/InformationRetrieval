import java.io.IOException;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.stream.Collectors;

public class MainClass {



    public static void main(String[] args) throws Exception {

        Util.setPosNegWordDataFromXLS("PosLex.xls",1);
        Util.setPosNegWordDataFromXLS("NegLex.xls",2);
        System.out.println("Do you want to record new tweets, insert recorded tweets into database, extract stats from database, extract daily and weekly stats, or find closest neighbours. Press 1,2, 3, 4 or 5 accordingly ");
        Scanner scanner= new Scanner(System.in);
        String next=scanner.nextLine();
        int input=Integer.parseInt(next);
        if (input==1){
            TwitterListener.startListener();
        }
        else if (input==2){
            setDbParameters(scanner);
            DatabaseImporter.insertRecordIntoTable("tweets.bin");
        }
        else if(input==3){
            setDbParameters(scanner);
            System.out.println("Do you want to extract data for syriza or newdemocracy? Press 1 or 2 accordingly");
            next=scanner.nextLine();
            input=Integer.parseInt(next);
            StatisticsExtractor extractor= new StatisticsExtractor();

            if(input==1){
                extractor.printStats(true);
            }
            else{
                extractor.printStats(false);
            }
        }
        else if(input==4){
            setDbParameters(scanner);
            StatisticsExtractor extractor= new StatisticsExtractor();
            extractor.printDailyStats();
            extractor.printWeeklyStats();
        } else if (input == 5) {
            setDbParameters(scanner);
            SimilarityExtractorContainer similarityExtractorContainer = new SimilarityExtractorContainer(SimilarityExtractor.SQL_ALL_TWEETS);
            similarityExtractorContainer.calculateNearestNeighboursForPValues(1, 2, 4, 5, 10);
            similarityExtractorContainer.saveNeighboursToFiles();
            similarityExtractorContainer.printExtMeans();
            similarityExtractorContainer.printExtendedLex();
        }

    }

    private static void setDbParameters(Scanner scanner) {
        System.out.println("please insert Database username \n");
        DbOperation.DB_USER=scanner.nextLine().trim();
        System.out.println("please insert Database password \n");
        DbOperation.DB_PASSWORD=scanner.nextLine().trim();
    }
}
