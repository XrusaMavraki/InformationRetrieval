import org.apache.commons.math3.linear.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by xrusa on 29-Jan-17.
 */
public class SimilarityExtractor {

    public static final String SQL_ALL_TWEETS = "select text from tweets";

    private final RealMatrix similarityMatrix;
    // antistoixizei index se term. Dhladh dothentos enos index (grammh) sto RealMatrix, poios oros vrisketai ekei?
    private final List<String> indexToTerm;

    /**
     * Private constructor. Objects are created from factory method {@link SimilarityExtractor#getSimilarityExtractorFromQuery(String)}
     */
    private SimilarityExtractor(List<String> tweets) {
        indexToTerm = new ArrayList<>();
        System.out.println("Creating Term To Document table...");
        RealMatrix matrix = createTermDocumentTable(tweets);
        System.out.println("Applying SVD... This might take a while...");
        matrix = applySVD(matrix);
        System.out.println("Calculating similarity matrix...");
        similarityMatrix = matrix.multiply(matrix.transpose());
    }

    /**
     * Creates SimilarityExtractor objects from tweets that result after executing the passed SQL query.
     * @param sql The SQL query to execute. The tweet text is expected to be on the first column.
     * @return A new SimilarityExtractor instance.
     */
    public static SimilarityExtractor getSimilarityExtractorFromQuery(String sql) {
        List<String> tweets = new ArrayList<>();
        try (Connection dbConnection = DbOperation.getDBConnection();
             PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
             ResultSet rs = preparedStatement.executeQuery()) {
            while (rs.next()) {
                tweets.add(Util.getCleanString(rs.getString(1)));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new SimilarityExtractor(tweets);
    }

    private RealMatrix createTermDocumentTable(List<String> tweets) {

        Map<String,Map<Integer,Double>> termtoDocuments = new TreeMap<>();
        Map<String,Map<Integer,Double>> filteredTermtoDocuments;
        for(int i=0; i<tweets.size();i++ ){//for each tweet
            String[] terms=tweets.get(i).split(" "); //the terms
            for (String term : terms){//for each term
                if (term.trim().isEmpty())
                    continue;
                term = term.trim();
                Map<Integer,Double> documentQuantity = termtoDocuments.computeIfAbsent(term, t -> {
                    Map<Integer,Double> m=new HashMap<>();
                    termtoDocuments.put(t,m);
                    return m;
                });
                double curr=documentQuantity.getOrDefault(i,0.0);
                documentQuantity.put(i,++curr);
            }
        }
        filteredTermtoDocuments = termtoDocuments.entrySet().stream()
                .filter(entry -> entry.getValue().size() > 1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        termtoDocuments.clear();
        double[][] matrix= create2Darray(filteredTermtoDocuments, tweets.size());
        return new Array2DRowRealMatrix(matrix);
    }

    private double[][] create2Darray(Map<String, Map<Integer, Double>> filteredTermtoDocuments,int documentsSize) {
        int termSize=filteredTermtoDocuments.size();
        double[][] matrix= new double[termSize][documentsSize];
        int rowIndex = 0;
        for (Map.Entry<String, Map<Integer, Double>> termToDocuments : filteredTermtoDocuments.entrySet()){
            for (Map.Entry<Integer, Double> documentIdToQuantity : termToDocuments.getValue().entrySet()) {
                matrix[rowIndex][documentIdToQuantity.getKey()]=documentIdToQuantity.getValue();
            }
            indexToTerm.add(termToDocuments.getKey());
            rowIndex++;
        }
        return matrix;
    }

    private RealMatrix applySVD(RealMatrix matrix) {
        SingularValueDecomposition svd = new SingularValueDecomposition(matrix);
        RealMatrix uMatrix = svd.getU();
        for (int i = 0; i < uMatrix.getRowDimension(); i++) {
            RealVector rowVector = uMatrix.getRowVector(i).mapMultiply(1.0 / uMatrix.getRowVector(i).getNorm());
            uMatrix.setRowVector(i, rowVector);
        }
        return uMatrix;
    }

    public Map<String, List<String>> classifyNearestNeighbours(int p) {
        Map<String, List<String>> termToNearestTerms = new HashMap<>();
        for (int i=0; i<similarityMatrix.getRowDimension();i++){
            String curWord = indexToTerm.get(i);
            if (!Util.POSITIVE_WORDS.contains(curWord) && !Util.NEGATIVE_WORDS.contains(curWord))
                continue;
            final double[] curRow = similarityMatrix.getRow(i);
            List<String> temp = IntStream.range(0, similarityMatrix.getColumnDimension()) // iterate on indexes for (int i = 0; i < length; i++)...
                    .boxed() // turns int to Integer object. Required for invoking sorted
                    // sorts the indices by comparing the columns they point to. In reversed order (descending).
                    .sorted(Comparator.comparingDouble((Integer c1) -> curRow[c1]).reversed())
                    // skips the first result, because it will be the same word.
                    .skip(1)
                    // limits to p first results
                    .limit(p)
                    // maps to the word using the indexToTerm list.
                    .map(indexToTerm::get)
                    // collects everything to a list.
                    .collect(Collectors.toList());
            termToNearestTerms.put(curWord, temp);
        }
        return termToNearestTerms;
    }
}
