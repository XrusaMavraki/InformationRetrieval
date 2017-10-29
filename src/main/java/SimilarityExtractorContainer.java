import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by xrusa on 05-Feb-17.
 */
public class SimilarityExtractorContainer {

    private final SimilarityExtractor similarityExtractor;
    private final Map<Integer, Map<String, List<String>>> pValueToNearestWordToNearestNeighbours;

    public SimilarityExtractorContainer(String sql) {
        this.similarityExtractor = SimilarityExtractor.getSimilarityExtractorFromQuery(sql);
        pValueToNearestWordToNearestNeighbours = new TreeMap<>();
    }

    public void calculateNearestNeighboursForPValues(int... pValues) {
        for (int pValue : pValues) {
            pValueToNearestWordToNearestNeighbours.put(pValue, similarityExtractor.classifyNearestNeighbours(pValue));
        }
    }

    /**
     * For task 6. Saves results to files.
     * @throws IOException
     */
    public void saveNeighboursToFiles() throws IOException {
        // The name of the root folder.
        String timestamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date());
        Path rootPath = Files.createDirectories(Paths.get("similarities", timestamp));
        for (Map.Entry<Integer, Map<String, List<String>>> entry : pValueToNearestWordToNearestNeighbours.entrySet()) {
            Path curPathForP = Files.createDirectories(rootPath.resolve("p_" + entry.getKey()));
            saveNeighboursInDirectory(curPathForP, entry.getValue());
        }
    }

    private void saveNeighboursInDirectory(Path dirRoot, Map<String, List<String>> wordToNearestNeighbours) {
        for (Map.Entry<String, List<String>> entry : wordToNearestNeighbours.entrySet()) {
            String fileName = Util.POSITIVE_WORDS.contains(entry.getKey()) ? "ExtPos_" : "ExtNeg_";
            fileName += entry.getKey();
            Path filePath = dirRoot.resolve(fileName);
            try (final BufferedWriter bw = Files.newBufferedWriter(filePath)) {
                bw.write(entry.getValue().stream().collect(Collectors.joining("\n")));
                bw.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * For question 7
     */
    public void printExtMeans() {
        for (Map.Entry<Integer, Map<String, List<String>>> entry : pValueToNearestWordToNearestNeighbours.entrySet()) {
            System.out.println("--- For p value = " + entry.getKey() + " ---");
            double positiveAvg = calculateAverageHits(Util.POSITIVE_WORDS, entry.getValue());
            double negativeAvg = calculateAverageHits(Util.NEGATIVE_WORDS, entry.getValue());
            System.out.println("Mean Number of Positive hits: " + positiveAvg);
            System.out.println("Mean Number of Negative hits: " + negativeAvg);
        }
    }

    private double calculateAverageHits(final Set<String> lex, Map<String, List<String>> wordToNeighbours) {
        return wordToNeighbours.entrySet().parallelStream()
                // filters the map so that only the current lex (positive or negative) words remain
                .filter(e -> lex.contains(e.getKey()))
                // counts for every word, how many of the neighbours are already in the lex
                .mapToLong(e -> e.getValue().stream().filter(lex::contains).count())
                // just computes the average
                .average().orElse(0.0);
    }

    public void printExtendedLex() {
        System.out.println("### Extended Lexicons ###");
        for (Map.Entry<Integer, Map<String, List<String>>> entry : pValueToNearestWordToNearestNeighbours.entrySet()) {
            System.out.println("--- For p value = " + entry.getKey() + " ---");
            System.out.println("Extended Positive Lex: " + calculateExtendedLex(Util.POSITIVE_WORDS, entry.getValue()));
            System.out.println("Extended Negative Lex: " + calculateExtendedLex(Util.NEGATIVE_WORDS, entry.getValue()));
        }
    }

    private String calculateExtendedLex(final Set<String> lex, Map<String, List<String>> wordToNeighbours) {
        return wordToNeighbours.entrySet().stream()
                .filter(e -> lex.contains(e.getKey()))
                .flatMap(e -> e.getValue().stream().filter(s -> !lex.contains(s)))
                .distinct()
                .collect(Collectors.joining(", "));
    }
}
