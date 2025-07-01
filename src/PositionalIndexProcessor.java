
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class PositionalIndexProcessor {

    public static void main(String[] args) throws IOException {
        // File path for reading the positional index data
        String filePath = "src/part-r-00000";

        // Maps for storing term frequency, weighted TF, document frequency, and TF-IDF
        Map<String, Map<String, Integer>> termFrequency = new TreeMap<>();
        Map<String, Map<String, Double>> weightedTF = new TreeMap<>();
        Map<String, Integer> documentFrequency = new TreeMap<>();
        Map<String, Map<String, Double>> tfIdf = new TreeMap<>();
        Set<String> documents = new HashSet<>();
        Map<String, Map<String, List<Integer>>> positionalIndex = new TreeMap<>();

        // Reading data from the file
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                String term = parts[0]; // Extract term (key)
                String[] postings = parts[1].split(";");
                Set<String> uniqueDocs = new HashSet<>(); // To store unique document IDs for each term

                // Processing each posting to extract document ID and frequency
                for (String posting : postings) {
                    if (posting.contains(":")) {
                        String[] docParts = posting.split(":");
                        String docID = docParts[0].trim();
                        documents.add(docID); // Add document to the set of unique documents
                        uniqueDocs.add(docID); // Add document to unique docs for the current term
                        int frequency = docParts[1].split(",").length; // Count frequency of positions

                        // Update term frequency for each document
                        termFrequency.computeIfAbsent(term, k -> new TreeMap<>()).put(docID, frequency);
                        String[] positions = docParts[1].split(",");
                        List<Integer> positionList = new ArrayList<>();
                        for (String pos : positions) {
                            positionList.add(Integer.parseInt(pos.trim()));
                        }

                        positionalIndex.computeIfAbsent(term, k -> new TreeMap<>())
                                .put(docID, positionList);
                        // Compute weighted TF (w * tf(1 + log(tf))) for each document
                        double weightedTFValue = frequency * (1 + Math.log(frequency));
                        weightedTF.computeIfAbsent(term, k -> new TreeMap<>()).put(docID, weightedTFValue);
                    }
                }
                // Calculate Document Frequency (DF) for the term
                documentFrequency.put(term, uniqueDocs.size());
            }
        }

        // Calculate the total number of documents
        int totalDocuments = documents.size();

        // Sort document IDs numerically for better presentation
        List<String> sortedDocuments = new ArrayList<>(documents);
        Collections.sort(sortedDocuments, (doc1, doc2) -> {
            int num1 = Integer.parseInt(doc1.replaceAll("\\D", ""));
            int num2 = Integer.parseInt(doc2.replaceAll("\\D", ""));
            return Integer.compare(num1, num2); // Compare document IDs numerically
        });

        // Calculate Inverse Document Frequency (IDF) for each term
        Map<String, Double> idf = new HashMap<>();
        for (String term : documentFrequency.keySet()) {
            int df = documentFrequency.get(term);
            double idfValue = Math.log10((double) totalDocuments / (df)); // IDF formula
            idf.put(term, idfValue);
        }

        // Calculate TF-IDF for each term in each document
        for (String term : termFrequency.keySet()) {
            Map<String, Integer> termDocFrequency = termFrequency.get(term);
            Map<String, Double> tfIdfValues = new TreeMap<>();
            for (String doc : termDocFrequency.keySet()) {
                int tfValue = termDocFrequency.get(doc);
                double tfIdfValue = tfValue * idf.get(term); // TF-IDF calculation
                tfIdfValues.put(doc, tfIdfValue);
            }
            tfIdf.put(term, tfIdfValues);
        }

        // Displaying various tables and metrics
        printTFMatrix(termFrequency, sortedDocuments);
        printWeightedTFMatrix(weightedTF, sortedDocuments);
        printDFAndIDFTable(documentFrequency, totalDocuments);
        printTFIDFTable(tfIdf, sortedDocuments);
        printDocumentLength(tfIdf, sortedDocuments);
        printNormalizedTFIDFTable(tfIdf, sortedDocuments);

        // Handling user search queries interactively
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Enter a search query (or type 'exit' to quit): ");
            String query = scanner.nextLine();
            if (query.equalsIgnoreCase("exit")) {
                System.out.println("Exiting the program.");
                break;
            }
            handleQuery(query, tfIdf, positionalIndex, sortedDocuments, totalDocuments, documentFrequency);
        }
    }

    // Method to print the Term Frequency (TF) matrix
    private static void printTFMatrix(Map<String, Map<String, Integer>> termFrequency, List<String> documents) {
        System.out.println("TF Table:\n");
        System.out.printf("%-15s", "Term");
        for (String doc : documents) {
            System.out.printf("%-10s", doc);
        }
        System.out.println();

        // Print TF values for each term in each document
        for (String term : termFrequency.keySet()) {
            System.out.printf("%-15s", term);
            for (String doc : documents) {
                int frequency = termFrequency.getOrDefault(term, new TreeMap<>()).getOrDefault(doc, 0);
                System.out.printf("%-10d", frequency);
            }
            System.out.println();
        }
        System.out.println();
    }

    private static void printWeightedTFMatrix(Map<String, Map<String, Double>> weightedTF, List<String> documents) {
        System.out.println("Weighted TF Table:");
        // طباعة رأس الجدول
        System.out.printf("\n%-15s", "Term");
        for (String doc : documents) {
            System.out.printf("%-10s", doc);
        }
        System.out.println();
        // طباعة قيم w * tf(1 + log(tf)) للمصطلحات
        for (String term : weightedTF.keySet()) {
            System.out.printf("%-15s", term);
            for (String doc : documents) {
                double weightedTFValue = weightedTF.getOrDefault(term, new TreeMap<>()).getOrDefault(doc, 0.0);
                System.out.printf("%-10.2f", weightedTFValue); // طباعة النتيجة بتنسيق عشري
            }
            System.out.println();
        }
        System.out.println();
    }

    private static void printDFAndIDFTable(Map<String, Integer> documentFrequency, int totalDocuments) {
        System.out.println("DF and IDF Table:");
        // طباعة رأس الجدول
        System.out.printf("\n%-15s%-10s%-10s\n", "Term", "DF", "IDF");
        // حساب وطباعة DF و IDF للمصطلحات
        for (String term : documentFrequency.keySet()) {
            int df = documentFrequency.get(term);
            double idf = Math.log10((double) totalDocuments / (df)); // حساب IDF
            System.out.printf("%-15s%-10d%-10.2f\n", term, df, idf);
        }
        System.out.println();
    }

    private static void printTFIDFTable(Map<String, Map<String, Double>> tfIdf, List<String> documents) {
        System.out.println("TF-IDF Table:");

        System.out.printf("\n%-15s", "Term");
        for (String doc : documents) {
            System.out.printf("%-10s", doc);
        }
        System.out.println();

        for (String term : tfIdf.keySet()) {
            System.out.printf("%-15s", term);
            for (String doc : documents) {
                double tfIdfValue = tfIdf.getOrDefault(term, new TreeMap<>()).getOrDefault(doc, 0.0);
                System.out.printf("%-10.2f", tfIdfValue);
            }
            System.out.println();
        }
        System.out.println();
    }

    private static void printDocumentLength(Map<String, Map<String, Double>> tfIdf, List<String> documents) {
        System.out.println("Document Lengths:");

        System.out.printf("\n%-15s%-10s\n", "DocID", "Length");

        for (String doc : documents) {
            double length = 0;
            for (String term : tfIdf.keySet()) {
                double tfIdfValue = tfIdf.getOrDefault(term, new TreeMap<>()).getOrDefault(doc, 0.0);
                length += Math.pow(tfIdfValue, 2);
            }
            length = Math.sqrt(length);
            System.out.printf("%-15s%-10.2f\n", doc, length);
        }
        System.out.println();
    }

    private static void printNormalizedTFIDFTable(Map<String, Map<String, Double>> tfIdf, List<String> documents) {
        System.out.println("Normalized TF-IDF Table:");
        // طباعة رأس الجدول
        System.out.printf("\n%-15s", "Term");
        for (String doc : documents) {
            System.out.printf("%-10s", doc);
        }
        System.out.println();
        // حساب Normalized TF-IDF
        for (String term : tfIdf.keySet()) {
            System.out.printf("%-15s", term);
            for (String doc : documents) {
                double tfIdfValue = tfIdf.getOrDefault(term, new TreeMap<>()).getOrDefault(doc, 0.0);
                double docLength = 0;
                for (String t : tfIdf.keySet()) {
                    docLength += Math.pow(tfIdf.getOrDefault(t, new TreeMap<>()).getOrDefault(doc, 0.0), 2);
                }
                docLength = Math.sqrt(docLength); // حساب الجذر التربيعي للطول
                double normalizedTFIDF = (docLength != 0) ? tfIdfValue / docLength : 0; // تطبيع TF-IDF
                System.out.printf("%-10.2f", normalizedTFIDF); // طباعة النتيجة بتنسيق عشري
            }
            System.out.println();
        }
        System.out.println();
    }

    public static List<Map.Entry<String, Double>> handleLogicalOperators(String query, Map<String, Map<String, Double>> tfIdf, Map<String, Map<String, List<Integer>>> positions, List<String> documents) {
        // Extract the operator and phrases from the query
        String[] parts;
        String operator = null;
        if (query.contains(" AND NOT ")) {
            parts = query.split(" AND NOT ");
            operator = "AND NOT";
        } else if (query.contains(" AND ")) {
            parts = query.split(" AND ");
            operator = "AND";
        } else if (query.contains(" OR ")) {
            parts = query.split(" OR ");
            operator = "OR";
        } else {
            throw new IllegalArgumentException("Invalid query format. Use AND, AND NOT, or OR.");
        }
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid query format. Provide exactly two phrases.");
        }

        // Trim and process the two phrases
        String phrase1 = parts[0].trim();

        String phrase2 = parts[1].trim();

        // Get documents for each phrase
        List<Map.Entry<String, Double>> docs1 = processUserQuery(phrase1, tfIdf, positions, documents);

        List<Map.Entry<String, Double>> docs2 = processUserQuery(phrase2, tfIdf, positions, documents);

        // Handle the logical operator
        Set<String> resultDocs = new HashSet<>();
        Map<String, Double> similarityMap = new HashMap<>();

        // Process the operator logic
        switch (operator) {
            case "AND" ->
                docs1.forEach(entry -> {
                    if (docs2.stream().anyMatch(d -> d.getKey().equals(entry.getKey()))) {
                        resultDocs.add(entry.getKey());
                        similarityMap.put(entry.getKey(), Math.min(entry.getValue(), docs2.stream().filter(d -> d.getKey().equals(entry.getKey())).findFirst().get().getValue()));
                    }
                });
            case "AND NOT" ->
                docs1.forEach(entry -> {
                    if (docs2.stream().anyMatch(d -> d.getKey().equals(entry.getKey()))) {

                        return;
                    }
                    resultDocs.add(entry.getKey());
                    similarityMap.put(entry.getKey(), entry.getValue());
                });
            case "OR" -> {
                docs1.forEach(entry -> {
                    resultDocs.add(entry.getKey());
                    similarityMap.put(entry.getKey(), entry.getValue());
                });
                docs2.forEach(entry -> {
                    resultDocs.add(entry.getKey());
                    similarityMap.put(entry.getKey(), entry.getValue());
                });
            }
            default ->
                throw new IllegalStateException("Unexpected value: " + operator);
        }

        // Create the result list with similarities
        List<Map.Entry<String, Double>> resultList = new ArrayList<>();
        resultDocs.forEach(docID -> resultList.add(new AbstractMap.SimpleEntry<>(docID, similarityMap.get(docID))));

        // Sort the result list by similarity
        resultList.sort((entry1, entry2) -> Double.compare(entry2.getValue(), entry1.getValue()));

        return resultList;

    }

    public static void handleQuery(String query, Map<String, Map<String, Double>> tfIdf, Map<String, Map<String, List<Integer>>> positions, List<String> documents, int totaldocs, Map<String, Integer> documentFrequency) {

        String[] queryTerms = query.split(" ");
        boolean validQuery = Arrays.stream(queryTerms)
                .anyMatch(term -> tfIdf.containsKey(term)); // Check if any query term exists in tfIdf

        if (!validQuery) {
            System.out.println("Invalid query: no terms in the query exist in the dataset.");
            return; // Exit the method if no valid terms are found
        }

        if (query.contains(" AND NOT ") || query.contains(" AND ") || query.contains(" OR ")) {
            List<Map.Entry<String, Double>> docs = handleLogicalOperators(query, tfIdf, positions, documents);
            // Check if the result is empty
            if (docs.isEmpty()) {
                System.out.println("No relevant documents found.");
            } else {
                computeTFIDF(docs, query, tfIdf, documents, totaldocs, documentFrequency);
                System.out.printf("Relevant Docs are: ");
                for (int i = 0; i < docs.size(); i++) {
                    String docID = docs.get(i).getKey();
                    if (i < docs.size() - 1) {
                        System.out.printf(docID + ", ");
                    } else {
                        System.out.printf(docID);
                    }
                }
                System.out.println();
            }
        } else {
            List<Map.Entry<String, Double>> docs = processUserQuery(query, tfIdf, positions, documents);
            // Filter documents to ensure they contain all query terms
            docs = docs.stream()
                    .filter(entry -> {
                        String docID = entry.getKey();
                        return Arrays.stream(queryTerms)
                                .allMatch(term -> tfIdf.getOrDefault(term, new TreeMap<>()).getOrDefault(docID, 0.0) > 0);
                    })
                    .collect(Collectors.toList());

            // Check if the result is empty
            if (docs.isEmpty()) {
                System.out.println("No relevant documents found.");
            } else {
                computeTFIDF(docs, query, tfIdf, documents, totaldocs, documentFrequency);
                // Print the documents with their similarity values
                for (Map.Entry<String, Double> entry : docs) {
                    String docID = entry.getKey();
                    double similarityValue = entry.getValue();
                    System.out.println("Document " + docID + ": Similarity = " + similarityValue);
                }
                System.out.printf("Relevant Docs are: ");
                for (int i = 0; i < docs.size(); i++) {
                    String docID = docs.get(i).getKey();
                    if (i < docs.size() - 1) {
                        System.out.printf(docID + ", ");
                    } else {
                        System.out.printf(docID);
                    }
                }
                System.out.println();

            }
        }
    }

    private static List<Map.Entry<String, Double>> processUserQuery(String query, Map<String, Map<String, Double>> tfIdf,
            Map<String, Map<String, List<Integer>>> positions, List<String> documents) {
        String[] queryTerms = query.split(" ");
        Map<String, Double> documentScores = new TreeMap<>();

        boolean validQuery = Arrays.stream(queryTerms)
                .anyMatch(term -> tfIdf.containsKey(term));

        if (!validQuery) {
            throw new IllegalArgumentException("Invalid query: no terms in the query exist in the dataset.");
        }

        for (String doc : documents) {
            boolean matchesOrder = true;
            List<Integer> lastTermPositions = null;

            for (String term : queryTerms) {
                List<Integer> termPositions = positions.getOrDefault(term, new TreeMap<>()).get(doc);
                if (termPositions == null || termPositions.isEmpty()) {
                    matchesOrder = false;
                    break;
                }

                boolean found = false;
                if (lastTermPositions != null) {

                    for (int pos : termPositions) {
                        boolean validPosition = lastTermPositions.stream().anyMatch(lastPos -> pos == lastPos + 1);
                        if (validPosition) {
                            found = true;
                            break;
                        }
                    }
                } else {
                    found = true;
                }

                if (!found) {
                    matchesOrder = false;
                    break;
                }

                lastTermPositions = termPositions;
            }

            if (matchesOrder) {
                double dotProduct = 0.0;
                double queryNorm = 0.0;
                double docNorm = 0.0;

                Map<String, Double> queryVector = new TreeMap<>();
                for (String term : queryTerms) {
                    double idfValue = tfIdf.getOrDefault(term, new TreeMap<>()).getOrDefault(doc, 0.0);
                    if (idfValue > 0) {
                        queryVector.put(term, idfValue);
                    }
                }

                for (String term : queryVector.keySet()) {
                    dotProduct += queryVector.get(term) * tfIdf.getOrDefault(term, new TreeMap<>()).getOrDefault(doc, 0.0);
                    queryNorm += Math.pow(queryVector.get(term), 2);
                }
                queryNorm = Math.sqrt(queryNorm);

                for (String term : tfIdf.keySet()) {
                    double value = tfIdf.getOrDefault(term, new TreeMap<>()).getOrDefault(doc, 0.0);
                    docNorm += Math.pow(value, 2);
                }
                docNorm = Math.sqrt(docNorm);

                double similarity = (queryNorm != 0 && docNorm != 0) ? dotProduct / (queryNorm * docNorm) : 0;
                if (similarity > 0) {
                    documentScores.put(doc, similarity);
                }
            }
        }

        List<Map.Entry<String, Double>> rankedDocuments = new ArrayList<>(documentScores.entrySet());
        rankedDocuments.sort((e1, e2) -> Double.compare(e2.getValue(), e1.getValue()));

        rankedDocuments = rankedDocuments.stream()
                .filter(entry -> {
                    String docID = entry.getKey();
                    return Arrays.stream(queryTerms)
                            .allMatch(term -> tfIdf.getOrDefault(term, new TreeMap<>()).getOrDefault(docID, 0.0) > 0);
                })
                .collect(Collectors.toList());

        return rankedDocuments;
    }

    private static Map<String, Map<String, Double>> getNormalizedTFIDFTable(Map<String, Map<String, Double>> tfIdf, List<String> documents) {
        Map<String, Map<String, Double>> normalizedTFIDFTable = new HashMap<>();

        Map<String, Double> docLengths = new HashMap<>();
        for (String doc : documents) {
            double docLength = 0;
            for (String term : tfIdf.keySet()) {
                double tfIdfValue = tfIdf.getOrDefault(term, new TreeMap<>()).getOrDefault(doc, 0.0);
                docLength += Math.pow(tfIdfValue, 2);
            }
            docLengths.put(doc, Math.sqrt(docLength));
        }

        for (String term : tfIdf.keySet()) {
            for (String doc : documents) {
                double tfIdfValue = tfIdf.getOrDefault(term, new TreeMap<>()).getOrDefault(doc, 0.0);
                double docLength = docLengths.getOrDefault(doc, 0.0);
                double normalizedTFIDF = (docLength != 0) ? tfIdfValue / docLength : 0;

                normalizedTFIDFTable
                        .computeIfAbsent(doc, k -> new HashMap<>())
                        .put(term, normalizedTFIDF);
            }
        }

        return normalizedTFIDFTable;
    }

// Method to calculate document lengths based on squared TF-IDF values
    public static List<Double> getDocumentLengths(Map<String, Map<String, Double>> tfIdf, List<String> documents) {
        List<Double> lengths = new ArrayList<>();

        for (String doc : documents) {
            double length = 0.0;

            for (String term : tfIdf.keySet()) {
                double tfIdfValue = tfIdf.getOrDefault(term, new TreeMap<>()).getOrDefault(doc, 0.0);
                length += Math.pow(tfIdfValue, 2);  // Sum of squared TF-IDF values
            }

            lengths.add(Math.sqrt(length));  // Square root to get the document length
        }

        return lengths;
    }

    public static void computeTFIDF(List<Map.Entry<String, Double>> docs, String query,
            Map<String, Map<String, Double>> tfIdf,
            List<String> documents,
            int totalDocs,
            Map<String, Integer> documentFrequency) {
        Map<String, Map<String, Double>> norm = getNormalizedTFIDFTable(tfIdf, documents);

        String regex = "\\s+|\\bAND\\b|\\bOR\\b|\\bAND NOT\\b";
        String[] terms = query.split(regex);

        List<String> validTerms = Arrays.stream(terms)
                .map(String::trim)
                .filter(term -> !term.isEmpty())
                .collect(Collectors.toList());

        List<Map<String, Double>> storedValues = new ArrayList<>();

        System.out.printf("\n%-15s%-15s%-10s%-10s%-10s%-15s%n",
                "Term", "TF", "TFw", "IDF", "TF-IDF", "Normalized");

        double totallengthq = 0;

        for (String term : validTerms) {
            double idfValue = Math.log10((double) totalDocs / documentFrequency.getOrDefault(term, 1));
            double tfValue = 1;
            double tfw = 1 + Math.log10(tfValue);
            double tfIdfValue = tfw * idfValue;

            Map<String, Double> termValues = new HashMap<>();
            termValues.put("TF", tfValue);
            termValues.put("TFw", tfw);
            termValues.put("IDF", idfValue);
            termValues.put("TF-IDF", tfIdfValue);

            storedValues.add(termValues);
            totallengthq += Math.pow(tfIdfValue, 2);
        }

// حساب norm المطلقة للاستعلام
        totallengthq = Math.sqrt(totallengthq);

        List<Map<String, Double>> termNormalizedValues = new ArrayList<>();

        for (int i = 0; i < validTerms.size(); i++) {
            String term = validTerms.get(i);
            double tf = storedValues.get(i).get("TF");
            double tfw = storedValues.get(i).get("TFw");
            double idfValue = storedValues.get(i).get("IDF");
            double tfIdfValue = storedValues.get(i).get("TF-IDF");
            double normalizedValue = tfIdfValue / totallengthq;

            Map<String, Double> termValue = new HashMap<>();
            termValue.put(term, normalizedValue);

            termNormalizedValues.add(termValue);

            System.out.printf("%-15s%-10.2f%-10.2f%-10.2f%-10.2f%-15.2f%n",
                    term, tf, tfw, idfValue, tfIdfValue, normalizedValue);
        }
        System.out.println("\nTotal Length Query = " + totallengthq);

        List<Map<String, Object>> termDocValues = new ArrayList<>();

        for (String term : validTerms) {

            for (Map.Entry<String, Double> entry : docs) {
                String doc = entry.getKey();

                if (norm.containsKey(doc) && norm.get(doc).containsKey(term) && norm.get(doc).get(term) > 0) {
                    double normalizedValue = norm.get(doc).get(term);

                    double termNormalizedValue = 0.0;
                    for (Map<String, Double> termValue : termNormalizedValues) {
                        if (termValue.containsKey(term)) {
                            termNormalizedValue = termValue.get(term);
                            break;
                        }
                    }

                    double resultNorm = normalizedValue * termNormalizedValue;

                    Map<String, Object> result = new HashMap<>();
                    result.put("Term", term);
                    result.put("Document", doc);
                    result.put("NormalizedValue", resultNorm);

                    termDocValues.add(result);
                }
            }
        }

        if (!termDocValues.isEmpty()) {
            System.out.printf("\n%-15s%-15s%-15s%n", "Term", "Document", "NormalizedValue");
            for (Map<String, Object> entry : termDocValues) {
                System.out.printf("%-15s%-15s%-15.4f%n",
                        entry.get("Term"),
                        entry.get("Document"),
                        entry.get("NormalizedValue"));
            }
        } else {
            System.out.println("No results found.");
        }

        Map<String, Double> docSimilarityMap = new HashMap<>();

        for (Map<String, Object> entry : termDocValues) {
            String doc = (String) entry.get("Document");
            double normalizedValue = (double) entry.get("NormalizedValue");

            docSimilarityMap.put(doc, docSimilarityMap.getOrDefault(doc, 0.0) + normalizedValue);
        }

        if (!docSimilarityMap.isEmpty()) {
            System.out.printf("\n%-15s%-15s%n", "Document", "Similarity");
            for (Map.Entry<String, Double> entry : docSimilarityMap.entrySet()) {
                String doc = entry.getKey();
                double similarityValue = entry.getValue();

                System.out.printf("%-15s%-15.4f%n", doc, similarityValue);
            }
        } else {
            System.out.println("No results found.");
        }

    }

}
