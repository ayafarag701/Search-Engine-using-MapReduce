
## 📁 Search Engine using MapReduce (Cloudera Hadoop)

This project is a simple **search engine** implemented using **Java MapReduce** on **Cloudera Hadoop**.

### 🔧 How it works:

1. **Preprocessing Phase (MapReduce)**

   * Java MapReduce program reads raw documents.
   * Outputs a processed file mapping words to the documents they appear in (inverted index).
   * Run this part on **Cloudera Hadoop**.

2. **Search Phase (Query Engine)**

   * Java application takes the preprocessed output.
   * Accepts search keywords and returns relevant document names.


### ▶️ How to run:

1. Run the MapReduce code first on Cloudera to generate the processed file.
2. Use the search engine (QueryEngine) to search within that file.

