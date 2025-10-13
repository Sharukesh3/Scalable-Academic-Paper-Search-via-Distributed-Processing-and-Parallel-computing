# Scalable Academic Paper Search via Distributed Processing and Parallel Computing

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Status](https://img.shields.io/badge/Status-Development%20Completed-success)
![License](https://img.shields.io/badge/License-MIT-green)
![Framework](https://img.shields.io/badge/Framework-FastAPI-orange)
![Compute](https://img.shields.io/badge/Distributed-Apache%20Spark-red)
![Database](https://img.shields.io/badge/Vector%20DB-FAISS-lightgrey)

---

A high-performance **semantic search engine** for academic literature that leverages **distributed computing** and **advanced NLP techniques** to deliver intelligent paper discovery at scale.

---

## 📘 Overview

This project provides a **scalable academic paper search system** designed to process millions of scholarly publications efficiently.  
By combining **semantic understanding** with **distributed processing**, it enables meaning-based, context-aware search — far beyond simple keyword matching.

---

## 🧠 Dataset

The system is powered by the **S2ORC (Semantic Scholar Open Research Corpus)** — a massive, multi-domain dataset of academic papers.

- **Scale:** Millions of papers  
- **Format:** Structured JSON (metadata + full text)  
- **Coverage:** Computer Science, Medicine, Biology, Physics, and more  

---

<details>
<summary>🏗️ <b>System Architecture (Click to Expand)</b></summary>

### **1️⃣ Data Collection Pipeline**
- **CORE Dataset:** One-time static download  
- **S2ORC Dataset:** Supports incremental updates  
- **Format Standardization:** Unifies metadata and full-text structures  

### **2️⃣ Text Processing Engine**
- **Symbol Normalization:** Standardizes mathematical notation  
- **Stopword Elimination:** Removes uninformative tokens  
- **Tokenization:** Splits text into meaningful units  

### **3️⃣ Semantic Understanding Layer**
- **NLP Processing:** Extracts semantic meaning  
- **Embedding Generation:** Converts text into vector representations  
- **Topic Modeling:** Identifies themes and domains  
- **Relationship Extraction:** Finds links between papers and concepts  

### **4️⃣ Vector Database Storage**
- **FAISS (Facebook AI Similarity Search):** High-speed vector similarity retrieval  

### **5️⃣ Distributed Computing Framework**
- **Parallel Processing:** Multi-threaded indexing and queries  
- **Load Balancing:** Equitable distribution of query load  
- **Fault Tolerance:** Auto-recovery from node failure  
- **Scalability:** Supports horizontal scaling across clusters  

</details>

---

<details>
<summary>⚙️ <b>Features (Click to Expand)</b></summary>

### 🔍 **Semantic Search Capabilities**
- **Intent Understanding:** Goes beyond keyword matching  
- **Contextual Relevance:** Considers domain and topic hierarchy  
- **Citation Analysis:** Integrates citation networks for scoring  

### 🧮 **Advanced Ranking System**
- **Semantic Similarity:** Vector-based ranking  
- **Citation Impact:** Paper influence weighting  
- **Recency Weighting:** Prioritizes newer work  
- **Domain Expertise:** Discipline-specific ranking  

### 🚀 **Performance Optimizations**
- **Caching Layer:** Intelligent query caching (Redis planned)  
- **Index Optimization:** Memory-efficient FAISS indexing  
- **Batch Processing:** Handles concurrent query streams  
- **Memory Management:** Adaptive resource utilization  

</details>

---

## 🧩 Technology Stack

| Component | Technology |
|------------|-------------|
| **Language** | Python 3.12 |
| **NLP Framework** | Transformers |
| **Vector Search** | FAISS |
| **Databases** | PostgreSQL, Redis *(planned)* |
| **Distributed Computing** | Apache Spark |
| **API Framework** | FastAPI |

---

## 🧠 Machine Learning Models

| Task | Model / Algorithm |
|------|--------------------|
| **Text Embeddings** | SciBERT |
| **Topic Modeling** | KNN |
| **Similarity Metric** | Cosine Similarity |
| **Ranking Algorithms** | FWCI, Top-percentile |

---

## ✅ Development Status

### ✔️ **Completed Milestones**
- Repository setup and literature review  
- Dataset acquisition and preprocessing  
- NLP model benchmarking  
- Vector database evaluation (FAISS)  
- Distributed architecture design and implementation  
- Semantic search functionality  
- Web API deployment with FastAPI  
- Parallel processing integration  
- Demo release  

---

## 🎥 Demo

<video src="./Demo.mp4" width="720" controls>
  Your browser does not support embedded videos.  
  👉 [Click here to watch the demo](./Demo.mp4)
</video>

*(Located in the root directory as `Demo.mp4`)*

---

## 📜 License

This project is released under the [MIT License](LICENSE).

---

## 💡 Future Directions

- 🔹 Integration with Redis caching  
- 🔹 Continuous dataset ingestion  
- 🔹 UI dashboard for research discovery  
- 🔹 Model fine-tuning using citation-based feedback  

---

⭐ **If you like this project, consider starring the repo!**
