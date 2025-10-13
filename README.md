# Scalable Academic Paper Search via Distributed Processing and Parallel Computing

**Status: ✅ Development Completed**

A high-performance semantic search engine for academic literature that leverages distributed computing and advanced NLP techniques to deliver intelligent paper discovery at scale.

---

## 📘 Overview

This project delivers a scalable academic paper search system designed to handle massive datasets of scholarly publications. By combining semantic understanding with distributed processing capabilities, it provides researchers with precise, meaning-based search results rather than simple keyword matching.

The system processes millions of academic papers from various sources and creates a searchable knowledge base that understands the semantic relationships between different research topics, methodologies, and findings.

---

## 🧠 Dataset

This project utilizes the **S2ORC (Semantic Scholar Open Research Corpus)** dataset, a comprehensive collection of academic papers spanning multiple disciplines. S2ORC provides structured metadata and full-text content for millions of scholarly publications.

- **Scale:** Millions of academic papers across various domains  
- **Format:** Structured JSON with metadata and full-text content  
- **Coverage:** Computer Science, Medicine, Biology, Physics, and more  

---

## 🏗️ System Architecture

### **Data Collection Pipeline**

- **CORE Dataset**
  - One-time download  
  - Massive dataset (static snapshot)

- **S2ORC Dataset**
  - Supports selective updates  
  - Incremental fetching based on date  

- **Format Standardization:** Converts diverse document formats to a unified structure  

---

### **Text Processing Engine**

- **Symbol Normalization:** Standardizes mathematical notation and special characters  
- **Stopword Elimination:** Removes non-semantic words  
- **Tokenization:** Breaks down text into meaningful units  

---

### **Semantic Understanding Layer**

- **NLP Processing:** Advanced language understanding  
- **Embedding Generation:** Converts text to high-dimensional vector representations  
- **Topic Modeling:** Identifies key research areas  
- **Relationship Extraction:** Discovers concept and paper-level connections  

---

### **Vector Database Storage**

- **FAISS:** Facebook AI Similarity Search for efficient nearest neighbor retrieval  

---

### **Distributed Computing Framework**

- **Parallel Processing:** Multi-threaded ingestion and indexing  
- **Load Balancing:** Distributes search queries across multiple nodes  
- **Fault Tolerance:** Handles node failures and ensures recovery  
- **Scalability:** Horizontally scales for large datasets  

---

## ⚙️ Features

### **Semantic Search Capabilities**

- **Intent Understanding:** Goes beyond keyword matching  
- **Contextual Relevance:** Accounts for research context and domain  
- **Citation Analysis:** Integrates citation networks into relevance scoring  

---

### **Advanced Ranking System**

- **Semantic Similarity:** Core ranking based on vector similarity  
- **Citation Impact:** Considers citation counts and influence metrics  
- **Recency Weighting:** Weighs recent publications higher  
- **Domain Expertise:** Specialized ranking for different disciplines  

---

### **Performance Optimizations**

- **Caching Layer:** Stores frequent queries  
- **Index Optimization:** Uses efficient retrieval structures  
- **Batch Processing:** Handles concurrent searches  
- **Memory Management:** Optimizes large-scale data operations  

---

## 🚀 Development Status

### ✅ **Completed**

- Project initialization and repository setup  
- Literature review of existing academic search systems  
- Dataset acquisition and preliminary analysis  
- Technology stack evaluation  
- Data preprocessing pipeline development  
- NLP model selection and testing  
- Vector database performance comparison  
- Distributed computing framework design  
- Complete data preprocessing pipeline  
- Implemented semantic search functionality  
- Deployed FAISS vector database setup  
- Parallel processing implementation  
- Web API interface deployment  

---

## 🧩 Technology Stack

- **Language:** Python 3.12  
- **NLP Framework:** Transformers  
- **Vector Processing:** NumPy, FAISS  
- **Databases:** PostgreSQL, Redis *(for caching — planned)*  
- **Distributed Computing:** Apache Spark  
- **API Framework:** FastAPI  

---

## 🧠 Machine Learning Models

- **Text Embeddings:** SciBERT  
- **Topic Modeling:** KNN  
- **Similarity Metric:** Cosine Similarity  
- **Ranking Algorithms:** FWCI and Top-percentile  

---

## 🎥 Demo Video

A short demonstration of the system in action is available below:

👉 [**Watch Demo**](./Demo.mp4)

*(Located in the root directory as `Demo.mp4`)*

---
