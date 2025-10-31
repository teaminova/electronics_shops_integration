# Data Normalization and Integration of Electronics Store Products  
**(Anhoch & Neptun)**

## Project Overview
This project focuses on collecting, cleaning, and integrating product data from two major Macedonian electronics retailers — **Anhoch** and **Neptun**.  
The goal is to build a system that identifies and matches equivalent products across both stores, even when they differ in naming conventions, specifications, or data structures.

Developed as part of the **ICT Project Management** course at the  
**Faculty of Computer Science and Engineering (FINKI), Ss. Cyril and Methodius University – Skopje.**

## Key Components
- **Web Scraping:**  
  Automated extraction of product data and specifications using **Playwright** and **BeautifulSoup**.
- **Categorization:**  
  Product classification using **LLaMA 3 (Groq API)** for consistent and standardized categories.
- **Specification Normalization:**  
  Unified JSON schema generation per product type and structured data extraction via LLMs.
- **Keyword Extraction:**  
  Identification of core model names for accurate product comparison.
- **Product Matching:**  
  Cosine similarity with **SentenceTransformers (MiniLM)** embeddings to match identical items between stores.

## Tools & Technologies
- **Languages:** Python  
- **Libraries:** Playwright, BeautifulSoup, Asyncio, Pandas, SentenceTransformers  
- **AI/LLM Models:** LLaMA 3 via Groq API  
- **Data Format:** CSV, JSON  

## Results
- Unified dataset of products with normalized specifications  
- Automated product matching between Anhoch and Neptun  
- Foundation for a price comparison or analytics platform in the Macedonian e-commerce sector  

## Authors
- **Tea Minova**  
- **Nikola Jankovikj**  

**Mentor:** Milena Trajanoska  

---

