# 🎬 MovieLens Data Analysis using PySpark

## 📘 Project Overview
This project analyzes the **MovieLens dataset** using **PySpark** to uncover audience insights about movie ratings, genres, and tagging behavior.  
The analysis includes data integration from multiple CSV files (`movies.csv`, `ratings.csv`, `tags.csv`) to perform large-scale aggregations and joins efficiently on a distributed framework.

---

## 🧩 Dataset Description
- **movies.csv** — contains movie titles and genres.  
- **ratings.csv** — includes user ratings for each movie (scale: 0.5–5.0).  
- **tags.csv** — represents user-assigned descriptive tags for movies.

---

## 🔍 Analysis Summary

### 1️⃣ Top Rated Movies (Q6)
Identified the **top 5 movies** with the highest average ratings, considering only titles that received **at least 100 ratings** to ensure reliability.

### 2️⃣ Most Tagged Movie (Q7)
Found the **movie tagged by the most unique users**, revealing which title inspired the greatest level of audience tagging activity.

### 3️⃣ Highest Rated Genres (Q8)
Calculated the **average rating for each genre** and listed the **top 3 genres** most favored by users across the entire dataset.

### 4️⃣ Tags Linked to High Ratings (Q9)
Examined the connection between **tags and highly rated movies (≥ 4.5)** to determine which tag is most commonly associated with high-quality films.

### 5️⃣ Widest Rating Range (Q10)
Measured the **difference between maximum and minimum ratings** per movie to find which film had the **widest range**, indicating the most divisive audience opinions.

---

## ⚙️ Technologies Used
- **Apache Spark (PySpark)**
- **Python 2.7**
- **HDFS / Hadoop Environment**
- **HDP Sandbox**

---

## 📈 Output Format
Each analysis outputs results to HDFS as CSV files for easy export and visualization:
