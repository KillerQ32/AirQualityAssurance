# AirQualityAssurance

### **Overview**
**AirQualityAssurance** is a data engineering and analytics project that explores how air quality, temperature, and carbon dioxide (CO₂) levels interact across different U.S. cities.
The project integrates real-world environmental data from OpenAQ and NOAA into a unified PostgreSQL database, processed with Python, and visualized using Tableau and Power BI.

---

## Tech Stack

**Language**: Python (`pandas`, `requests`, `SQLAlchemy`) : ETL, data cleaning, and transformation
**Database**: PostgreSQL 16 : Central data warehouse 
**APIs**: OpenAQ, NOAA GHCN, NOAA GML CO₂ : Real environmental data sources
**Visualization**: Tableau, Power BI : Dashboards and data storytelling
**Automation**: Prefect (planned) : Workflow scheduling 
**Containerization**: Docker Compose : Local, reproducible setup
**Version Control**: GitHub : Collaboration and documentation

---

## Problem Statement
Environmental data is scattered across different systems and hard to compare across time or regions.  
**AirQualityAssurance** aims to:
- Consolidate open datasets on air quality, weather, and CO₂  
- Build a reproducible data engineering pipeline  
- Reveal patterns linking pollution and climate change

---

## Motivation
As a Computer Science and Data Science student, I’m passionate about using data to uncover real-world insights.  
This project allows me to:
- Practice end-to-end data engineering  
- Strengthen my data visualization and storytelling skills  
- Contribute meaningfully to understanding environmental trends

---

## Approach & Methodology

1. **Data Extraction**
   - Collect PM2.5, O₃, and NO₂ readings from **OpenAQ**
   - Retrieve temperature and precipitation from **NOAA GHCN Daily**
   - Add global CO₂ concentration from **NOAA GML Mauna Loa**

2. **Data Transformation**
   - Clean and normalize all datasets in **Python**
   - Handle missing or inconsistent values
   - Standardize time zones, units, and formats

3. **Data Storage**
   - Load cleaned data into **PostgreSQL**
   - Design a relational schema for easy joins and time-series queries

4. **Visualization**
   - Create interactive dashboards in **Tableau** and **Power BI**
   - Show trends, correlations, and regional comparisons

---

## Key Features
- Unified climate + air quality database  
- Modular Python ETL scripts  
- SQL views for analytical queries  
- Interactive Tableau and Power BI dashboards  
- Optional Dockerized environment for reproducibility  

---

## Project Structure
